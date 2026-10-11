import {
  authorizeProjectCommand,
  authorizeCommandAdmission,
  mutationDenied,
  type MutationAuthority,
  type CommandAdmission,
} from "../auth/CommandAuthorization.ts";
import {
  CommandId,
  EnvironmentAuthorizationError,
  type OrchestrationProjectShell,
  ProjectId,
  type Project,
  type ProjectCreatePayload,
  type ProjectUpdatePayload,
  type ProjectSnapshot,
  ThreadId,
} from "@t3tools/contracts";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "../orchestration-v2/IdAllocator.ts";
import * as LegacyV1ThreadImporter from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  decodeProjectCommandRejection,
  encodeProjectCommandRejection,
  planProjectCommand,
  type ProjectCommand,
  ProjectCommandNotEmptyError,
} from "../orchestration-v2/ProjectCommands.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import { planThreadDeletion } from "../orchestration-v2/ThreadDeletion.ts";
import * as ProjectEnrichmentService from "./ProjectEnrichmentService.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";

export interface ProjectCreateInput extends ProjectCreatePayload {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
}

export interface ProjectUpdateInput extends ProjectUpdatePayload {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
}

export interface ProjectBootstrapInput extends ProjectCreateInput {}

export interface ProjectDeleteInput {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly force?: boolean;
}

export class ProjectNotFoundError extends Schema.TaggedError<ProjectNotFoundError>()(
  "ProjectNotFoundError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project ${this.projectId} was not found.`;
  }
}

export class ProjectConflictError extends Schema.TaggedError<ProjectConflictError>()(
  "ProjectConflictError",
  {
    projectId: ProjectId,
    workspaceRoot: Schema.String,
    conflictingProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Workspace ${this.workspaceRoot} already belongs to project ${this.conflictingProjectId}.`;
  }
}

export class ProjectNotEmptyError extends Schema.TaggedError<ProjectNotEmptyError>()(
  "ProjectNotEmptyError",
  { projectId: ProjectId },
) {
  override get message(): string {
    return `Project ${this.projectId} is not empty.`;
  }
}

export class ProjectOperationError extends Schema.TaggedError<ProjectOperationError>()(
  "ProjectOperationError",
  {
    operation: Schema.Literals([
      "normalize-workspace",
      "read-project",
      "list-projects",
      "list-threads",
      "delete-thread",
      "dispatch-project-command",
    ]),
    projectId: Schema.optional(ProjectId),
    workspaceRoot: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Project operation '${this.operation}' failed${this.projectId === undefined ? "" : ` for ${this.projectId}`}.`;
  }
}

export type ProjectServiceError =
  | ProjectNotFoundError
  | ProjectConflictError
  | ProjectNotEmptyError
  | ProjectOperationError
  | EnvironmentAuthorizationError;

export class ProjectService extends Context.Service<
  ProjectService,
  {
    readonly create: (
      authority: MutationAuthority,
      input: ProjectCreateInput,
    ) => Effect.Effect<Project, ProjectServiceError>;
    readonly bootstrap: (
      authority: MutationAuthority,
      input: ProjectBootstrapInput,
    ) => Effect.Effect<
      { readonly project: Project; readonly created: boolean },
      ProjectServiceError
    >;
    readonly update: (
      authority: MutationAuthority,
      input: ProjectUpdateInput,
    ) => Effect.Effect<Project, ProjectServiceError>;
    readonly delete: (
      authority: MutationAuthority,
      input: ProjectDeleteInput,
    ) => Effect.Effect<Project, ProjectServiceError>;
    readonly getById: (
      projectId: ProjectId,
      options?: { readonly includeDeleted?: boolean },
    ) => Effect.Effect<Option.Option<Project>, ProjectOperationError>;
    readonly getByWorkspaceRoot: (
      workspaceRoot: string,
      options?: { readonly includeDeleted?: boolean },
    ) => Effect.Effect<Option.Option<Project>, ProjectOperationError>;
    readonly snapshot: Effect.Effect<ProjectSnapshot, ProjectOperationError>;
    readonly getSnapshot: (options?: {
      readonly projectIds?: ReadonlyArray<ProjectId>;
    }) => Effect.Effect<ProjectSnapshot, ProjectOperationError>;
    /**
     * An active project's shell with its immediately available repository
     * identity; missing identity resolves in the background.
     */
    readonly getShell: (
      projectId: ProjectId,
    ) => Effect.Effect<Option.Option<OrchestrationProjectShell>, ProjectOperationError>;
    /** Active project shells, enriched like `getShell`, in creation order. */
    readonly listShells: (options?: {
      readonly projectIds?: ReadonlyArray<ProjectId>;
    }) => Effect.Effect<ReadonlyArray<OrchestrationProjectShell>, ProjectOperationError>;
  }
>()("t3/project/ProjectService") {}

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projectEnrichment = yield* ProjectEnrichmentService.ProjectEnrichmentService;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const threadProjections = yield* ProjectionStore.ProjectionStoreV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const legacyImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  // Commands for one project run in order. Commands that claim a workspace root
  // also hold that root, so two projects cannot both claim it.
  const projectLocks = yield* KeyedLock.make<ProjectId>();
  const workspaceLocks = yield* KeyedLock.make<string>();

  const toProject = (
    row: ProjectStore.ProjectRow,
    enrichment: ProjectEnrichmentService.ProjectEnrichment | null,
  ): Project => ({
    id: row.projectId,
    title: row.title,
    workspaceRoot: row.workspaceRoot,
    repositoryIdentity: enrichment?.repositoryIdentity ?? null,
    faviconPath: row.faviconPath ?? enrichment?.faviconPath ?? null,
    defaultModelSelection: row.defaultModelSelection,
    defaultThreadEnvMode: row.defaultThreadEnvMode,
    autoPull: row.autoPull,
    projectIcon: row.projectIcon,
    scripts: row.scripts,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    deletedAt: row.deletedAt,
  });

  const hydrate = Effect.fn("ProjectService.hydrate")(function* (row: ProjectStore.ProjectRow) {
    const enrichment =
      row.deletedAt === null
        ? yield* projectEnrichment.getAvailable(row.workspaceRoot)
        : yield* projectEnrichment.peek(row.workspaceRoot);
    return toProject(row, enrichment);
  });

  const readRow = (projectId: ProjectId, options?: { readonly includeDeleted?: boolean }) =>
    projects
      .get(projectId, options)
      .pipe(
        Effect.mapError(
          (cause) => new ProjectOperationError({ operation: "read-project", projectId, cause }),
        ),
      );

  const normalizeWorkspaceRoot = (input: {
    readonly projectId?: ProjectId;
    readonly workspaceRoot: string;
    readonly createIfMissing?: boolean;
  }) =>
    workspacePaths
      .normalizeWorkspaceRoot(input.workspaceRoot, {
        createIfMissing: input.createIfMissing ?? false,
      })
      .pipe(
        Effect.mapError(
          (cause) =>
            new ProjectOperationError({
              operation: "normalize-workspace",
              ...(input.projectId === undefined ? {} : { projectId: input.projectId }),
              workspaceRoot: input.workspaceRoot,
              cause,
            }),
        ),
      );

  const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
  const isNotEmptyError = Schema.is(ProjectCommandNotEmptyError);
  const isSinkWriteError = Schema.is(EventSink.EventSinkWriteError);
  const mutationError = (
    operation: ProjectOperationError["operation"],
    projectId: ProjectId,
    cause: unknown,
  ) => {
    if (isAuthorizationError(cause)) return cause;
    if (isNotEmptyError(cause)) return new ProjectNotEmptyError({ projectId });
    if (isSinkWriteError(cause)) {
      if (isAuthorizationError(cause.cause)) return cause.cause;
      if (isNotEmptyError(cause.cause)) return new ProjectNotEmptyError({ projectId });
    }
    return new ProjectOperationError({ operation, projectId, cause });
  };

  const admit = (
    authority: MutationAuthority,
    command: Parameters<typeof authorizeProjectCommand>[2],
  ) =>
    sql
      .withTransaction(authorizeProjectCommand(sql, authority, command))
      .pipe(Effect.mapError(() => mutationDenied()));

  /**
   * Plan one command against rows read under its locks, then commit its event or
   * its rejection. A reused command id resolves to the receipt it already has.
   */
  const commit = Effect.fn("ProjectService.commit")(function* (
    authority: MutationAuthority,
    command: ProjectCommand,
  ) {
    const { projectId } = command;
    const dispatchError = (cause: unknown) =>
      mutationError("dispatch-project-command", projectId, cause);
    const workspaceRoot = command.type === "project.delete" ? undefined : command.workspaceRoot;
    const planAndCommit = Effect.gen(function* () {
      yield* admit(authority, command);
      const project = Option.getOrUndefined(yield* readRow(projectId, { includeDeleted: true }));
      const workspaceOwner =
        workspaceRoot === undefined
          ? undefined
          : Option.getOrUndefined(
              yield* projects
                .findActiveByWorkspaceRoot(workspaceRoot)
                .pipe(Effect.mapError(dispatchError)),
            );
      const now = yield* DateTime.now;
      const eventId = yield* idAllocator.allocate
        .event({ commandId: command.commandId })
        .pipe(Effect.mapError(dispatchError));
      const planned = planProjectCommand({
        command,
        state: { project, workspaceOwner },
        eventId,
        now,
      });
      if (Result.isSuccess(planned)) {
        const { receipt } = yield* eventSink.commitProjectCommand(
          { authority, command },
          {
            commandId: command.commandId,
            projectId,
            commandType: command.type,
            acceptedAt: now,
            event: planned.success,
          },
        );
        return receipt;
      }
      return yield* eventSink.commitRejectedProjectCommand(
        { authority, command },
        {
          commandId: command.commandId,
          projectId,
          commandType: command.type,
          rejectedAt: now,
          error: encodeProjectCommandRejection(planned.failure),
        },
      );
    });
    const receipt = yield* projectLocks
      .withLock(
        projectId,
        workspaceRoot === undefined
          ? planAndCommit
          : workspaceLocks.withLock(workspaceRoot, planAndCommit),
      )
      .pipe(Effect.mapError(dispatchError));
    if (receipt.projectId !== projectId || receipt.commandType !== command.type) {
      return yield* dispatchError(
        `Command ${command.commandId} was already used by ${receipt.commandType} for ${receipt.projectId}.`,
      );
    }
    // A retried command re-plans against the state it already produced, so its
    // first receipt, not the new plan, decides the outcome.
    if (receipt.status === "accepted") return;
    const rejection = Option.getOrUndefined(decodeProjectCommandRejection(receipt.error));
    switch (rejection?._tag) {
      case "ProjectWorkspaceConflictError":
        return yield* new ProjectConflictError({
          projectId,
          workspaceRoot: rejection.workspaceRoot,
          conflictingProjectId: rejection.conflictingProjectId,
        });
      case "ProjectCommandMissingProjectError":
        return yield* new ProjectNotFoundError({ projectId });
      default:
        return yield* dispatchError(
          rejection ?? receipt.error ?? "The command was previously rejected.",
        );
    }
  });

  const readCommitted = Effect.fn("ProjectService.readCommitted")(function* (projectId: ProjectId) {
    const row = yield* readRow(projectId, { includeDeleted: true });
    if (Option.isNone(row)) {
      return yield* new ProjectOperationError({
        operation: "read-project",
        projectId,
        cause: "The accepted project command did not produce a project row.",
      });
    }
    return yield* hydrate(row.value);
  });

  const getById: ProjectService["Service"]["getById"] = Effect.fn("ProjectService.getById")(
    function* (projectId, options) {
      const row = yield* readRow(projectId, options);
      return Option.isNone(row) ? Option.none() : Option.some(yield* hydrate(row.value));
    },
  );

  const getByWorkspaceRoot: ProjectService["Service"]["getByWorkspaceRoot"] = Effect.fn(
    "ProjectService.getByWorkspaceRoot",
  )(function* (workspaceRoot, options) {
    const normalized = yield* normalizeWorkspaceRoot({ workspaceRoot });
    const row = yield* (
      options?.includeDeleted === true
        ? projects
            .list({ includeDeleted: true })
            .pipe(
              Effect.map((rows) =>
                Option.fromUndefinedOr(rows.find((row) => row.workspaceRoot === normalized)),
              ),
            )
        : projects.findActiveByWorkspaceRoot(normalized)
    ).pipe(
      Effect.mapError((cause) => new ProjectOperationError({ operation: "list-projects", cause })),
    );
    return Option.isNone(row) ? Option.none() : Option.some(yield* hydrate(row.value));
  });

  const create: ProjectService["Service"]["create"] = Effect.fn("ProjectService.create")(
    function* (authority, input) {
      yield* admit(authority, { ...input, type: "project.create" });
      const workspaceRoot = yield* normalizeWorkspaceRoot({
        projectId: input.projectId,
        workspaceRoot: input.workspaceRoot,
        createIfMissing: input.createWorkspaceRootIfMissing ?? false,
      });
      yield* commit(authority, {
        type: "project.create",
        commandId: input.commandId,
        projectId: input.projectId,
        title: input.title,
        workspaceRoot,
        ...(input.scripts === undefined ? {} : { scripts: input.scripts }),
      });
      yield* admit(authority, { ...input, type: "project.create" });
      yield* projectEnrichment.invalidate([workspaceRoot]);
      return yield* readCommitted(input.projectId);
    },
  );

  const update: ProjectService["Service"]["update"] = Effect.fn("ProjectService.update")(
    function* (authority, input) {
      yield* admit(authority, { ...input, type: "project.meta.update" });
      const existing = yield* readRow(input.projectId);
      if (Option.isNone(existing)) {
        return yield* new ProjectNotFoundError({ projectId: input.projectId });
      }
      const previousRoot = existing.value.workspaceRoot;
      const workspaceRoot =
        input.workspaceRoot === undefined
          ? previousRoot
          : yield* normalizeWorkspaceRoot({
              projectId: input.projectId,
              workspaceRoot: input.workspaceRoot,
            });
      yield* commit(authority, {
        type: "project.meta.update",
        commandId: input.commandId,
        projectId: input.projectId,
        ...(input.title === undefined ? {} : { title: input.title }),
        ...(input.workspaceRoot === undefined ? {} : { workspaceRoot }),
        ...(input.defaultModelSelection === undefined
          ? {}
          : { defaultModelSelection: input.defaultModelSelection }),
        ...(input.autoPull === undefined ? {} : { autoPull: input.autoPull }),
        ...(input.projectIcon === undefined ? {} : { projectIcon: input.projectIcon }),
        ...(input.faviconPath === undefined ? {} : { faviconPath: input.faviconPath }),
        ...(input.defaultThreadEnvMode === undefined
          ? {}
          : { defaultThreadEnvMode: input.defaultThreadEnvMode }),
        ...(input.scripts === undefined ? {} : { scripts: input.scripts }),
      });
      yield* admit(authority, { ...input, type: "project.meta.update" });
      if (workspaceRoot !== previousRoot) {
        yield* projectEnrichment.invalidate([previousRoot, workspaceRoot]);
      }
      return yield* readCommitted(input.projectId);
    },
  );

  const bootstrap: ProjectService["Service"]["bootstrap"] = Effect.fn("ProjectService.bootstrap")(
    function* (authority, input) {
      yield* admit(authority, { ...input, type: "project.create" });
      const workspaceRoot = yield* normalizeWorkspaceRoot({ workspaceRoot: input.workspaceRoot });
      const existing = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* authorizeProjectCommand(sql, authority, { ...input, type: "project.create" });
            return yield* projects.findActiveByWorkspaceRoot(workspaceRoot);
          }),
        )
        .pipe(Effect.mapError((cause) => mutationError("read-project", input.projectId, cause)));
      if (Option.isSome(existing)) {
        yield* admit(authority, { ...input, type: "project.create" });
        return { project: yield* hydrate(existing.value), created: false };
      }
      return { project: yield* create(authority, input), created: true };
    },
  );

  /** Delete one child thread durably; a stable command id makes a retry resume the cascade. */
  const deleteChildThread = Effect.fn("ProjectService.deleteChildThread")(function* (
    authority: MutationAuthority,
    input: ProjectDeleteInput,
    threadId: ThreadId,
  ) {
    const command = {
      type: "thread.delete" as const,
      commandId: CommandId.make(`${input.commandId}:delete-thread:${threadId}`),
      threadId,
    };
    const admission = { authority, command, projectId: input.projectId } satisfies CommandAdmission;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* authorizeProjectCommand(sql, authority, { ...input, type: "project.delete" });
        yield* authorizeCommandAdmission(sql, admission, command.commandId);
      }),
    );
    const rows = yield* sql<{ readonly deleted: string | null; readonly project: string }>`
      SELECT deleted_at AS deleted, project_id AS project FROM orchestration_v2_projection_threads WHERE thread_id = ${threadId}
    `;
    if (rows[0]?.project !== input.projectId || rows[0].deleted !== null) return;
    yield* legacyImporter.ensureTranscript(threadId);
    const projection = yield* threadProjections.getThreadRecords(threadId, [
      "runs",
      "attempts",
      "nodes",
      "runtimeRequests",
      "subagents",
      "providerSessions",
    ]);
    if (projection.thread.deletedAt !== null || projection.thread.projectId !== input.projectId) {
      return;
    }
    const now = yield* DateTime.now;
    const plan = yield* planThreadDeletion({
      command,
      projection,
      attachmentIds: yield* threadProjections.getThreadAttachmentIds(threadId),
      now,
      idAllocator,
    });
    const committed = yield* eventSink.commitCommand(admission, {
      commandId: command.commandId,
      commandType: command.type,
      threadId,
      acceptedAt: now,
      events: plan.events,
      effects: plan.effects,
    });
    if (
      committed.receipt.threadId !== command.threadId ||
      committed.receipt.commandType !== command.type
    ) {
      return yield* Effect.fail("The thread deletion command ID belongs to a different command.");
    }
    if (committed.receipt.status === "rejected") {
      return yield* Effect.fail(
        committed.receipt.error ?? "Thread deletion was previously rejected.",
      );
    }
  });

  /** Refuse a non-empty project without force, else delete its live threads first. */
  const deleteChildThreads = Effect.fn("ProjectService.deleteChildThreads")(function* (
    authority: MutationAuthority,
    input: ProjectDeleteInput,
  ) {
    const { projectId } = input;
    yield* admit(authority, { ...input, type: "project.delete" });
    const projectThreads = yield* sql<{ readonly id: string }>`
      SELECT thread_id AS id FROM orchestration_v2_projection_threads
      WHERE project_id = ${projectId} AND deleted_at IS NULL ORDER BY thread_id
    `.pipe(Effect.mapError((cause) => mutationError("list-threads", projectId, cause)));
    if (projectThreads.length > 0 && input.force !== true) {
      return yield* new ProjectNotEmptyError({ projectId });
    }
    // Delete children durably before the project so a failed cascade can be retried.
    yield* Effect.forEach(
      projectThreads,
      (thread) =>
        threadCommands
          .withLock(
            ThreadId.make(thread.id),
            deleteChildThread(authority, input, ThreadId.make(thread.id)),
          )
          .pipe(Effect.mapError((cause) => mutationError("delete-thread", projectId, cause))),
      { concurrency: 1, discard: true },
    );
  });

  const deleteProject: ProjectService["Service"]["delete"] = Effect.fn("ProjectService.delete")(
    function* (authority, input) {
      const { projectId } = input;
      yield* admit(authority, { ...input, type: "project.delete" });
      // A deleted row still reaches commit, so a retried command id replays its
      // receipt and any other command id is rejected as not found.
      const existing = yield* readRow(projectId, { includeDeleted: true });
      if (Option.isNone(existing)) {
        return yield* new ProjectNotFoundError({ projectId });
      }

      if (existing.value.deletedAt === null) {
        yield* deleteChildThreads(authority, input);
      }
      yield* commit(authority, { type: "project.delete", commandId: input.commandId, projectId });
      yield* admit(authority, { ...input, type: "project.delete" });
      yield* projectEnrichment.invalidate([existing.value.workspaceRoot]);
      return yield* readCommitted(projectId);
    },
  );

  const enrichShell = (shell: OrchestrationProjectShell) =>
    projectEnrichment.getAvailable(shell.workspaceRoot).pipe(
      Effect.map((enrichment) => ({
        ...shell,
        repositoryIdentity: enrichment.repositoryIdentity,
      })),
    );

  const getShell: ProjectService["Service"]["getShell"] = Effect.fn("ProjectService.getShell")(
    function* (projectId) {
      const shell = yield* projects
        .getShell(projectId)
        .pipe(
          Effect.mapError(
            (cause) => new ProjectOperationError({ operation: "read-project", projectId, cause }),
          ),
        );
      return Option.isNone(shell) ? shell : Option.some(yield* enrichShell(shell.value));
    },
  );

  const listShells: ProjectService["Service"]["listShells"] = Effect.fn(
    "ProjectService.listShells",
  )(function* (options) {
    const shells = yield* projects
      .listShells(options)
      .pipe(
        Effect.mapError(
          (cause) => new ProjectOperationError({ operation: "list-projects", cause }),
        ),
      );
    return yield* Effect.forEach(shells, enrichShell, { concurrency: 16 });
  });

  const getSnapshot: ProjectService["Service"]["getSnapshot"] = Effect.fn(
    "ProjectService.getSnapshot",
  )(function* (options) {
    const rows = yield* projects
      .list(options)
      .pipe(
        Effect.mapError(
          (cause) => new ProjectOperationError({ operation: "list-projects", cause }),
        ),
      );
    const hydrated = yield* Effect.forEach(rows, hydrate, { concurrency: 8 });
    return {
      projects: hydrated,
      updatedAt: DateTime.formatIso(yield* DateTime.now),
    } satisfies ProjectSnapshot;
  });

  return ProjectService.of({
    create,
    bootstrap,
    update,
    delete: deleteProject,
    getById,
    getByWorkspaceRoot,
    snapshot: getSnapshot(),
    getSnapshot,
    getShell,
    listShells,
  });
});

export const layer = Layer.effect(ProjectService, make).pipe(
  Layer.provide(ThreadCommandExecutor.layer),
);
