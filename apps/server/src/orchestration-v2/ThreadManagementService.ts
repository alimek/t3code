import {
  authorizeCommandAdmission,
  authorizeCommandReceipt,
  authorizeThreadMutation,
  type MutationAuthority,
} from "../auth/CommandAuthorization.ts";
import type {
  OrchestrationV2SearchThreadInput,
  OrchestrationV2SearchThreadResult,
} from "@t3tools/contracts";
import type {
  ProjectionRecordField,
  ProjectionRecordFilter,
  ProjectionRecords,
} from "./ProjectionStore.ts";
import {
  type ChatAttachment,
  CommandId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Actor,
  type OrchestrationV2Command,
  type OrchestrationV2GetTurnItemResult,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2CreationSource,
  type OrchestrationV2Run,
  type OrchestrationV2ThreadShellSnapshot,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2TurnItem,
  ProjectId,
  RunId,
  type ScheduledTaskId,
  ThreadId,
  type TurnItemId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import {
  authorizeThreadRead,
  redactReadThreadMetadata,
  trustedReadAuthority,
  type ThreadReadAuthority,
} from "../auth/ResourceAuthorization.ts";
import { decodeThreadHistoryCursor } from "./threadHistoryPaging.ts";

import * as Orchestrator from "./Orchestrator.ts";
import { projectTurnItemForDetail } from "./WireProjection.ts";
import * as LegacyV1ThreadImporter from "./legacy/LegacyV1ThreadImporter.ts";

const isOrchestratorError = Schema.is(Orchestrator.OrchestratorV2Error);

export type ThreadManagementSendMode = "auto" | "queue" | "steer" | "restart";

export interface ThreadManagementProvenance {
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
}

export function withCreationProvenance(
  command: OrchestrationV2Command,
  provenance: ThreadManagementProvenance,
): OrchestrationV2Command {
  switch (command.type) {
    case "thread.create":
    case "message.dispatch":
    case "thread.fork":
    case "thread.merge_back":
    case "delegated_task.request":
      return { ...command, ...provenance };
    default:
      return command;
  }
}

export function existingThreadIdsForCommand(
  command: OrchestrationV2ServerCommand,
): ReadonlyArray<ThreadId> {
  switch (command.type) {
    case "thread.create":
      return [];
    // Read-state commands only rewrite the thread payload's visited/unread
    // watermark; they never touch messages, so they do not need the imported
    // v1 transcript hydrated first. Visits fire on every thread-activity bump
    // while a thread is open, so keeping them off the import path matters.
    case "thread.visit":
    case "thread.mark-unread":
      return [];
    case "thread.fork":
      return [command.sourceThreadId];
    case "thread.merge_back":
      return command.sourceThreadId === command.targetThreadId
        ? [command.sourceThreadId]
        : [command.sourceThreadId, command.targetThreadId];
    case "delegated_task.request":
    case "delegated_task.wake-policy":
    case "delegated_task.completion-delivery.acknowledge":
    case "delegated_task.completion-delivery.dispose":
      return [command.parentThreadId];
    case "thread.created.record":
      return command.parentThreadId === command.targetThreadId
        ? [command.parentThreadId]
        : [command.parentThreadId, command.targetThreadId];
    default:
      return [command.threadId];
  }
}

export type ThreadManagementTerminalRunStatus = Extract<
  OrchestrationV2Run["status"],
  "completed" | "failed" | "cancelled" | "interrupted" | "rolled_back"
>;

export interface ThreadManagementSendInput {
  readonly projectId: ProjectId;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly scheduledTaskId?: ScheduledTaskId;
  readonly senderThreadId?: ThreadId;
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly modelSelection?: ModelSelection;
  readonly mode: ThreadManagementSendMode;
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
}

export interface ThreadManagementSendResult {
  readonly dispatch: Orchestrator.OrchestratorV2DispatchResult;
  readonly projection: Pick<
    OrchestrationV2ThreadProjection,
    "thread" | "runs" | "messages" | "turnItems"
  >;
  readonly message: OrchestrationV2ConversationMessage;
  readonly run: OrchestrationV2Run;
  /** Null for queued sends: the user turn item materializes when the queued turn starts. */
  readonly turnItem: Extract<OrchestrationV2TurnItem, { readonly type: "user_message" }> | null;
  readonly delivery: "started" | "queued" | "steered" | "restarted";
}

export interface ThreadManagementWaitInput {
  readonly projectId: ProjectId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly timeoutMs: number;
}

export interface ThreadManagementWaitResult {
  readonly threadId: ThreadId;
  readonly run: OrchestrationV2Run | null;
  readonly timedOut: boolean;
}

export interface ThreadManagementInterruptInput {
  readonly projectId: ProjectId;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId?: RunId;
  readonly reason?: string;
}

export type ThreadManagementInterruptResult =
  | {
      readonly type: "interrupt_requested";
      readonly run: OrchestrationV2Run;
      readonly dispatch: Orchestrator.OrchestratorV2DispatchResult;
    }
  | { readonly type: "no_active_run" }
  | {
      readonly type: "already_terminal";
      readonly run: OrchestrationV2Run & { readonly status: ThreadManagementTerminalRunStatus };
    };

export class ThreadManagementThreadNotFoundError extends Schema.TaggedError<ThreadManagementThreadNotFoundError>()(
  "ThreadManagementThreadNotFoundError",
  {
    projectId: ProjectId,
    threadId: ThreadId,
  },
) {
  override get message(): string {
    return `Thread ${this.threadId} was not found in project ${this.projectId}.`;
  }
}

export class ThreadManagementRunNotFoundError extends Schema.TaggedError<ThreadManagementRunNotFoundError>()(
  "ThreadManagementRunNotFoundError",
  {
    threadId: ThreadId,
    runId: RunId,
  },
) {
  override get message(): string {
    return `Run ${this.runId} does not belong to thread ${this.threadId}.`;
  }
}

export class ThreadManagementThreadArchivedError extends Schema.TaggedError<ThreadManagementThreadArchivedError>()(
  "ThreadManagementThreadArchivedError",
  {
    threadId: ThreadId,
  },
) {
  override get message(): string {
    return `Thread ${this.threadId} is archived and cannot receive messages.`;
  }
}

export class ThreadManagementNoSteerableRunError extends Schema.TaggedError<ThreadManagementNoSteerableRunError>()(
  "ThreadManagementNoSteerableRunError",
  {
    threadId: ThreadId,
    mode: Schema.Literals(["steer", "restart"]),
  },
) {
  override get message(): string {
    return `Thread ${this.threadId} has no running turn that can be ${this.mode === "steer" ? "steered" : "restarted"}.`;
  }
}

export class ThreadManagementThreadNotInterruptibleError extends Schema.TaggedError<ThreadManagementThreadNotInterruptibleError>()(
  "ThreadManagementThreadNotInterruptibleError",
  {
    threadId: ThreadId,
    runId: RunId,
  },
) {
  override get message(): string {
    return `Run ${this.runId} is not currently interruptible.`;
  }
}

export class ThreadManagementProjectionLoadError extends Schema.TaggedError<ThreadManagementProjectionLoadError>()(
  "ThreadManagementProjectionLoadError",
  {
    projectId: ProjectId,
    threadId: ThreadId,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Unable to load thread ${this.threadId} in project ${this.projectId}.`;
  }
}

export class ThreadManagementProjectThreadsListError extends Schema.TaggedError<ThreadManagementProjectThreadsListError>()(
  "ThreadManagementProjectThreadsListError",
  {
    projectId: ProjectId,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Unable to list threads in project ${this.projectId}.`;
  }
}

export class ThreadManagementDurableRunProjectionError extends Schema.TaggedError<ThreadManagementDurableRunProjectionError>()(
  "ThreadManagementDurableRunProjectionError",
  {
    threadId: ThreadId,
    messageId: MessageId,
  },
) {
  override get message(): string {
    return `Message ${this.messageId} was accepted on thread ${this.threadId} without a durable run projection.`;
  }
}

export const ThreadManagementError = Schema.Union([
  ThreadManagementThreadNotFoundError,
  ThreadManagementRunNotFoundError,
  ThreadManagementThreadArchivedError,
  ThreadManagementNoSteerableRunError,
  ThreadManagementThreadNotInterruptibleError,
  ThreadManagementProjectionLoadError,
  ThreadManagementProjectThreadsListError,
  ThreadManagementDurableRunProjectionError,
]);
export type ThreadManagementError = typeof ThreadManagementError.Type;

type ThreadManagementFailure = ThreadManagementError | Orchestrator.OrchestratorV2Error;

type AuthorizedRead<F> = F extends (...args: infer A) => infer R
  ? (authority: ThreadReadAuthority, ...args: A) => R
  : never;

export interface ThreadManagementServiceShape {
  readonly searchThreadStream: (
    authority: ThreadReadAuthority,
    input: OrchestrationV2SearchThreadInput,
  ) => Stream.Stream<OrchestrationV2SearchThreadResult, Orchestrator.OrchestratorV2Error>;
  readonly searchThread: (
    authority: ThreadReadAuthority,
    input: OrchestrationV2SearchThreadInput,
  ) => Effect.Effect<OrchestrationV2SearchThreadResult, Orchestrator.OrchestratorV2Error>;
  readonly ensureLegacyTranscript: (
    authority: ThreadReadAuthority,
    threadId: ThreadId,
  ) => Effect.Effect<void, LegacyV1ThreadImporter.LegacyV1ThreadImportError>;
  readonly dispatch: (
    authority: MutationAuthority,
    command: OrchestrationV2ServerCommand,
  ) => Effect.Effect<Orchestrator.OrchestratorV2DispatchResult, Orchestrator.OrchestratorV2Error>;
  readonly getThreadHistoryPage: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getThreadHistoryPage"]
  >;
  readonly getTimelinePage: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getTimelinePage"]
  >;
  readonly getMessageCount: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getMessageCount"]
  >;
  /**
   * One turn item with the input and output the thread stream withholds,
   * bounded for the wire. Clients fetch it when a tool row is expanded.
   */
  readonly getTurnItem: (
    authority: ThreadReadAuthority,
    input: {
      readonly threadId: ThreadId;
      readonly itemId: TurnItemId;
    },
  ) => Effect.Effect<OrchestrationV2GetTurnItemResult, Orchestrator.OrchestratorV2Error>;
  readonly getThreadRecords: <K extends ProjectionRecordField>(
    authority: ThreadReadAuthority,
    threadId: ThreadId,
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) => Effect.Effect<ProjectionRecords<K>, Orchestrator.OrchestratorV2Error>;
  readonly getReadWorkspaceRoots: (
    authority: ThreadReadAuthority,
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<string>, Orchestrator.OrchestratorV2Error>;
  readonly getThreadProjection: (
    authority: ThreadReadAuthority,
    threadId: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadProjection, Orchestrator.OrchestratorV2Error>;
  readonly getCheckpointContext: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getCheckpointContext"]
  >;
  readonly getThreadSnapshot: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getThreadSnapshot"]
  >;
  readonly getThreadSnapshotWindow: AuthorizedRead<
    Orchestrator.OrchestratorV2["Service"]["getThreadSnapshotWindow"]
  >;
  readonly getProjectThreadRecords: <K extends ProjectionRecordField>(
    input: { readonly projectId: ProjectId; readonly threadId: ThreadId },
    fields: ReadonlyArray<K>,
    filter?: ProjectionRecordFilter,
  ) => Effect.Effect<
    ProjectionRecords<K>,
    ThreadManagementProjectionLoadError | ThreadManagementThreadNotFoundError
  >;
  readonly getProjectThread: (input: {
    readonly projectId: ProjectId;
    readonly threadId: ThreadId;
  }) => Effect.Effect<OrchestrationV2ThreadProjection, ThreadManagementError>;
  readonly getShellSnapshot: (options?: {
    readonly projectIds?: ReadonlyArray<ProjectId>;
    readonly location?: "active" | "archive";
  }) => Effect.Effect<OrchestrationV2ThreadShellSnapshot, Orchestrator.OrchestratorV2Error>;
  readonly readShellSnapshot: (options?: {
    readonly projectIds?: ReadonlyArray<ProjectId>;
    readonly location?: "active" | "archive";
  }) => Effect.Effect<
    Effect.Effect<OrchestrationV2ThreadShellSnapshot, Orchestrator.OrchestratorV2Error>,
    Orchestrator.OrchestratorV2Error
  >;
  readonly getThreadShell: Orchestrator.OrchestratorV2["Service"]["getThreadShell"];
  readonly listProjectThreads: (input: {
    readonly projectId: ProjectId;
    readonly includeSubagents: boolean;
  }) => Effect.Effect<ReadonlyArray<OrchestrationV2ThreadShell>, ThreadManagementError>;
  readonly sendToThread: (
    authority: MutationAuthority,
    input: ThreadManagementSendInput,
  ) => Effect.Effect<ThreadManagementSendResult, ThreadManagementFailure>;
  readonly waitForThread: (
    input: ThreadManagementWaitInput,
  ) => Effect.Effect<ThreadManagementWaitResult, ThreadManagementError>;
  /**
   * Waits for `runId` to end, then settles the thread if the run completed.
   * Settling stops the provider session, so an agent that settles its own
   * thread uses this to wait for its turn to end. A run that ends any other
   * way leaves the thread active, as does anything `thread.settle` refuses
   * then, such as a queued message.
   */
  readonly settleAfterRun: (
    authority: MutationAuthority,
    input: {
      readonly projectId: ProjectId;
      readonly threadId: ThreadId;
      readonly runId: RunId;
    },
  ) => Effect.Effect<void, ThreadManagementFailure>;
  /**
   * Settles a thread. When the thread's own agent asks, it is mid-turn, so
   * the thread settles through `settleAfterRun` once that turn ends.
   */
  readonly settleThread: (
    authority: MutationAuthority,
    input: {
      readonly threadId: ThreadId;
      readonly commandId: CommandId;
      readonly byOwnAgent: boolean;
    },
  ) => Effect.Effect<
    { readonly sequence: number } | { readonly settlesWhenTurnEnds: true },
    Orchestrator.OrchestratorV2Error
  >;
  readonly interruptThread: (
    authority: MutationAuthority,
    input: ThreadManagementInterruptInput,
  ) => Effect.Effect<ThreadManagementInterruptResult, ThreadManagementFailure>;
  /**
   * Sends `thread.stop` to every delegated task under a thread, depth first. Their command IDs
   * derive from `commandId`, so a retry repeats nothing that already stopped. A task that
   * cannot be stopped does not keep the others running: all are tried, then it fails.
   */
  readonly stopDelegatedTasks: (
    authority: MutationAuthority,
    input: {
      readonly threadId: ThreadId;
      readonly commandId: CommandId;
      readonly reason?: string | undefined;
    },
  ) => Effect.Effect<void, Orchestrator.OrchestratorV2Error>;
  readonly getThreadEventSequence: Orchestrator.OrchestratorV2["Service"]["getThreadEventSequence"];
  readonly recoverDelegatedTask: Orchestrator.OrchestratorV2["Service"]["recoverDelegatedTask"];
  readonly delegatedTaskResultPending: Orchestrator.OrchestratorV2["Service"]["delegatedTaskResultPending"];
  readonly streamStoredEvents: Orchestrator.OrchestratorV2["Service"]["streamStoredEvents"];
  readonly streamStoredEventsFrom: Orchestrator.OrchestratorV2["Service"]["streamStoredEventsFrom"];
  readonly streamDomainEvents: Orchestrator.OrchestratorV2["Service"]["streamDomainEvents"];
}

export class ThreadManagementService extends Context.Service<
  ThreadManagementService,
  ThreadManagementServiceShape
>()("t3/orchestration-v2/ThreadManagementService") {}

export function isActiveRun(run: OrchestrationV2Run): boolean {
  return (
    run.status === "preparing" ||
    run.status === "starting" ||
    run.status === "running" ||
    run.status === "waiting"
  );
}

export function isTerminalRunStatus(
  status: OrchestrationV2Run["status"],
): status is ThreadManagementTerminalRunStatus {
  return (
    status === "completed" ||
    status === "failed" ||
    status === "cancelled" ||
    status === "interrupted" ||
    status === "rolled_back"
  );
}

export function latestRun(
  projection: Pick<OrchestrationV2ThreadProjection, "runs">,
): OrchestrationV2Run | undefined {
  return projection.runs.toSorted((left, right) => right.ordinal - left.ordinal)[0];
}

export function latestActiveRun(
  projection: Pick<OrchestrationV2ThreadProjection, "runs">,
): OrchestrationV2Run | undefined {
  return projection.runs
    .filter(isActiveRun)
    .toSorted((left, right) => right.ordinal - left.ordinal)[0];
}

function latestSteerableRun(
  projection: Pick<OrchestrationV2ThreadProjection, "runs" | "providerTurns">,
): OrchestrationV2Run | undefined {
  return projection.runs
    .filter(
      (run) =>
        run.status === "running" &&
        run.activeAttemptId !== null &&
        projection.providerTurns.some(
          (turn) => turn.runAttemptId === run.activeAttemptId && turn.status === "running",
        ),
    )
    .toSorted((left, right) => right.ordinal - left.ordinal)[0];
}

const SETTLE_AFTER_RUN_WAIT_MS = 24 * 60 * 60 * 1_000;

const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sql = yield* SqlClient.SqlClient;
  const layerScope = yield* Effect.scope;
  const legacyImporter = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;

  const hydrateLegacyTranscript = Effect.fn(
    "orchestrationV2.threadManagement.ensureLegacyTranscript",
  )(function* (threadId: ThreadId) {
    yield* legacyImporter.ensureTranscript(threadId).pipe(
      Effect.tapError((cause) =>
        Effect.logWarning("Unable to hydrate migrated v1 thread transcript", {
          threadId,
          cause,
        }),
      ),
    );
  });

  const ensureProjectionTranscript = (threadId: ThreadId) =>
    hydrateLegacyTranscript(threadId).pipe(
      Effect.mapError(
        (cause) =>
          new Orchestrator.OrchestratorProjectionError({
            threadId,
            cause,
          }),
      ),
    );

  const ensureCommandTranscripts = Effect.fn(
    "orchestrationV2.threadManagement.ensureCommandTranscripts",
  )(function* (command: OrchestrationV2ServerCommand) {
    yield* Effect.forEach(
      existingThreadIdsForCommand(command),
      (threadId) => hydrateLegacyTranscript(threadId),
      { discard: true },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new Orchestrator.OrchestratorDispatchError({
            commandId: command.commandId,
            commandType: command.type,
            cause,
          }),
      ),
    );
  });

  const read = <A>(
    authority: ThreadReadAuthority,
    threadId: ThreadId,
    operation: Effect.Effect<A, Orchestrator.OrchestratorV2Error>,
    anchorThreadId?: ThreadId,
  ) =>
    Effect.gen(function* () {
      const check = authorizeThreadRead(sql, authority, threadId, anchorThreadId).pipe(
        Effect.mapError(
          (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
        ),
      );
      yield* check;
      yield* ensureProjectionTranscript(threadId);
      // Membership and source graph cannot change between final admission and the read.
      return yield* sql
        .withTransaction(check.pipe(Effect.andThen(operation)))
        .pipe(
          Effect.mapError((cause) =>
            Schema.is(Orchestrator.OrchestratorProjectionError)(cause)
              ? cause
              : new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
          ),
        );
    });

  const getThreadProjection: ThreadManagementServiceShape["getThreadProjection"] = (
    authority,
    threadId,
  ) =>
    read(
      authority,
      threadId,
      orchestrator.getThreadProjection(threadId).pipe(
        Effect.flatMap((projection) => redactReadThreadMetadata(sql, authority, projection)),
        Effect.mapError(
          (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
        ),
      ),
    );
  const getCheckpointContext: ThreadManagementServiceShape["getCheckpointContext"] = (
    authority,
    threadId,
  ) => read(authority, threadId, orchestrator.getCheckpointContext(threadId));
  const getThreadSnapshot: ThreadManagementServiceShape["getThreadSnapshot"] = (
    authority,
    threadId,
  ) =>
    read(
      authority,
      threadId,
      orchestrator.getThreadSnapshot(threadId).pipe(
        Effect.flatMap((snapshot) =>
          redactReadThreadMetadata(sql, authority, snapshot.projection).pipe(
            Effect.map((projection) => ({ ...snapshot, projection })),
          ),
        ),
        Effect.mapError(
          (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
        ),
      ),
    );
  const getThreadSnapshotWindow: ThreadManagementServiceShape["getThreadSnapshotWindow"] = (
    authority,
    threadId,
    options,
  ) =>
    read(
      authority,
      threadId,
      orchestrator.getThreadSnapshotWindow(threadId, options).pipe(
        Effect.flatMap((snapshot) =>
          redactReadThreadMetadata(sql, authority, snapshot.projection).pipe(
            Effect.map((projection) => ({ ...snapshot, projection })),
          ),
        ),
        Effect.mapError(
          (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
        ),
      ),
      options.anchorThreadId,
    );

  const dispatch: ThreadManagementServiceShape["dispatch"] = (authority, command) =>
    authorizeCommandAdmission(sql, { authority, command }, command.commandId).pipe(
      Effect.andThen(ensureCommandTranscripts(command)),
      Effect.andThen(orchestrator.dispatch(authority, command)),
      Effect.mapError((cause) =>
        isOrchestratorError(cause)
          ? cause
          : new Orchestrator.OrchestratorDispatchError({
              commandId: command.commandId,
              commandType: command.type,
              cause,
            }),
      ),
    );

  const getProjectThread: ThreadManagementServiceShape["getProjectThread"] = (input) =>
    getThreadProjection(trustedReadAuthority, input.threadId).pipe(
      Effect.mapError(
        (cause) =>
          new ThreadManagementProjectionLoadError({
            projectId: input.projectId,
            threadId: input.threadId,
            cause,
          }),
      ),
      Effect.flatMap((projection) =>
        projection.thread.projectId === input.projectId && projection.thread.deletedAt === null
          ? Effect.succeed(projection)
          : Effect.fail(
              new ThreadManagementThreadNotFoundError({
                projectId: input.projectId,
                threadId: input.threadId,
              }),
            ),
      ),
    );

  const getProjectThreadRecords: ThreadManagementServiceShape["getProjectThreadRecords"] = (
    input,
    fields,
    filter,
  ) =>
    ensureProjectionTranscript(input.threadId)
      .pipe(Effect.andThen(orchestrator.getThreadRecords(input.threadId, fields, filter)))
      .pipe(
        Effect.mapError(
          (cause) =>
            new ThreadManagementProjectionLoadError({
              projectId: input.projectId,
              threadId: input.threadId,
              cause,
            }),
        ),
        Effect.flatMap((projection) =>
          projection.thread.projectId === input.projectId && projection.thread.deletedAt === null
            ? Effect.succeed(projection)
            : Effect.fail(
                new ThreadManagementThreadNotFoundError({
                  projectId: input.projectId,
                  threadId: input.threadId,
                }),
              ),
        ),
      );

  const listProjectThreads: ThreadManagementServiceShape["listProjectThreads"] = (input) =>
    orchestrator.getShellSnapshot().pipe(
      Effect.mapError(
        (cause) =>
          new ThreadManagementProjectThreadsListError({
            projectId: input.projectId,
            cause,
          }),
      ),
      Effect.map((snapshot) =>
        snapshot.threads
          .filter((thread) => thread.projectId === input.projectId)
          .filter(
            (thread) =>
              input.includeSubagents || thread.lineage.relationshipToParent !== "subagent",
          )
          .toSorted(
            (left, right) =>
              DateTime.toEpochMillis(right.updatedAt) - DateTime.toEpochMillis(left.updatedAt) ||
              right.id.localeCompare(left.id),
          ),
      ),
    );

  const sendToThread: ThreadManagementServiceShape["sendToThread"] = (authority, input) =>
    Effect.gen(function* () {
      const command = {
        type: "message.dispatch",
        commandId: input.commandId,
        threadId: input.threadId,
        messageId: input.messageId,
        ...(input.scheduledTaskId === undefined ? {} : { scheduledTaskId: input.scheduledTaskId }),
        ...(input.senderThreadId === undefined ? {} : { senderThreadId: input.senderThreadId }),
        text: input.text,
        attachments: input.attachments,
        ...(input.modelSelection === undefined ? {} : { modelSelection: input.modelSelection }),
        dispatchMode: { type: "start_immediately" },
        createdBy: input.createdBy,
        creationSource: input.creationSource,
      } satisfies OrchestrationV2Command;
      yield* authorizeCommandAdmission(sql, { authority, command }, input.commandId).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
        ),
      );
      const target = yield* getProjectThreadRecords(input, ["runs", "providerTurns"]);
      if (target.thread.archivedAt !== null) {
        return yield* new ThreadManagementThreadArchivedError({
          threadId: input.threadId,
        });
      }

      const steerableRun = latestSteerableRun(target);
      let dispatchMode: Extract<
        OrchestrationV2Command,
        { readonly type: "message.dispatch" }
      >["dispatchMode"];
      if (input.mode === "steer" || input.mode === "restart") {
        if (steerableRun === undefined) {
          return yield* new ThreadManagementNoSteerableRunError({
            threadId: input.threadId,
            mode: input.mode,
          });
        }
        dispatchMode = {
          type: input.mode === "steer" ? "steer_active" : "restart_active",
          targetRunId: steerableRun.id,
        };
      } else if (input.mode === "auto" && steerableRun !== undefined) {
        dispatchMode = { type: "steer_active", targetRunId: steerableRun.id };
      } else {
        dispatchMode = {
          type: input.mode === "queue" ? "queue_after_active" : "start_immediately",
        };
      }

      const dispatch = yield* orchestrator.dispatch(authority, { ...command, dispatchMode });
      yield* authorizeCommandReceipt(sql, authority, input.commandId).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
        ),
      );
      const projection = yield* getProjectThreadRecords(input, ["runs", "messages", "turnItems"], {
        messageIds: [input.messageId],
        turnItemTypes: ["user_message"],
      });
      const message = projection.messages.find((candidate) => candidate.id === input.messageId);
      const run =
        message?.runId === null || message?.runId === undefined
          ? undefined
          : projection.runs.find((candidate) => candidate.id === message.runId);
      const turnItem =
        projection.turnItems.find(
          (
            candidate,
          ): candidate is Extract<OrchestrationV2TurnItem, { readonly type: "user_message" }> =>
            candidate.type === "user_message" && candidate.messageId === input.messageId,
        ) ?? null;
      // A queued message's user turn item is deliberately not emitted at
      // dispatch time — it materializes when the queued turn actually starts,
      // so it can map onto the provider turn. Every other dispatch mode still
      // produces its turn item transactionally with the run.
      if (
        message === undefined ||
        run === undefined ||
        (turnItem === null && run.status !== "queued")
      ) {
        return yield* new ThreadManagementDurableRunProjectionError({
          threadId: input.threadId,
          messageId: input.messageId,
        });
      }
      const delivery: ThreadManagementSendResult["delivery"] =
        turnItem === null || turnItem.inputIntent === "queued_turn"
          ? "queued"
          : turnItem.inputIntent === "turn_start"
            ? "started"
            : input.mode === "restart"
              ? "restarted"
              : "steered";
      return { dispatch, projection, message, run, turnItem, delivery };
    });

  const waitForThread: ThreadManagementServiceShape["waitForThread"] = (input) =>
    Effect.gen(function* () {
      const loadError = (cause: unknown) =>
        new ThreadManagementProjectionLoadError({
          projectId: input.projectId,
          threadId: input.threadId,
          cause,
        });
      // Taken before the first read, so a run update between that read and
      // the subscription below still replays.
      const afterSequence = yield* orchestrator
        .getThreadEventSequence(input.threadId)
        .pipe(Effect.mapError(loadError));
      const target = yield* getProjectThreadRecords(input, ["runs"]);
      const selectedRun =
        input.runId === undefined
          ? latestRun(target)
          : target.runs.find((candidate) => candidate.id === input.runId);
      if (input.runId !== undefined && selectedRun === undefined) {
        return yield* new ThreadManagementRunNotFoundError({
          threadId: input.threadId,
          runId: input.runId,
        });
      }
      if (selectedRun === undefined) {
        return { threadId: input.threadId, run: null, timedOut: false };
      }
      if (isTerminalRunStatus(selectedRun.status)) {
        return { threadId: input.threadId, run: selectedRun, timedOut: false };
      }

      // Re-read the run only when the thread records a run update or its
      // deletion, instead of polling the projection for up to an hour. Each
      // stream keeps one event type, so transcript events never fill its buffer.
      const wait = Stream.merge(
        orchestrator.streamStoredEventsFrom({
          threadId: input.threadId,
          afterSequence,
          eventType: "run.updated",
        }),
        orchestrator.streamStoredEventsFrom({
          threadId: input.threadId,
          afterSequence,
          eventType: "thread.deleted",
        }),
      ).pipe(
        Stream.mapError(loadError),
        Stream.filter(
          (stored) =>
            stored.event.type !== "run.updated" || stored.event.payload.id === selectedRun.id,
        ),
        Stream.mapEffect(() =>
          getProjectThreadRecords(input, ["runs"], { runIds: [selectedRun.id] }).pipe(
            Effect.flatMap((current) => {
              const run = current.runs.find((candidate) => candidate.id === selectedRun.id);
              return run === undefined
                ? Effect.fail(
                    new ThreadManagementRunNotFoundError({
                      threadId: input.threadId,
                      runId: selectedRun.id,
                    }),
                  )
                : Effect.succeed(run);
            }),
          ),
        ),
        Stream.filter((run) => isTerminalRunStatus(run.status)),
        Stream.runHead,
        Effect.timeoutOption(Duration.millis(Math.max(1, input.timeoutMs))),
      );
      const waited = Option.flatten(yield* wait);
      if (Option.isSome(waited)) {
        return { threadId: input.threadId, run: waited.value, timedOut: false };
      }
      const current = yield* getProjectThreadRecords(input, ["runs"], { runIds: [selectedRun.id] });
      const run = current.runs.find((candidate) => candidate.id === selectedRun.id);
      if (run === undefined) {
        return yield* new ThreadManagementRunNotFoundError({
          threadId: input.threadId,
          runId: selectedRun.id,
        });
      }
      // The run may have reached a terminal status while the timeout was
      // winning the race; the final projection read decides what actually
      // happened, so only a still-active run counts as timed out.
      return { threadId: input.threadId, run, timedOut: !isTerminalRunStatus(run.status) };
    });

  const settleAfterRun: ThreadManagementServiceShape["settleAfterRun"] = Effect.fn(
    "orchestrationV2.threadManagement.settleAfterRun",
  )(function* (authority, input) {
    yield* authorizeThreadMutation(sql, authority, input.threadId, input.runId).pipe(
      Effect.mapError(
        (cause) =>
          new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
      ),
    );
    // A turn that outlives this wait leaves its thread active.
    const { run } = yield* waitForThread({ ...input, timeoutMs: SETTLE_AFTER_RUN_WAIT_MS });
    if (run?.status !== "completed") return;
    yield* dispatch(authority, {
      type: "thread.settle",
      commandId: CommandId.make(`server:settle-after-run:${input.runId}`),
      threadId: input.threadId,
    });
  });

  const settleThread: ThreadManagementServiceShape["settleThread"] = Effect.fn(
    "orchestrationV2.threadManagement.settleThread",
  )(function* (authority, input) {
    yield* authorizeThreadMutation(sql, authority, input.threadId).pipe(
      Effect.mapError(
        (cause) =>
          new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
      ),
    );
    const shell = input.byOwnAgent ? yield* orchestrator.getThreadShell(input.threadId) : null;
    if (shell != null && shell.activeRunId !== null) {
      yield* settleAfterRun(authority, {
        projectId: shell.projectId,
        threadId: shell.id,
        runId: shell.activeRunId,
      }).pipe(Effect.ignoreCause({ log: true }), Effect.forkIn(layerScope));
      return { settlesWhenTurnEnds: true } as const;
    }
    const result = yield* dispatch(authority, {
      type: "thread.settle",
      commandId: input.commandId,
      threadId: input.threadId,
    });
    return { sequence: result.sequence };
  });

  const interruptThread: ThreadManagementServiceShape["interruptThread"] = (authority, input) =>
    Effect.gen(function* () {
      yield* authorizeThreadMutation(sql, authority, input.threadId, input.runId).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
        ),
      );
      yield* authorizeCommandReceipt(sql, authority, input.commandId).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
        ),
      );
      const target = yield* getProjectThreadRecords(input, ["runs", "providerTurns"]);
      const explicitRun =
        input.runId === undefined
          ? undefined
          : target.runs.find((candidate) => candidate.id === input.runId);
      if (input.runId !== undefined && explicitRun === undefined) {
        return yield* new ThreadManagementRunNotFoundError({
          threadId: input.threadId,
          runId: input.runId,
        });
      }
      if (explicitRun !== undefined && isTerminalRunStatus(explicitRun.status)) {
        return {
          type: "already_terminal",
          run: explicitRun as OrchestrationV2Run & {
            readonly status: ThreadManagementTerminalRunStatus;
          },
        } as const;
      }
      const interruptibleRun = latestActiveRun(target);
      if (interruptibleRun === undefined) {
        if (input.runId === undefined) {
          return { type: "no_active_run" } as const;
        }
        return yield* new ThreadManagementThreadNotInterruptibleError({
          threadId: input.threadId,
          runId: input.runId,
        });
      }
      if (input.runId !== undefined && interruptibleRun.id !== input.runId) {
        return yield* new ThreadManagementThreadNotInterruptibleError({
          threadId: input.threadId,
          runId: input.runId,
        });
      }
      const dispatch = yield* orchestrator.dispatch(authority, {
        type: "run.interrupt",
        commandId: input.commandId,
        threadId: input.threadId,
        runId: interruptibleRun.id,
        ...(input.reason === undefined ? {} : { reason: input.reason }),
      });
      return { type: "interrupt_requested", run: interruptibleRun, dispatch } as const;
    });

  const stopDelegatedTasks: ThreadManagementServiceShape["stopDelegatedTasks"] = (
    authority,
    input,
  ) =>
    Effect.gen(function* () {
      yield* authorizeThreadMutation(sql, authority, input.threadId).pipe(
        Effect.mapError(
          (cause) =>
            new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
        ),
      );
      const { subagents } = yield* orchestrator.getThreadRecords(input.threadId, ["subagents"]);
      const failures: Array<Orchestrator.OrchestratorV2Error> = [];
      for (const task of subagents) {
        if (task.origin !== "app_owned" || task.childThreadId === null) continue;
        const threadId = task.childThreadId;
        yield* dispatch(authority, {
          type: "thread.stop",
          commandId: CommandId.make(`${input.commandId}:stop:${threadId}`),
          threadId,
          ...(input.reason === undefined ? {} : { reason: input.reason }),
        }).pipe(
          Effect.andThen(stopDelegatedTasks(authority, { ...input, threadId })),
          Effect.catch((error) =>
            Effect.logWarning("Unable to stop a delegated task", {
              parentThreadId: input.threadId,
              threadId,
              error,
            }).pipe(Effect.andThen(Effect.sync(() => failures.push(error)))),
          ),
        );
      }
      if (failures[0] !== undefined) return yield* Effect.fail(failures[0]);
    });

  return ThreadManagementService.of({
    ensureLegacyTranscript: (authority, threadId) =>
      authorizeThreadRead(sql, authority, threadId).pipe(
        Effect.mapError(
          (cause) =>
            new LegacyV1ThreadImporter.LegacyV1ThreadImportError({
              threadId,
              operation: "hydrate transcript for",
              cause,
            }),
        ),
        Effect.andThen(hydrateLegacyTranscript(threadId)),
      ),
    searchThreadStream: (authority, input) =>
      Stream.unwrap(
        read(authority, input.threadId, Effect.succeed(orchestrator.searchThreadStream(input))),
      ).pipe(
        // Search may hydrate bodies incrementally after the first result.
        Stream.mapEffect((result) => read(authority, input.threadId, Effect.succeed(result))),
      ),
    searchThread: (authority, input) =>
      read(authority, input.threadId, orchestrator.searchThread(input)),
    dispatch,
    getThreadHistoryPage: (authority, threadId, cursor, throughEntryId, conversationOnly) =>
      Effect.try({
        try: () => ThreadId.make(decodeThreadHistoryCursor(cursor).st),
        catch: (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
      }).pipe(
        Effect.flatMap((anchor) =>
          read(
            authority,
            threadId,
            orchestrator.getThreadHistoryPage(threadId, cursor, throughEntryId, conversationOnly),
            anchor,
          ),
        ),
      ),
    getTimelinePage: (authority, threadId, options) =>
      read(authority, threadId, orchestrator.getTimelinePage(threadId, options)),
    getMessageCount: (authority, threadId) =>
      read(authority, threadId, orchestrator.getMessageCount(threadId)),
    getTurnItem: (authority, input) =>
      read(authority, input.threadId, orchestrator.getTurnItem(input)).pipe(
        Effect.map((item) => ({ item: item === null ? null : projectTurnItemForDetail(item) })),
      ),
    getThreadRecords: (authority, threadId, fields, filter) =>
      read(authority, threadId, orchestrator.getThreadRecords(threadId, fields, filter)),
    getReadWorkspaceRoots: (authority, threadId) =>
      read(
        authority,
        threadId,
        sql<{ readonly workspace_root: string; readonly worktree_path: string | null }>`
        SELECT p.workspace_root, json_extract(t.payload_json, '$.worktreePath') AS worktree_path
        FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
        WHERE t.thread_id = ${threadId} AND p.deleted_at IS NULL
      `.pipe(
          Effect.map((rows) =>
            rows.flatMap((row) =>
              row.worktree_path === null
                ? [row.workspace_root]
                : [row.workspace_root, row.worktree_path],
            ),
          ),
          Effect.mapError(
            (cause) => new Orchestrator.OrchestratorProjectionError({ threadId, cause }),
          ),
        ),
      ),
    getThreadProjection,
    getCheckpointContext,
    getThreadSnapshot,
    getThreadSnapshotWindow,
    getProjectThreadRecords,
    getProjectThread,
    getShellSnapshot: orchestrator.getShellSnapshot,
    readShellSnapshot: orchestrator.readShellSnapshot,
    getThreadShell: orchestrator.getThreadShell,
    listProjectThreads,
    sendToThread,
    waitForThread,
    settleAfterRun,
    settleThread,
    interruptThread,
    stopDelegatedTasks,
    getThreadEventSequence: orchestrator.getThreadEventSequence,
    recoverDelegatedTask: orchestrator.recoverDelegatedTask,
    delegatedTaskResultPending: orchestrator.delegatedTaskResultPending,
    streamStoredEvents: orchestrator.streamStoredEvents,
    streamStoredEventsFrom: orchestrator.streamStoredEventsFrom,
    streamDomainEvents: orchestrator.streamDomainEvents,
  });
});

const layerLegacyV1ThreadImporterNoop = Layer.succeed(
  LegacyV1ThreadImporter.LegacyV1ThreadImporter,
  LegacyV1ThreadImporter.LegacyV1ThreadImporter.of({
    pendingThreadCount: Effect.succeed(0),
    reconcileShells: Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
    ensureTranscript: () => Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
    importPendingTranscripts: Effect.succeed({ importedThreadCount: 0, importedMessageCount: 0 }),
  }),
);

export const layer: Layer.Layer<
  ThreadManagementService,
  never,
  Orchestrator.OrchestratorV2 | SqlClient.SqlClient
> = Layer.effect(ThreadManagementService, make).pipe(
  Layer.provide(layerLegacyV1ThreadImporterNoop),
);

export const layerWithLegacyImporter: Layer.Layer<
  ThreadManagementService,
  never,
  LegacyV1ThreadImporter.LegacyV1ThreadImporter | Orchestrator.OrchestratorV2 | SqlClient.SqlClient
> = Layer.effect(ThreadManagementService, make);
