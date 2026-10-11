import type { WorkspaceActionAdmission } from "./workspaceReadAccess.ts";
import {
  AuthOrchestrationOperateScope,
  AuthSourceControlWriteScope,
  EnvironmentAuthorizationError,
  ProjectId,
  ThreadId,
  resourceGrantAllowsProject,
  type AuthResourceGrant,
  type CommandId,
  type RunId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/sql/SqlClient";
import {
  authorizeNodeIds,
  authorizeOwnerThreadIds,
  authorizeProviderIds,
  authorizeProviderTurnIds,
  authorizeThreadResources,
  type ThreadReadAuthority,
  trustedReadAuthority,
} from "./ResourceAuthorization.ts";
import { deriveProviderThread } from "../orchestration-v2/IdAllocator.ts";
import type { ProjectCommand } from "../orchestration-v2/ProjectCommands.ts";
import type { CommandReceiptV2 } from "../orchestration-v2/CommandReceiptStore.ts";
import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";

const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

/** App-owned work opts in explicitly; external calls must reload device authority. */
export const trustedMutationAuthority = Symbol("trusted mutation");
const registeredCommandWorkspace = Symbol("canonical launch workspace");
type RegisteredCommandWorkspace = {
  readonly commandId: CommandId;
  readonly projectId: ProjectId;
  readonly threadId: ThreadId;
  readonly path: string;
  readonly roots: ReadonlyArray<string>;
};
export type MutationAuthority =
  | typeof trustedMutationAuthority
  | (Exclude<ThreadReadAuthority, typeof trustedReadAuthority> & {
      readonly [registeredCommandWorkspace]?: RegisteredCommandWorkspace;
    });

/** Only a canonical-checked launch claim carries this exact command/path association. */
export const withRegisteredCommandWorkspace = (
  authority: MutationAuthority,
  workspace: RegisteredCommandWorkspace,
): MutationAuthority =>
  authority === trustedMutationAuthority
    ? authority
    : { ...authority, [registeredCommandWorkspace]: workspace };
const sourceControlLink = Symbol("checked source-control link");
export type SourceControlLinkAuthority = {
  readonly [sourceControlLink]: {
    readonly workspace: WorkspaceActionAdmission;
    readonly command: Extract<
      OrchestrationV2ServerCommand,
      { readonly type: "thread.pull-request.link" }
    >;
  };
};
export type CommandAuthority = MutationAuthority | SourceControlLinkAuthority;
/** A checked Git action may link only its original thread, using the existing command lock/commit. */
export const checkedSourceControlLinkAuthority = (
  workspace: WorkspaceActionAdmission,
  command: Extract<OrchestrationV2ServerCommand, { readonly type: "thread.pull-request.link" }>,
): SourceControlLinkAuthority => ({ [sourceControlLink]: { workspace, command } });
export const isCheckedSourceControlLinkAuthority = (authority: CommandAuthority) =>
  typeof authority === "object" && sourceControlLink in authority;
const linkAdmission = (authority: CommandAuthority) =>
  typeof authority === "object" && sourceControlLink in authority
    ? authority[sourceControlLink]
    : undefined;
/** Filesystem admission stays outside SQL, after the existing thread lock is acquired. */
export const beforeSourceControlLinkEffect = (authority: CommandAuthority) => {
  const link = linkAdmission(authority);
  return link === undefined ? Effect.void : link.workspace.beforeEffect(link.workspace.input.cwd);
};
const sourceControlDenied = () =>
  new EnvironmentAuthorizationError({
    requiredScope: AuthSourceControlWriteScope,
    message: "This connection cannot link this Git action's pull request.",
  });
const authorizeSourceControlLink = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  authority: SourceControlLinkAuthority,
  commandId: CommandId,
  command?: OrchestrationV2ServerCommand,
) {
  const { workspace, command: original } = authority[sourceControlLink];
  if (
    original.type !== "thread.pull-request.link" ||
    original.source !== "created" ||
    workspace.scope !== AuthSourceControlWriteScope ||
    workspace.input.threadId !== original.threadId ||
    original.commandId !== commandId ||
    (command !== undefined && command !== original)
  )
    return yield* sourceControlDenied();
  yield* workspace.checkSql;
  if (workspace.authority === trustedReadAuthority) return;
  const current = yield* workspace.authority.current;
  if (!current.scopes.includes(AuthSourceControlWriteScope)) return yield* sourceControlDenied();
  const owner = yield* sql<{ readonly project: string }>`
    SELECT t.project_id AS project FROM orchestration_v2_projection_threads t
    JOIN projection_projects p ON p.project_id = t.project_id
    WHERE t.thread_id = ${original.threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL
  `;
  if (
    owner.length !== 1 ||
    owner[0]!.project !== workspace.projectId ||
    !resourceGrantAllowsProject(current.resourceGrant, ProjectId.make(owner[0]!.project))
  )
    return yield* sourceControlDenied();
  const receipts = yield* sql<{
    readonly kind: string;
    readonly id: string;
    readonly type: string;
  }>`
    SELECT aggregate_kind AS kind, aggregate_id AS id, command_type AS type FROM orchestration_command_receipts WHERE command_id = ${commandId}
    UNION SELECT aggregate_kind, stream_id, 'thread.pull-request.link' FROM orchestration_events WHERE command_id = ${commandId}
  `;
  if (
    receipts.some(
      (r) =>
        r.kind !== "thread" || r.id !== original.threadId || r.type !== "thread.pull-request.link",
    )
  )
    return yield* sourceControlDenied();
  yield* workspace.checkSql;
});
export type CommandAdmission =
  | typeof trustedMutationAuthority
  | {
      readonly authority: CommandAuthority;
      readonly command: OrchestrationV2ServerCommand;
      readonly projectId?: ProjectId;
    };
export type ProjectAdmission = {
  readonly authority: MutationAuthority;
  readonly command: ProjectCommand;
};

const encodeIds = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
export const mutationDenied = () =>
  new EnvironmentAuthorizationError({
    requiredScope: AuthOrchestrationOperateScope,
    message: "This connection cannot mutate the requested thread resource.",
  });

/** Compiler coverage is independent of the transcript hydration helper. */
export function commandTargets(command: OrchestrationV2ServerCommand) {
  switch (command.type) {
    case "thread.create":
      return {
        thread: command.threadId,
        threads: [],
        createdThread: command.threadId,
        project: command.projectId,
      };
    case "thread.fork":
      return {
        thread: command.targetThreadId,
        threads: [command.sourceThreadId],
        createdThread: command.targetThreadId,
      };
    case "thread.merge_back":
      return {
        thread: command.targetThreadId,
        threads: [command.sourceThreadId, command.targetThreadId],
      };
    case "thread.created.record":
      return {
        thread: command.parentThreadId,
        threads: [command.parentThreadId, command.targetThreadId],
      };
    case "delegated_task.request":
    case "delegated_task.wake-policy":
    case "delegated_task.completion-delivery.acknowledge":
    case "delegated_task.completion-delivery.dispose":
      return { thread: command.parentThreadId, threads: [command.parentThreadId] };
    case "thread.archive":
    case "thread.unarchive":
    case "thread.delete":
    case "thread.settle":
    case "thread.auto-settle":
    case "thread.unsettle":
    case "thread.snooze":
    case "thread.unsnooze":
    case "thread.auto-settle.set":
    case "thread.pin":
    case "thread.unpin":
    case "thread.pin.reorder":
    case "thread.active.reorder":
    case "thread.visit":
    case "thread.mark-unread":
    case "thread.metadata.update":
    case "thread.pull-request.link":
    case "thread.pull-request.unlink":
    case "thread.pull-request-link.sync":
    case "thread.pull-request.watch":
    case "thread.pull-request.sync":
    case "thread.title.regeneration.complete":
    case "thread.runtime-mode.set":
    case "thread.interaction-mode.set":
    case "thread.model-selection.set":
    case "provider-session.detach":
    case "message.dispatch":
    case "prepared-run.release":
    case "notification.delivery.accept":
    case "prepared-run.progress":
    case "prepared-run.fail":
    case "prepared-run.retry":
    case "run.interrupt":
    case "queued-message.promote-to-steer":
    case "queue.resume":
    case "queued-run.reorder":
    case "queued-run.cancel":
    case "queued-run.edit":
    case "runtime-request.respond":
    case "thread.user-input.dismiss":
    case "checkpoint.rollback":
    case "provider.switch":
    case "thread.pull-request-watch.sync":
    case "checkpoint.rollback.fail":
    case "thread.background-work.settle":
    case "thread.stop":
    case "secret_request.record":
      return { thread: command.threadId, threads: [command.threadId] };
  }
  const exhaustive: never = command;
  return exhaustive;
}

const currentMutationGrant = Effect.fnUntraced(function* (authority: MutationAuthority) {
  if (authority === trustedMutationAuthority) return undefined;
  if (!authority?.current) return yield* mutationDenied();
  const current = yield* authority.current;
  if (!current.resourceGrant || !current.scopes.includes(AuthOrchestrationOperateScope))
    return yield* mutationDenied();
  return current.resourceGrant;
});

/** Rejected receipts do not retain original source participants; opaque diagnostics are not admission. */
export const receiptForMutationAuthority = Effect.fnUntraced(
  function* <T extends Pick<CommandReceiptV2, "status" | "error">>(
    authority: CommandAuthority,
    receipt: T,
  ) {
    if (receipt.status !== "rejected") return receipt;
    const link = linkAdmission(authority);
    const grant =
      link === undefined
        ? yield* currentMutationGrant(authority as MutationAuthority)
        : link.workspace.authority === trustedReadAuthority
          ? undefined
          : (yield* link.workspace.authority.current).resourceGrant;
    return grant !== undefined && (grant.projects.mode !== "all" || grant.providers.mode !== "all")
      ? { ...receipt, error: "Previously rejected." }
      : receipt;
  },
  Effect.mapError((error) => (isEnvironmentAuthorizationError(error) ? error : mutationDenied())),
);

const authorizeProjects = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
  includeDeleted = false,
) {
  if (ids.length === 0) return;
  const unique = [...new Set(ids)];
  const rows = yield* sql<{ readonly id: string }>`
    SELECT project_id AS id FROM projection_projects
    WHERE project_id IN (SELECT value FROM json_each(${encodeIds(unique)})) AND (${includeDeleted} OR deleted_at IS NULL)
  `;
  if (
    rows.length !== unique.length ||
    rows.some((r) => !resourceGrantAllowsProject(grant, ProjectId.make(r.id)))
  )
    return yield* mutationDenied();
});

/** Root/bootstrap creation has no safe restricted project association. */
export const requireUnrestrictedMutation = Effect.fnUntraced(function* (
  authority: MutationAuthority,
) {
  const grant = yield* currentMutationGrant(authority);
  if (grant !== undefined && (grant.projects.mode !== "all" || grant.providers.mode !== "all"))
    return yield* mutationDenied();
});

/** Raw project policy runs before normalization, root lookup or enrichment. */
export const authorizeProjectCommand = Effect.fnUntraced(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    command: {
      readonly type: ProjectCommand["type"];
      readonly commandId: CommandId;
      readonly projectId: ProjectId;
      readonly workspaceRoot?: string | undefined;
      readonly scripts?: unknown;
    },
  ) {
    const grant = yield* currentMutationGrant(authority);
    if (grant === undefined) return;
    const restricted = grant.projects.mode !== "all" || grant.providers.mode !== "all";
    if (
      restricted &&
      (command.type === "project.create" ||
        (command.type === "project.meta.update" &&
          (command.workspaceRoot !== undefined || command.scripts !== undefined)))
    )
      return yield* mutationDenied();
    // Own deleted rows remain usable for deletion receipts/retries. Missing IDs never become grants.
    if (restricted) yield* authorizeProjects(sql, grant, [command.projectId], true);
    yield* authorizeCommandReceipt(sql, authority, command.commandId, true, true);
  },
  Effect.mapError(() => mutationDenied()),
);

export const authorizeProjectAdmission = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  admission: ProjectAdmission,
  target: {
    readonly commandId: CommandId;
    readonly projectId: ProjectId;
    readonly commandType: string;
  },
) {
  if (
    !admission?.authority ||
    !admission.command ||
    admission.command.commandId !== target.commandId ||
    admission.command.projectId !== target.projectId ||
    admission.command.type !== target.commandType
  )
    return yield* mutationDenied();
  yield* authorizeProjectCommand(sql, admission.authority, admission.command);
});

/** Pure SQL association shared by raw commands and launch/retry canonical path admission. */
export const registeredMutationRoots = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  target: { readonly projectId: ProjectId; readonly threadId?: ThreadId },
) {
  const roots = yield* sql<{ readonly root: string | null }>`
    SELECT workspace_root AS root FROM projection_projects WHERE project_id = ${target.projectId} AND deleted_at IS NULL
    UNION SELECT json_extract(payload_json, '$.worktreePath') FROM orchestration_v2_projection_threads
      WHERE thread_id = ${target.threadId ?? ""} AND project_id = ${target.projectId} AND deleted_at IS NULL
  `;
  return roots.flatMap((row) => (row.root === null ? [] : [row.root]));
});

const requireRegisteredCommandPath = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  target: { readonly projectId: ProjectId; readonly threadId: ThreadId },
  requested: string,
  authority: MutationAuthority,
  commandId: CommandId,
) {
  const foreign = yield* sql`SELECT project_id FROM projection_projects
    WHERE workspace_root = ${requested} AND project_id != ${target.projectId} LIMIT 1`;
  if (foreign.length > 0) return yield* mutationDenied();
  const roots = yield* registeredMutationRoots(sql, target);
  if (roots.includes(requested)) return;
  const admitted =
    authority === trustedMutationAuthority ? undefined : authority[registeredCommandWorkspace];
  if (
    admitted === undefined ||
    admitted.commandId !== commandId ||
    admitted.projectId !== target.projectId ||
    admitted.threadId !== target.threadId ||
    admitted.path !== requested ||
    !admitted.roots.some((root) => roots.includes(root))
  )
    return yield* mutationDenied();
});

/** Direct command bindings use only an already registered root or this thread's workspace.
 * Launch claims may also carry a canonical-checked child path, tied to current SQL roots.
 * New worktrees are bound by the app-owned preparation path, which opts into trust explicitly.
 */
const authorizeCommandWorkspace = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  command: OrchestrationV2ServerCommand,
  authority: MutationAuthority,
) {
  if (grant.projects.mode === "all" && grant.providers.mode === "all") return;
  if (
    command.type !== "thread.create" &&
    command.type !== "thread.metadata.update" &&
    command.type !== "thread.pull-request.sync"
  )
    return;
  const paths = [
    "worktreePath" in command ? command.worktreePath : undefined,
    ...(command.type === "thread.metadata.update" ? [command.expectedWorktreePath] : []),
    ...(command.type === "thread.pull-request.sync" ? [command.expected.worktreePath] : []),
  ];
  if (paths.every((path) => path == null)) return;
  const owner =
    command.type === "thread.create"
      ? command.projectId
      : (yield* sql<{
          readonly project: string;
        }>`SELECT project_id AS project FROM orchestration_v2_projection_threads
      WHERE thread_id = ${command.threadId}`)[0]?.project;
  if (owner === undefined) return yield* mutationDenied();
  for (const path of new Set(paths))
    if (path != null)
      yield* requireRegisteredCommandPath(
        sql,
        { projectId: ProjectId.make(owner), threadId: command.threadId },
        path,
        authority,
        command.commandId,
      );
});

/** Only static projection table/column names from this module reach this resolver. */
const authorizeRecords = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  table: string,
  column: string,
  ids: ReadonlyArray<string>,
  includeDeleted = false,
) {
  if (ids.length === 0) return;
  const rows = yield* sql<{ readonly owner: string | null }>`
    SELECT r.thread_id AS owner FROM json_each(${encodeIds([...new Set(ids)])}) refs
    LEFT JOIN ${sql(table)} r ON r.${sql(column)} = refs.value
  `;
  if (rows.some((r) => r.owner === null)) return yield* mutationDenied();
  yield* authorizeOwnerThreadIds(
    sql,
    grant,
    rows.flatMap((r) => (r.owner === null ? [] : [ThreadId.make(r.owner)])),
    includeDeleted,
  );
});

const authorizeExistingRecord = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  table: string,
  column: string,
  id: string,
) {
  const rows = yield* sql<{
    readonly owner: string;
  }>`SELECT thread_id AS owner FROM ${sql(table)} WHERE ${sql(column)} = ${id}`;
  yield* authorizeOwnerThreadIds(
    sql,
    grant,
    rows.map((r) => ThreadId.make(r.owner)),
  );
});

const authorizeSession = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  id: string,
  includeDeleted = false,
  allowUnmaterialized = false,
) {
  const registered = yield* sql<{ readonly id: string }>`
    SELECT provider_session_id AS id FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = ${id}
  `;
  // Deferred launches register the provider-thread owner before the runtime creates its session row.
  const owners = yield* sql<{ readonly id: string }>`
    SELECT thread_id AS id FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id = ${id} AND thread_id IS NOT NULL
    UNION SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id = ${id}
    UNION SELECT thread_id FROM orchestration_v2_projection_provider_threads WHERE provider_session_id = ${id} AND thread_id IS NOT NULL
  `;
  if (owners.length === 0 || (!allowUnmaterialized && registered.length === 0))
    return yield* mutationDenied();
  yield* authorizeOwnerThreadIds(
    sql,
    grant,
    owners.map((r) => ThreadId.make(r.id)),
    includeDeleted,
  );
});

const authorizeAttachments = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
  includeDeleted = false,
  creatingThread?: ThreadId,
) {
  const segments = new Set<string>();
  for (const id of ids) {
    const segment = parseThreadSegmentFromAttachmentId(id);
    if (segment === null) return yield* mutationDenied();
    // Staged upload ownership belongs to ticket06; claims admit their target and all other sources.
    if (segment !== PENDING_ATTACHMENT_THREAD_SEGMENT) segments.add(segment);
  }
  if (segments.size === 0) return;
  // ponytail: O(registered threads) segment resolution; canonical attachment ownership can replace it in ticket06.
  const rows = yield* sql<{
    readonly id: string;
  }>`SELECT thread_id AS id FROM orchestration_v2_projection_threads`;
  for (const segment of segments) {
    const owners = rows.filter((r) => toSafeThreadAttachmentSegment(r.id) === segment);
    if (
      owners.length === 0 &&
      creatingThread !== undefined &&
      toSafeThreadAttachmentSegment(creatingThread) === segment
    )
      continue;
    // Normalization is not invertible: a colliding or unregistered segment is not authority.
    if (owners.length !== 1) return yield* mutationDenied();
    yield* authorizeOwnerThreadIds(
      sql,
      grant,
      owners.map((r) => ThreadId.make(r.id)),
      includeDeleted,
    );
  }
});

/** Materialization also includes legacy PR aliases, copied plans and claimed attachments. */
const authorizeMutationThread = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  threadId: ThreadId,
  includeDeleted = false,
) {
  const sources = yield* authorizeThreadResources(
    sql,
    grant,
    threadId,
    undefined,
    true,
    includeDeleted,
  );
  for (const id of sources) {
    const projects = yield* sql<{ readonly id: string | null }>`
      SELECT json_extract(payload_json, '$.linkedPullRequest.projectId') AS id FROM orchestration_v2_projection_threads WHERE thread_id = ${id}
      UNION SELECT json_extract(payload_json, '$.branchPullRequest.projectId') FROM orchestration_v2_projection_threads WHERE thread_id = ${id}
    `;
    yield* authorizeProjects(
      sql,
      grant,
      projects.flatMap((r) => (r.id === null ? [] : [r.id])),
    );
    const plans = yield* sql<{ readonly id: string | null }>`
      SELECT json_extract(payload_json, '$.sourcePlanRef.planId') AS id FROM orchestration_v2_projection_runs WHERE thread_id = ${id}
    `;
    yield* authorizeRecords(
      sql,
      grant,
      "orchestration_v2_projection_plans",
      "plan_id",
      plans.flatMap((r) => (r.id === null ? [] : [r.id])),
      includeDeleted,
    );
    const attachments = yield* sql<{ readonly id: string | null }>`
      SELECT json_extract(a.value, '$.id') AS id FROM orchestration_v2_projection_messages m, json_each(m.payload_json, '$.attachments') a WHERE m.thread_id = ${id}
      UNION SELECT json_extract(c.value, '$.attachmentId') FROM orchestration_v2_projection_messages m, json_each(m.payload_json, '$.context.records') c
        WHERE m.thread_id = ${id} AND json_extract(c.value, '$.kind') IN ('image', 'file')
      UNION SELECT json_extract(a.value, '$.id') FROM orchestration_v2_projection_turn_items t,
        json_each(t.payload_json, '$.questionAnswer.attachmentsByQuestionId') q, json_each(q.value) a WHERE t.thread_id = ${id}
      UNION SELECT json_extract(a.value, '$.id') FROM projection_thread_messages m, json_each(m.attachments_json) a WHERE m.thread_id = ${id}
    `;
    yield* authorizeAttachments(
      sql,
      grant,
      attachments.flatMap((r) => (r.id === null ? [] : [r.id])),
      includeDeleted,
    );
  }
});

const authorizeContext = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  context: Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>["context"],
  creatingThread?: ThreadId,
) {
  for (const record of context?.records ?? []) {
    if (record.kind === "thread" && "threadId" in record)
      yield* authorizeMutationThread(sql, grant, record.threadId);
    if ((record.kind === "image" || record.kind === "file") && "attachmentId" in record)
      yield* authorizeAttachments(sql, grant, [record.attachmentId], false, creatingThread);
  }
});

const authorizeTasks = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
  includeDeleted = false,
) {
  yield* authorizeRecords(
    sql,
    grant,
    "orchestration_v2_projection_subagents",
    "subagent_id",
    ids,
    includeDeleted,
  );
  if (ids.length === 0) return;
  const children = yield* sql<{ readonly id: string | null }>`
    SELECT json_extract(payload_json, '$.childThreadId') AS id
    FROM orchestration_v2_projection_subagents WHERE subagent_id IN (SELECT value FROM json_each(${encodeIds(ids)}))
  `;
  for (const row of children)
    if (row.id !== null)
      yield* authorizeMutationThread(sql, grant, ThreadId.make(row.id), includeDeleted);
});

export const authorizeThreadMutation = Effect.fnUntraced(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    threadId: ThreadId,
    runId?: RunId,
  ) {
    const grant = yield* currentMutationGrant(authority);
    if (grant !== undefined) {
      yield* authorizeMutationThread(sql, grant, threadId);
      if (runId !== undefined)
        yield* authorizeRecords(sql, grant, "orchestration_v2_projection_runs", "run_id", [runId]);
    }
  },
  Effect.mapError(() => mutationDenied()),
);

/** A new target needs a live allowed project; an existing id must prove its actual owner. */
export const authorizeMutationTarget = Effect.fnUntraced(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    input: {
      readonly threadId?: ThreadId;
      readonly projectId: ProjectId;
      readonly allowCreate?: boolean;
    },
  ) {
    const grant = yield* currentMutationGrant(authority);
    if (grant === undefined) return;
    yield* authorizeProjects(sql, grant, [input.projectId]);
    if (input.threadId === undefined) return;
    const rows = yield* sql<{ readonly project: string }>`
      SELECT project_id AS project FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}
    `;
    if (rows.length === 0 && input.allowCreate === true) return;
    if (rows.length !== 1 || rows[0]!.project !== input.projectId) return yield* mutationDenied();
    yield* authorizeMutationThread(sql, grant, input.threadId);
  },
  Effect.mapError(() => mutationDenied()),
);

/** Claim admission includes the original command, not only the attachments in this copy batch. */
export const authorizeAttachmentClaim = Effect.fnUntraced(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    input: {
      readonly threadId: string;
      readonly attachmentIds: ReadonlyArray<string>;
      readonly command: OrchestrationV2ServerCommand;
      readonly additionalCommands?: ReadonlyArray<OrchestrationV2ServerCommand>;
      readonly creatingThread?: { readonly threadId: ThreadId; readonly projectId: ProjectId };
    },
  ) {
    if (commandTargets(input.command).thread !== input.threadId) return yield* mutationDenied();
    yield* authorizeCommand(sql, authority, input.command, input.creatingThread);
    yield* authorizeCommandReceipt(sql, authority, input.command.commandId);
    const receipt = yield* sql<{ readonly owner: string; readonly status: string }>`
    SELECT aggregate_id AS owner, status FROM orchestration_command_receipts WHERE command_id = ${input.command.commandId}
  `;
    if (receipt.some((row) => row.status === "rejected" || row.owner !== input.threadId))
      return yield* mutationDenied();
    if (
      input.creatingThread !== undefined &&
      !(input.additionalCommands ?? []).some(
        (command) =>
          command.type === "thread.create" &&
          command.threadId === input.creatingThread?.threadId &&
          command.projectId === input.creatingThread.projectId,
      )
    )
      return yield* mutationDenied();
    for (const command of input.additionalCommands ?? []) {
      yield* authorizeCommand(sql, authority, command);
      yield* authorizeCommandReceipt(sql, authority, command.commandId);
      const receipt = yield* sql<{ readonly owner: string; readonly status: string }>`
      SELECT aggregate_id AS owner, status FROM orchestration_command_receipts WHERE command_id = ${command.commandId}
    `;
      if (receipt.some((row) => row.status === "rejected" || row.owner !== input.threadId))
        return yield* mutationDenied();
    }
    const grant = yield* currentMutationGrant(authority);
    if (grant !== undefined)
      yield* authorizeAttachments(
        sql,
        grant,
        input.attachmentIds,
        false,
        input.creatingThread?.threadId,
      );
  },
  Effect.mapError(() => mutationDenied()),
);

/** All supplied and stored participants precede importer/planning and are rechecked at commit. */
export const authorizeCommand = Effect.fn("resourceAuthorization.command")(
  function* (
    sql: SqlClient.SqlClient,
    authority: CommandAuthority,
    command: OrchestrationV2ServerCommand,
    creatingThread?: { readonly threadId: ThreadId; readonly projectId: ProjectId },
  ) {
    if (linkAdmission(authority) !== undefined)
      return yield* authorizeSourceControlLink(
        sql,
        authority as SourceControlLinkAuthority,
        command.commandId,
        command,
      );
    const mutationAuthority = authority as MutationAuthority;
    const grant = yield* currentMutationGrant(mutationAuthority);
    if (grant === undefined) return;
    if (command.type === "thread.create" && command.importedNativeThread !== undefined) {
      // An unregistered native session has no project owner. Restricted callers cannot claim it.
      if (grant.projects.mode !== "all" || grant.providers.mode !== "all")
        return yield* mutationDenied();
      const id = deriveProviderThread({
        driver: command.importedNativeThread.ref.driver,
        providerInstanceId: command.modelSelection.instanceId,
        nativeThreadId: command.importedNativeThread.ref.nativeId,
      });
      const rows = yield* sql<{
        readonly id: string;
      }>`SELECT provider_thread_id AS id FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = ${id}`;
      if (rows.length > 0) yield* authorizeProviderIds(sql, grant, [id]);
    }
    if (creatingThread !== undefined) {
      if (command.type !== "message.dispatch" || command.threadId !== creatingThread.threadId)
        return yield* mutationDenied();
      yield* authorizeMutationTarget(sql, mutationAuthority, {
        ...creatingThread,
        allowCreate: true,
      });
    }
    const targets = commandTargets(command);
    if (targets.project !== undefined) yield* authorizeProjects(sql, grant, [targets.project]);
    for (const id of new Set(targets.threads)) {
      if (id === creatingThread?.threadId) continue;
      yield* authorizeMutationThread(sql, grant, id, command.type === "thread.delete");
    }
    if (targets.createdThread !== undefined) {
      const existing = yield* sql<{ readonly id: string }>`
      SELECT thread_id AS id FROM orchestration_v2_projection_threads WHERE thread_id = ${targets.createdThread}
    `;
      if (existing.length > 0) yield* authorizeMutationThread(sql, grant, targets.createdThread);
      if (command.type === "thread.create")
        yield* authorizeMutationTarget(sql, mutationAuthority, {
          threadId: command.threadId,
          projectId: command.projectId,
          allowCreate: true,
        });
    }
    yield* authorizeCommandWorkspace(sql, grant, command, mutationAuthority);
    const runIds = [
      ...("runId" in command ? [command.runId] : []),
      ...("parentRunId" in command ? [command.parentRunId] : []),
      ...("targetRunId" in command && command.targetRunId !== null ? [command.targetRunId] : []),
      ...("queuedRunId" in command ? [command.queuedRunId] : []),
      ...("beforeRunId" in command && command.beforeRunId !== null ? [command.beforeRunId] : []),
      ...("observedByRunId" in command && command.observedByRunId !== null
        ? [command.observedByRunId]
        : []),
    ];
    if (command.type === "message.dispatch") {
      yield* authorizeExistingRecord(
        sql,
        grant,
        "orchestration_v2_projection_messages",
        "message_id",
        command.messageId,
      );
      if ("targetRunId" in command.dispatchMode) runIds.push(command.dispatchMode.targetRunId);
      for (const id of [
        command.restartContinuationOfRunId,
        command.usageLimitContinuationOfRunId,
        command.manualContinuationOfRunId,
      ])
        if (id !== undefined) runIds.push(id);
      if (command.delegatedCompletion) {
        runIds.push(command.delegatedCompletion.parentRunId);
        yield* authorizeTasks(sql, grant, command.delegatedCompletion.taskIds);
      }
      if (command.senderThreadId !== undefined)
        yield* authorizeMutationThread(sql, grant, command.senderThreadId);
      if (command.sourcePlanRef !== undefined) {
        yield* authorizeMutationThread(sql, grant, command.sourcePlanRef.threadId);
        yield* authorizeRecords(sql, grant, "orchestration_v2_projection_plans", "plan_id", [
          command.sourcePlanRef.planId,
        ]);
      }
      if (command.notification?.source.kind === "delegated_task")
        yield* authorizeTasks(sql, grant, command.notification.source.taskIds);
      if (
        command.notification &&
        "childThreadId" in command.notification.source &&
        command.notification.source.childThreadId !== undefined
      )
        yield* authorizeMutationThread(sql, grant, command.notification.source.childThreadId);
    }
    yield* authorizeRecords(sql, grant, "orchestration_v2_projection_runs", "run_id", runIds);
    yield* authorizeNodeIds(sql, grant, [
      ...("nodeId" in command ? [command.nodeId] : []),
      ...("parentNodeId" in command ? [command.parentNodeId] : []),
    ]);
    if ("taskId" in command) yield* authorizeTasks(sql, grant, [command.taskId]);
    if ("sourcePoint" in command) {
      if (command.sourcePoint.type === "run")
        yield* authorizeRecords(sql, grant, "orchestration_v2_projection_runs", "run_id", [
          command.sourcePoint.runId,
        ]);
      if (command.sourcePoint.type === "checkpoint")
        yield* authorizeRecords(
          sql,
          grant,
          "orchestration_v2_projection_checkpoints",
          "checkpoint_id",
          [command.sourcePoint.checkpointId],
        );
    }
    if (command.type === "provider-session.detach")
      yield* authorizeSession(sql, grant, command.providerSessionId);
    if (command.type === "thread.background-work.settle") {
      yield* authorizeProviderIds(sql, grant, [command.providerThreadId]);
      yield* authorizeProviderTurnIds(sql, grant, [command.providerTurnId]);
    }
    if (command.type === "runtime-request.respond" || command.type === "thread.user-input.dismiss")
      yield* authorizeRecords(
        sql,
        grant,
        "orchestration_v2_projection_runtime_requests",
        "runtime_request_id",
        [command.requestId],
      );
    if (command.type === "secret_request.record")
      yield* authorizeExistingRecord(
        sql,
        grant,
        "orchestration_v2_projection_turn_items",
        "turn_item_id",
        command.turnItemId,
      );
    if (command.type === "notification.delivery.accept")
      yield* authorizeRecords(sql, grant, "orchestration_v2_projection_messages", "message_id", [
        command.messageId,
      ]);
    if (command.type === "checkpoint.rollback") {
      yield* authorizeRecords(
        sql,
        grant,
        "orchestration_v2_projection_checkpoint_scopes",
        "scope_id",
        [command.scopeId],
      );
      yield* authorizeRecords(
        sql,
        grant,
        "orchestration_v2_projection_checkpoints",
        "checkpoint_id",
        [command.checkpointId],
      );
    }
    if ("projectId" in command) yield* authorizeProjects(sql, grant, [command.projectId]);
    if (command.type === "thread.metadata.update" || command.type === "thread.pull-request.sync") {
      yield* authorizeProjects(sql, grant, [
        ...(command.linkedPullRequest == null ? [] : [command.linkedPullRequest.projectId]),
        ...(command.type === "thread.pull-request.sync"
          ? [
              ...(command.branchPullRequest === null ? [] : [command.branchPullRequest.projectId]),
              ...(command.expected.linkedPullRequest === null
                ? []
                : [command.expected.linkedPullRequest.projectId]),
              ...(command.expected.branchPullRequest === null
                ? []
                : [command.expected.branchPullRequest.projectId]),
            ]
          : []),
      ]);
    }
    if (command.type === "message.dispatch" || command.type === "queued-run.edit") {
      yield* authorizeContext(sql, grant, command.context, creatingThread?.threadId);
      yield* authorizeAttachments(
        sql,
        grant,
        (command.attachments ?? []).map((a) => a.id),
        false,
        creatingThread?.threadId,
      );
    }
    if (command.type === "runtime-request.respond")
      yield* authorizeAttachments(
        sql,
        grant,
        Object.values(command.attachmentsByQuestionId ?? {})
          .flat()
          .map((a) => a.id),
      );
  },
  Effect.mapError((error) => (isEnvironmentAuthorizationError(error) ? error : mutationDenied())),
);

/** Inspect raw receipt participants before EventStore decodes any stored payload. */
export const authorizeCommandReceipt = Effect.fn("resourceAuthorization.commandReceipt")(
  function* (
    sql: SqlClient.SqlClient,
    authority: CommandAuthority,
    commandId: CommandId,
    includeDeleted = true,
    includeDeletedProject = false,
  ) {
    if (linkAdmission(authority) !== undefined)
      return yield* authorizeSourceControlLink(
        sql,
        authority as SourceControlLinkAuthority,
        commandId,
      );
    const mutationAuthority = authority as MutationAuthority;
    const grant = yield* currentMutationGrant(mutationAuthority);
    if (grant === undefined) return;
    const owners = yield* sql<{ readonly kind: string; readonly id: string }>`
    SELECT aggregate_kind AS kind, aggregate_id AS id FROM orchestration_command_receipts WHERE command_id = ${commandId}
    UNION SELECT aggregate_kind, stream_id FROM orchestration_events WHERE command_id = ${commandId}
  `;
    for (const owner of owners) {
      if (owner.kind === "thread")
        yield* authorizeMutationThread(sql, grant, ThreadId.make(owner.id), includeDeleted);
      else if (owner.kind === "project")
        yield* authorizeProjects(sql, grant, [owner.id], includeDeletedProject);
      else return yield* mutationDenied();
    }
    if (
      includeDeletedProject &&
      (grant.projects.mode !== "all" || grant.providers.mode !== "all")
    ) {
      const retargets = yield* sql`
        SELECT sequence FROM orchestration_events WHERE command_id = ${commandId} AND aggregate_kind = 'project'
          AND (event_type = 'project.created' OR json_type(payload_json, '$.workspaceRoot') IS NOT NULL
            OR json_type(payload_json, '$.scripts') IS NOT NULL)
      `;
      if (retargets.length > 0) return yield* mutationDenied();
    }
    const projectReferences = yield* sql<{ readonly id: string | null }>`
      SELECT json_extract(payload_json, '$.projectId') AS id FROM orchestration_events
      WHERE command_id = ${commandId} AND aggregate_kind = 'project'
    `;
    yield* authorizeProjects(
      sql,
      grant,
      projectReferences.flatMap((row) => (row.id === null ? [] : [row.id])),
      includeDeletedProject,
    );
    if (grant.projects.mode !== "all" || grant.providers.mode !== "all") {
      const paths = yield* sql<{
        readonly thread: string;
        readonly project: string;
        readonly path: string;
      }>`
        SELECT e.stream_id AS thread, t.project_id AS project, json_extract(e.payload_json, '$.worktreePath') AS path
        FROM orchestration_events e JOIN orchestration_v2_projection_threads t ON t.thread_id = e.stream_id
        WHERE e.command_id = ${commandId} AND e.aggregate_kind = 'thread'
          AND e.event_type IN ('thread.created', 'thread.metadata-updated')
          AND json_extract(e.payload_json, '$.worktreePath') IS NOT NULL
      `;
      for (const path of paths)
        yield* requireRegisteredCommandPath(
          sql,
          { projectId: ProjectId.make(path.project), threadId: ThreadId.make(path.thread) },
          path.path,
          mutationAuthority,
          commandId,
        );
    }
    const references = yield* sql<{ readonly kind: string; readonly id: string }>`
    WITH payloads AS (SELECT event_type, payload_json AS p FROM orchestration_events WHERE command_id = ${commandId} AND aggregate_kind = 'thread'),
    threads AS (
      SELECT json_extract(p, '$.threadId') AS id FROM payloads UNION SELECT json_extract(p, '$.appThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.senderThreadId') FROM payloads UNION SELECT json_extract(p, '$.sourceThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.targetThreadId') FROM payloads UNION SELECT json_extract(p, '$.sourcePlanRef.threadId') FROM payloads
      UNION SELECT json_extract(p, '$.sourcePoint.threadId') FROM payloads UNION SELECT json_extract(p, '$.basePoint.threadId') FROM payloads
      UNION SELECT json_extract(p, '$.forkedFrom.threadId') FROM payloads UNION SELECT json_extract(p, '$.childThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.source.threadId') FROM payloads UNION SELECT json_extract(p, '$.source.childThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.notification.source.childThreadId') FROM payloads
      UNION SELECT json_extract(c.value, '$.threadId') FROM payloads, json_each(p, '$.context.records') c WHERE json_extract(c.value, '$.kind') = 'thread'
      UNION SELECT json_extract(m.value, '$.threadId') FROM payloads, json_each(p, '$.history.messages') m
      UNION SELECT json_extract(b.value, '$.childThreadId') FROM payloads, json_each(p, '$.pendingBackgroundTasks') b WHERE json_extract(b.value, '$.kind') = 'subagent'
    ), projects AS (
      SELECT json_extract(p, '$.projectId') AS id FROM payloads UNION SELECT json_extract(p, '$.linkedPullRequest.projectId') FROM payloads UNION SELECT json_extract(p, '$.branchPullRequest.projectId') FROM payloads
    ), nodes AS (
      SELECT json_extract(p, '$.nodeId') AS id FROM payloads UNION SELECT json_extract(p, '$.rootNodeId') FROM payloads
      UNION SELECT json_extract(p, '$.parentNodeId') FROM payloads UNION SELECT json_extract(p, '$.ownerNodeId') FROM payloads
      UNION SELECT json_extract(p, '$.source.nodeId') FROM payloads UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type = 'node.updated'
    ), providers AS (
      SELECT json_extract(p, '$.providerThreadId') AS id FROM payloads UNION SELECT json_extract(p, '$.activeProviderThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.forkedFrom.providerThreadId') FROM payloads UNION SELECT json_extract(p, '$.source.providerThreadId') FROM payloads
      UNION SELECT json_extract(p, '$.toProviderThreadId') FROM payloads UNION SELECT f.value FROM payloads, json_each(p, '$.fromProviderThreadIds') f
      UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type = 'provider-thread.updated'
    ), turns AS (
      SELECT json_extract(p, '$.providerTurnId') AS id FROM payloads UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type = 'provider-turn.updated'
    ), plans AS (SELECT json_extract(p, '$.sourcePlanRef.planId') AS id FROM payloads),
    attachments AS (
      SELECT json_extract(a.value, '$.id') AS id FROM payloads, json_each(p, '$.attachments') a
      UNION SELECT json_extract(c.value, '$.attachmentId') FROM payloads, json_each(p, '$.context.records') c WHERE json_extract(c.value, '$.kind') IN ('image', 'file')
      UNION SELECT json_extract(a.value, '$.id') FROM payloads, json_each(p, '$.questionAnswer.attachmentsByQuestionId') q, json_each(q.value) a
    ), tasks AS (
      SELECT t.value AS id FROM payloads, json_each(p, '$.notification.source.taskIds') t WHERE json_extract(p, '$.notification.source.kind') = 'delegated_task'
      UNION SELECT t.value FROM payloads, json_each(p, '$.source.taskIds') t WHERE json_extract(p, '$.source.kind') = 'delegated_task'
      UNION SELECT t.value FROM payloads, json_each(p, '$.delegatedCompletion.taskIds') t
    ), runs AS (
      SELECT json_extract(p, '$.runId') AS id FROM payloads UNION SELECT json_extract(p, '$.parentRunId') FROM payloads
      UNION SELECT json_extract(p, '$.delegatedCompletion.parentRunId') FROM payloads
      UNION SELECT json_extract(p, '$.sourcePoint.runId') FROM payloads UNION SELECT json_extract(p, '$.basePoint.runId') FROM payloads
      UNION SELECT json_extract(p, '$.source.runId') FROM payloads UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type IN ('run.created','run.updated')
    ), scopes AS (
      SELECT json_extract(p, '$.scopeId') AS id FROM payloads UNION SELECT json_extract(p, '$.parentScopeId') FROM payloads
      UNION SELECT json_extract(p, '$.checkpointScopeId') FROM payloads UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type = 'checkpoint-scope.created'
    ), checkpoints AS (
      SELECT json_extract(p, '$.checkpointId') AS id FROM payloads UNION SELECT json_extract(p, '$.parentCheckpointId') FROM payloads
      UNION SELECT json_extract(p, '$.sourcePoint.checkpointId') FROM payloads UNION SELECT json_extract(p, '$.basePoint.checkpointId') FROM payloads
      UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type = 'checkpoint.captured'
    ), sessions AS (
      SELECT json_extract(p, '$.providerSessionId') AS id FROM payloads
      UNION SELECT json_extract(p, '$.id') FROM payloads WHERE event_type IN ('provider-session.attached','provider-session.updated')
    )
    SELECT 'thread' AS kind, id FROM threads WHERE id IS NOT NULL
    UNION SELECT 'project', id FROM projects WHERE id IS NOT NULL
    UNION SELECT 'node', id FROM nodes WHERE id IS NOT NULL
    UNION SELECT 'provider', id FROM providers WHERE id IS NOT NULL
    UNION SELECT 'turn', id FROM turns WHERE id IS NOT NULL
    UNION SELECT 'plan', id FROM plans WHERE id IS NOT NULL
    UNION SELECT 'attachment', id FROM attachments WHERE id IS NOT NULL
    UNION SELECT 'task', id FROM tasks WHERE id IS NOT NULL
    UNION SELECT 'run', id FROM runs WHERE id IS NOT NULL
    UNION SELECT 'scope', id FROM scopes WHERE id IS NOT NULL
    UNION SELECT 'checkpoint', id FROM checkpoints WHERE id IS NOT NULL
    UNION SELECT 'session', id FROM sessions WHERE id IS NOT NULL
  `;
    for (const row of references) {
      switch (row.kind) {
        case "thread":
          yield* authorizeMutationThread(sql, grant, ThreadId.make(row.id), includeDeleted);
          break;
        case "project":
          yield* authorizeProjects(sql, grant, [row.id]);
          break;
        case "node":
          yield* authorizeNodeIds(sql, grant, [row.id], includeDeleted);
          break;
        case "provider":
          yield* authorizeProviderIds(sql, grant, [row.id], includeDeleted);
          break;
        case "turn":
          yield* authorizeProviderTurnIds(sql, grant, [row.id], includeDeleted);
          break;
        case "plan":
          yield* authorizeRecords(
            sql,
            grant,
            "orchestration_v2_projection_plans",
            "plan_id",
            [row.id],
            includeDeleted,
          );
          break;
        case "attachment":
          yield* authorizeAttachments(sql, grant, [row.id], includeDeleted);
          break;
        case "task":
          yield* authorizeTasks(sql, grant, [row.id], includeDeleted);
          break;
        case "run":
          yield* authorizeRecords(
            sql,
            grant,
            "orchestration_v2_projection_runs",
            "run_id",
            [row.id],
            includeDeleted,
          );
          break;
        case "scope":
          yield* authorizeRecords(
            sql,
            grant,
            "orchestration_v2_projection_checkpoint_scopes",
            "scope_id",
            [row.id],
            includeDeleted,
          );
          break;
        case "checkpoint":
          yield* authorizeRecords(
            sql,
            grant,
            "orchestration_v2_projection_checkpoints",
            "checkpoint_id",
            [row.id],
            includeDeleted,
          );
          break;
        case "session":
          yield* authorizeSession(sql, grant, row.id, includeDeleted, true);
          break;
        default:
          return yield* mutationDenied();
      }
    }
  },
  Effect.mapError((error) => (isEnvironmentAuthorizationError(error) ? error : mutationDenied())),
);

export const authorizeCommandAdmission = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  admission: CommandAdmission,
  commandId: CommandId,
  target?: { readonly threadId: ThreadId; readonly commandType: string },
) {
  if (admission === trustedMutationAuthority) return;
  if (!admission?.authority || !admission.command || admission.command.commandId !== commandId)
    return yield* mutationDenied();
  if (
    target &&
    (target.threadId !== commandTargets(admission.command).thread ||
      target.commandType !== admission.command.type)
  )
    return yield* mutationDenied();
  if (linkAdmission(admission.authority)) {
    yield* authorizeCommand(sql, admission.authority, admission.command);
    yield* authorizeCommandReceipt(sql, admission.authority, commandId);
    return;
  }
  if (admission.projectId !== undefined) {
    const rows = yield* sql<{ readonly project: string }>`
      SELECT t.project_id AS project FROM orchestration_v2_projection_threads t
      JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.thread_id = ${commandTargets(admission.command).thread} AND p.deleted_at IS NULL
    `;
    if (rows.length !== 1 || rows[0]!.project !== admission.projectId)
      return yield* mutationDenied();
    yield* authorizeMutationTarget(sql, admission.authority as MutationAuthority, {
      projectId: admission.projectId,
    });
  }
  yield* authorizeCommand(sql, admission.authority, admission.command);
  yield* authorizeCommandReceipt(sql, admission.authority, commandId);
});
