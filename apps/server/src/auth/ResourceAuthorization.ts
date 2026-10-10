import {
  AuthOrchestrationReadScope,
  EnvironmentAuthorizationError,
  type AuthResourceGrant,
  type AuthEnvironmentScope,
  ThreadId,
  resourceGrantAllowsProject,
  ProjectId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { permittedCollectionThreadIds } from "./projectCollectionAccess.ts";
import type * as SqlClient from "effect/sql/SqlClient";

const encodeIdList = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

/** Internal work must opt in; missing authority never means owner access. */
export const trustedReadAuthority = Symbol("trusted thread read");
export type ThreadReadAuthority =
  | typeof trustedReadAuthority
  | {
      readonly current: Effect.Effect<
        {
          readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
          readonly resourceGrant: AuthResourceGrant;
          readonly authorityRevision?: number;
        },
        EnvironmentAuthorizationError
      >;
    };

export const threadReadDenied = () =>
  new EnvironmentAuthorizationError({
    requiredScope: AuthOrchestrationReadScope,
    message: "This connection cannot read the requested thread resource.",
  });

const authorizeOwnerThreadIds = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  threadIds: ReadonlyArray<ThreadId>,
) {
  const ids = [...new Set(threadIds)];
  if (ids.length === 0) return;
  // Collection selectors admit tombstones; direct owners must still exist in a live project.
  const rows = yield* sql<{ readonly thread_id: string; readonly project_id: string }>`
    SELECT t.thread_id, t.project_id FROM json_each(${encodeIdList(ids)}) refs
    JOIN orchestration_v2_projection_threads t ON t.thread_id = refs.value
    JOIN projection_projects p ON p.project_id = t.project_id
    WHERE t.deleted_at IS NULL AND p.deleted_at IS NULL
  `;
  if (
    rows.length !== ids.length ||
    rows.some((row) => !resourceGrantAllowsProject(grant, ProjectId.make(row.project_id)))
  )
    return yield* threadReadDenied();
});

/** Resolve registered owners without reading provider/node bodies. Every declared owner must exist. */
const authorizeProviderIds = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
) {
  if (ids.length === 0) return;
  const rows = yield* sql<{
    readonly registered: string | null;
    readonly owner: string | null;
    readonly owner_node: string | null;
    readonly node_owner: string | null;
  }>`
    SELECT p.provider_thread_id AS registered, p.thread_id AS owner,
      p.owner_node_id AS owner_node, n.thread_id AS node_owner
    FROM json_each(${encodeIdList(ids)}) refs
    LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = refs.value
    LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
  `;
  if (
    rows.some(
      (row) =>
        row.registered === null ||
        (row.owner === null && row.node_owner === null) ||
        (row.owner_node !== null && row.node_owner === null),
    )
  )
    return yield* threadReadDenied();
  const owners = rows.flatMap((row) =>
    [row.owner, row.node_owner].flatMap((id) => (id === null ? [] : [ThreadId.make(id)])),
  );
  yield* authorizeOwnerThreadIds(sql, grant, owners);
});

const authorizeNodeIds = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
) {
  if (ids.length === 0) return;
  const rows = yield* sql<{ readonly owner: string | null }>`
    SELECT n.thread_id AS owner FROM json_each(${encodeIdList(ids)}) refs
    LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = refs.value
  `;
  if (rows.some((row) => row.owner === null)) return yield* threadReadDenied();
  const owners = rows.flatMap((row) => (row.owner === null ? [] : [ThreadId.make(row.owner)]));
  yield* authorizeOwnerThreadIds(sql, grant, owners);
});

const authorizeProviderTurnIds = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  grant: AuthResourceGrant,
  ids: ReadonlyArray<string>,
) {
  if (ids.length === 0) return;
  const rows = yield* sql<{
    readonly owner: string | null;
    readonly provider: string | null;
    readonly node: string | null;
  }>`
    SELECT t.thread_id AS owner, t.provider_thread_id AS provider, t.node_id AS node FROM json_each(${encodeIdList(ids)}) refs
    LEFT JOIN orchestration_v2_projection_provider_turns t ON t.provider_turn_id = refs.value
  `;
  if (rows.some((row) => row.owner === null || row.provider === null || row.node === null))
    return yield* threadReadDenied();
  yield* authorizeOwnerThreadIds(
    sql,
    grant,
    rows.flatMap((row) => (row.owner === null ? [] : [ThreadId.make(row.owner)])),
  );
  yield* authorizeProviderIds(
    sql,
    grant,
    rows.flatMap((row) => (row.provider === null ? [] : [row.provider])),
  );
  yield* authorizeNodeIds(
    sql,
    grant,
    rows.flatMap((row) => (row.node === null ? [] : [row.node])),
  );
});

/** Check membership before decoding any target/source transcript, including imported ancestry. */
export const authorizeThreadRead = Effect.fn("resourceAuthorization.threadRead")(function* (
  sql: SqlClient.SqlClient,
  authority: ThreadReadAuthority,
  threadId: ThreadId,
  anchorThreadId?: ThreadId,
  includeStoredSources = true,
) {
  if (authority === trustedReadAuthority) return undefined;
  if (!authority?.current) return yield* threadReadDenied();
  const current = yield* authority.current;
  if (!current.resourceGrant || !current.scopes.includes(AuthOrchestrationReadScope))
    return yield* threadReadDenied();
  const seen = new Set<ThreadId>();
  let id: ThreadId | undefined = threadId;
  while (id !== undefined && !seen.has(id)) {
    seen.add(id);
    // Membership columns precede JSON extraction: a hidden malformed payload cannot be decoded.
    yield* authorizeOwnerThreadIds(sql, current.resourceGrant, [id]);
    const forks: ReadonlyArray<{ readonly source: string | null }> = yield* sql<{
      readonly source: string | null;
    }>`
      SELECT CASE WHEN json_extract(payload_json, '$.forkedFrom.type') = 'run'
        THEN json_extract(payload_json, '$.forkedFrom.threadId') END AS source
      FROM orchestration_v2_projection_threads WHERE thread_id = ${id}
    `;
    id = forks[0]?.source == null ? undefined : ThreadId.make(forks[0].source);
  }
  if (includeStoredSources && current.resourceGrant?.projects.mode === "explicit") {
    // Typed source references can carry copied history/result text. Deny that materialization
    // as a whole rather than returning an apparently complete, silently censored transcript.
    for (const sourceId of seen) {
      // ProjectionStore also selects native provider threads through node/subagent/cohort
      // associations. Admit their registered owners before touching their JSON bodies.
      const providers = yield* sql<{ readonly id: string }>`
        SELECT provider_thread_id AS id FROM orchestration_v2_projection_provider_threads WHERE thread_id = ${sourceId}
        UNION SELECT p.provider_thread_id FROM orchestration_v2_projection_provider_threads p
          JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id WHERE n.thread_id = ${sourceId}
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_subagents WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_runs WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_provider_turns WHERE thread_id = ${sourceId}
        UNION SELECT provider_thread_id FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ${sourceId} AND provider_thread_id IS NOT NULL
        UNION SELECT json_extract(payload_json, '$.activeProviderThreadId') FROM orchestration_v2_projection_threads
          WHERE thread_id = ${sourceId} AND json_extract(payload_json, '$.activeProviderThreadId') IS NOT NULL
      `;
      yield* authorizeProviderIds(
        sql,
        current.resourceGrant,
        providers.map((row) => row.id),
      );
      const providerIds = encodeIdList(providers.map((row) => row.id));
      const providerSources = yield* sql<{ readonly id: string | null }>`
        SELECT json_extract(payload_json, '$.forkedFrom.providerThreadId') AS id
          FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id IN (SELECT value FROM json_each(${providerIds}))
        UNION SELECT r.value FROM orchestration_v2_projection_context_handoffs h,
          json_each(h.payload_json, '$.fromProviderThreadIds') r WHERE h.thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.toProviderThreadId') FROM orchestration_v2_projection_context_handoffs WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.source.providerThreadId') FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${sourceId} AND json_extract(payload_json, '$.source.type') = 'provider_thread'
      `;
      yield* authorizeProviderIds(
        sql,
        current.resourceGrant,
        providerSources.flatMap((row) => (row.id === null ? [] : [row.id])),
      );
      const turnSources = yield* sql<{ readonly id: string | null }>`
        SELECT provider_turn_id AS id FROM orchestration_v2_projection_provider_turns WHERE thread_id = ${sourceId}
        UNION SELECT provider_turn_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId}
        UNION SELECT provider_turn_id FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${sourceId}
        UNION SELECT provider_turn_id FROM orchestration_v2_projection_runtime_requests WHERE thread_id = ${sourceId}
        UNION SELECT provider_turn_id FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
      `;
      yield* authorizeProviderTurnIds(
        sql,
        current.resourceGrant,
        turnSources.flatMap((row) => (row.id === null ? [] : [row.id])),
      );
      const nodeSources = yield* sql<{ readonly id: string | null }>`
        SELECT json_extract(payload_json, '$.source.nodeId') AS id FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${sourceId} AND json_extract(payload_json, '$.source.type') = 'node'
        UNION SELECT node_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId}
        UNION SELECT parent_node_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId}
        UNION SELECT root_node_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.rootNodeId') FROM orchestration_v2_projection_runs WHERE thread_id = ${sourceId}
        UNION SELECT root_node_id FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${sourceId}
        UNION SELECT parent_node_id FROM orchestration_v2_projection_subagents WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_provider_turns WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_runtime_requests WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_messages WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_plans WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_checkpoints WHERE thread_id = ${sourceId}
        UNION SELECT node_id FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
      `;
      yield* authorizeNodeIds(
        sql,
        current.resourceGrant,
        nodeSources.flatMap((row) => (row.id === null ? [] : [row.id])),
      );
      const refs = yield* sql<{ readonly id: string | null }>`
        SELECT source_thread_id AS id FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT target_thread_id FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.sourcePoint.threadId') FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.basePoint.threadId') FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.sourcePlanRef.threadId') FROM orchestration_v2_projection_runs WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.appThreadId') FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id IN (SELECT value FROM json_each(${providerIds}))
        UNION SELECT json_extract(b.value, '$.childThreadId') FROM orchestration_v2_projection_provider_threads p,
          json_each(p.payload_json, '$.pendingBackgroundTasks') b WHERE p.provider_thread_id IN (SELECT value FROM json_each(${providerIds}))
        UNION SELECT json_extract(payload_json, '$.childThreadId') FROM orchestration_v2_projection_subagents WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(m.value, '$.threadId') FROM orchestration_v2_projection_context_handoffs h,
          json_each(h.payload_json, '$.history.messages') m WHERE h.thread_id = ${sourceId}
        UNION SELECT json_extract(c.value, '$.threadId') FROM projection_thread_messages m,
          json_each(CASE WHEN json_valid(m.context_json) THEN m.context_json ELSE '{}' END, '$.records') c
          WHERE m.thread_id = ${sourceId} AND json_extract(c.value, '$.kind') = 'thread'
        UNION SELECT json_extract(payload_json, '$.senderThreadId') FROM orchestration_v2_projection_messages WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.notification.source.childThreadId') FROM orchestration_v2_projection_messages WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(c.value, '$.threadId') FROM orchestration_v2_projection_messages m,
          json_each(m.payload_json, '$.context.records') c WHERE m.thread_id = ${sourceId} AND json_extract(c.value, '$.kind') = 'thread'
        UNION SELECT json_extract(payload_json, '$.childThreadId') FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.targetThreadId') FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.senderThreadId') FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.source.threadId') FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.source.childThreadId') FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
      `;
      const ids = refs.flatMap((row) => (row.id === null ? [] : [ThreadId.make(row.id)]));
      yield* authorizeOwnerThreadIds(sql, current.resourceGrant, ids);
    }
  }
  if (anchorThreadId !== undefined && !seen.has(anchorThreadId)) return yield* threadReadDenied();
  return current;
});

/** Navigation metadata does not require a hidden source's transcript. Withhold those refs. */
export const redactReadThreadMetadata = Effect.fnUntraced(function* <
  T extends { readonly thread: OrchestrationV2AppThread },
>(sql: SqlClient.SqlClient, authority: ThreadReadAuthority, projection: T) {
  if (authority === trustedReadAuthority) return projection;
  if (!authority?.current) return yield* threadReadDenied();
  const current = yield* authority.current;
  const grant = current.resourceGrant;
  if (grant?.projects.mode !== "explicit") return projection;
  const thread = projection.thread;
  const permitted = yield* permittedCollectionThreadIds(
    sql,
    [
      thread.lineage.rootThreadId,
      ...(thread.lineage.parentThreadId === null ? [] : [thread.lineage.parentThreadId]),
    ],
    grant,
  );
  const parentVisible =
    thread.lineage.parentThreadId === null || permitted.has(thread.lineage.parentThreadId);
  let forkedFrom = thread.forkedFrom;
  if (forkedFrom !== null && forkedFrom.type !== "run") {
    const visible = yield* (
      forkedFrom.type === "node"
        ? authorizeNodeIds(sql, grant, [forkedFrom.nodeId])
        : authorizeProviderIds(sql, grant, [forkedFrom.providerThreadId])
    ).pipe(
      Effect.as(true),
      Effect.catchTags({ EnvironmentAuthorizationError: () => Effect.succeed(false) }),
    );
    if (!visible) forkedFrom = null;
  }
  const forbiddenPrUrls = new Set(
    [thread.linkedPullRequest, thread.branchPullRequest].flatMap((link) =>
      link && !resourceGrantAllowsProject(grant, link.projectId) ? [link.url] : [],
    ),
  );
  return {
    ...projection,
    thread: {
      ...thread,
      forkedFrom,
      ...(thread.pullRequests === undefined
        ? {}
        : { pullRequests: thread.pullRequests.filter((link) => !forbiddenPrUrls.has(link.url)) }),
      lineage: {
        parentThreadId: parentVisible ? thread.lineage.parentThreadId : null,
        relationshipToParent: parentVisible ? thread.lineage.relationshipToParent : null,
        rootThreadId: permitted.has(thread.lineage.rootThreadId)
          ? thread.lineage.rootThreadId
          : thread.id,
      },
      ...(thread.linkedPullRequest &&
      !resourceGrantAllowsProject(grant, thread.linkedPullRequest.projectId)
        ? { linkedPullRequest: null }
        : {}),
      ...(thread.branchPullRequest &&
      !resourceGrantAllowsProject(grant, thread.branchPullRequest.projectId)
        ? { branchPullRequest: null }
        : {}),
    },
  };
});

function itemReferences(item: OrchestrationV2TurnItem): ReadonlyArray<ThreadId> {
  switch (item.type) {
    case "user_message":
      return [
        ...(item.senderThreadId === undefined ? [] : [item.senderThreadId]),
        ...(item.context?.records ?? []).flatMap((record) =>
          record.kind === "thread" && "threadId" in record ? [record.threadId] : [],
        ),
      ];
    case "subagent":
      return item.childThreadId === null ? [] : [item.childThreadId];
    case "thread_created":
      return [item.targetThreadId];
    case "fork":
      return [item.targetThreadId, ...(item.source.type === "run" ? [item.source.threadId] : [])];
    case "notification":
      return "childThreadId" in item.source && item.source.childThreadId !== undefined
        ? [item.source.childThreadId]
        : [];
    default:
      return [];
  }
}

/** Live frames validate only participating references; no transcript scan on each update. */
export const authorizeThreadReadEvent = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  authority: ThreadReadAuthority,
  event: OrchestrationV2DomainEvent,
) {
  const current = yield* authorizeThreadRead(sql, authority, event.threadId, undefined, false);
  if (current?.resourceGrant?.projects.mode !== "explicit") return;
  let references: ThreadId[] = "threadId" in event.payload ? [event.payload.threadId] : [];
  const payload = event.payload;
  if (
    event.type === "provider-thread.updated" &&
    event.payload.appThreadId === null &&
    event.payload.ownerNodeId === null
  )
    return yield* threadReadDenied();
  const nodeIds = [
    ...("nodeId" in payload && payload.nodeId !== null ? [payload.nodeId] : []),
    ...("rootNodeId" in payload && payload.rootNodeId !== null ? [payload.rootNodeId] : []),
    ...("parentNodeId" in payload && payload.parentNodeId !== null ? [payload.parentNodeId] : []),
    ...("ownerNodeId" in payload && payload.ownerNodeId !== null ? [payload.ownerNodeId] : []),
    ...(event.type === "node.updated" ? [event.payload.id] : []),
    ...(event.type === "turn-item.updated" &&
    event.payload.type === "fork" &&
    event.payload.source.type === "node"
      ? [event.payload.source.nodeId]
      : []),
  ];
  yield* authorizeNodeIds(sql, current.resourceGrant, nodeIds);
  const providerIds = [
    ...("providerThreadId" in payload && payload.providerThreadId != null
      ? [payload.providerThreadId]
      : []),
    ...("activeProviderThreadId" in payload && payload.activeProviderThreadId != null
      ? [payload.activeProviderThreadId]
      : []),
    ...(event.type === "provider-thread.updated"
      ? [
          event.payload.id,
          ...(event.payload.forkedFrom === null ? [] : [event.payload.forkedFrom.providerThreadId]),
        ]
      : []),
    ...(event.type === "context-handoff.updated"
      ? [...event.payload.fromProviderThreadIds, event.payload.toProviderThreadId]
      : []),
    ...(event.type === "turn-item.updated" &&
    event.payload.type === "fork" &&
    event.payload.source.type === "provider_thread"
      ? [event.payload.source.providerThreadId]
      : []),
  ];
  yield* authorizeProviderIds(sql, current.resourceGrant, providerIds);
  yield* authorizeProviderTurnIds(sql, current.resourceGrant, [
    ...("providerTurnId" in payload && payload.providerTurnId != null
      ? [payload.providerTurnId]
      : []),
    ...(event.type === "provider-turn.updated" ? [event.payload.id] : []),
  ]);
  const notification =
    event.type === "message.updated"
      ? event.payload.notification
      : event.type === "turn-item.updated" && event.payload.type === "notification"
        ? event.payload
        : undefined;
  const taskIds = notification?.source.kind === "delegated_task" ? notification.source.taskIds : [];
  for (const taskId of taskIds) {
    const tasks = yield* sql<{ readonly thread_id: string; readonly child: string | null }>`
      SELECT thread_id, json_extract(payload_json, '$.childThreadId') AS child
      FROM orchestration_v2_projection_subagents WHERE subagent_id = ${taskId}
    `;
    if (tasks.length === 0) return yield* threadReadDenied();
    references.push(
      ...tasks.flatMap((row) => [
        ThreadId.make(row.thread_id),
        ...(row.child === null ? [] : [ThreadId.make(row.child)]),
      ]),
    );
  }
  const sourceReferences = references;
  switch (event.type) {
    case "run.created":
    case "run.updated":
      references =
        event.payload.sourcePlanRef === undefined ? [] : [event.payload.sourcePlanRef.threadId];
      break;
    case "turn-item.updated":
      references = [event.payload.threadId, ...itemReferences(event.payload)];
      break;
    case "message.updated":
      references = [
        event.payload.threadId,
        ...(event.payload.senderThreadId === undefined ? [] : [event.payload.senderThreadId]),
        ...(event.payload.context?.records ?? []).flatMap((record) =>
          record.kind === "thread" && "threadId" in record ? [record.threadId] : [],
        ),
        ...(event.payload.notification !== undefined &&
        "childThreadId" in event.payload.notification.source &&
        event.payload.notification.source.childThreadId !== undefined
          ? [event.payload.notification.source.childThreadId]
          : []),
      ];
      break;
    case "subagent.updated":
      references = event.payload.childThreadId === null ? [] : [event.payload.childThreadId];
      break;
    case "context-transfer.created":
    case "context-transfer.updated":
      references = [
        event.payload.sourceThreadId,
        event.payload.targetThreadId,
        event.payload.sourcePoint.threadId,
        ...(event.payload.basePoint === null ? [] : [event.payload.basePoint.threadId]),
      ];
      break;
    case "context-handoff.updated":
      references = (event.payload.history?.messages ?? []).map((message) => message.threadId);
      break;
    case "provider-thread.updated":
      references = [
        ...(event.payload.appThreadId === null ? [] : [event.payload.appThreadId]),
        ...(event.payload.pendingBackgroundTasks ?? []).flatMap((task) =>
          task.kind === "subagent" && task.childThreadId !== undefined ? [task.childThreadId] : [],
        ),
      ];
      break;
    default:
      break;
  }
  yield* authorizeOwnerThreadIds(sql, current.resourceGrant, [...sourceReferences, ...references]);
});
