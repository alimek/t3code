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
    const rows = yield* sql<{ readonly project_id: string }>`
      SELECT t.project_id FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.thread_id = ${id} AND t.deleted_at IS NULL AND p.deleted_at IS NULL
    `;
    if (
      !rows[0] ||
      !resourceGrantAllowsProject(current.resourceGrant, ProjectId.make(rows[0].project_id))
    )
      return yield* threadReadDenied();
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
      const providers = yield* sql<{
        readonly id: string;
        readonly owner: string | null;
        readonly node_owner: string | null;
      }>`
        SELECT p.provider_thread_id AS id, p.thread_id AS owner, n.thread_id AS node_owner
        FROM orchestration_v2_projection_provider_threads p
        LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
        WHERE p.thread_id = ${sourceId} OR n.thread_id = ${sourceId}
          OR p.provider_thread_id IN (
            SELECT provider_thread_id FROM orchestration_v2_projection_subagents WHERE thread_id = ${sourceId}
            UNION SELECT provider_thread_id FROM orchestration_v2_projection_nodes WHERE thread_id = ${sourceId}
            UNION SELECT provider_thread_id FROM orchestration_v2_projection_runs WHERE thread_id = ${sourceId}
            UNION SELECT provider_thread_id FROM orchestration_v2_projection_run_attempts WHERE thread_id = ${sourceId}
            UNION SELECT provider_thread_id FROM orchestration_v2_projection_turn_items WHERE thread_id = ${sourceId}
          )
      `;
      const owners = providers.flatMap((row) =>
        [row.owner, row.node_owner].flatMap((id) => (id === null ? [] : [ThreadId.make(id)])),
      );
      const permittedOwners = yield* permittedCollectionThreadIds(
        sql,
        owners,
        current.resourceGrant,
      );
      if (owners.some((id) => !permittedOwners.has(id))) return yield* threadReadDenied();
      const providerIds = encodeIdList(providers.map((row) => row.id));
      const refs = yield* sql<{ readonly id: string | null }>`
        SELECT source_thread_id AS id FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT target_thread_id FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.sourcePoint.threadId') FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.basePoint.threadId') FROM orchestration_v2_projection_context_transfers
          WHERE source_thread_id = ${sourceId} OR target_thread_id = ${sourceId}
        UNION SELECT COALESCE(p.thread_id, n.thread_id, '') FROM orchestration_v2_projection_provider_threads t
          LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = json_extract(t.payload_json, '$.forkedFrom.providerThreadId')
          LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
          WHERE t.provider_thread_id IN (SELECT value FROM json_each(${providerIds})) AND json_extract(t.payload_json, '$.forkedFrom.providerThreadId') IS NOT NULL
        UNION SELECT COALESCE(p.thread_id, n.thread_id, '') FROM orchestration_v2_projection_context_handoffs h,
          json_each(h.payload_json, '$.fromProviderThreadIds') r
          LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = r.value
          LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
          WHERE h.thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.sourcePlanRef.threadId') FROM orchestration_v2_projection_runs WHERE thread_id = ${sourceId}
        UNION SELECT json_extract(payload_json, '$.appThreadId') FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id IN (SELECT value FROM json_each(${providerIds}))
        UNION SELECT json_extract(b.value, '$.childThreadId') FROM orchestration_v2_projection_provider_threads p,
          json_each(p.payload_json, '$.pendingBackgroundTasks') b WHERE p.provider_thread_id IN (SELECT value FROM json_each(${providerIds}))
        UNION SELECT COALESCE(n.thread_id, '') FROM orchestration_v2_projection_turn_items i
          LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = json_extract(i.payload_json, '$.source.nodeId')
          WHERE i.thread_id = ${sourceId} AND json_extract(i.payload_json, '$.source.type') = 'node'
        UNION SELECT COALESCE(p.thread_id, n.thread_id, '') FROM orchestration_v2_projection_turn_items i
          LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id = json_extract(i.payload_json, '$.source.providerThreadId')
          LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id
          WHERE i.thread_id = ${sourceId} AND json_extract(i.payload_json, '$.source.type') = 'provider_thread'
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
      const permitted = yield* permittedCollectionThreadIds(sql, ids, current.resourceGrant);
      if (ids.some((ref) => !permitted.has(ref))) return yield* threadReadDenied();
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
    const owners =
      forkedFrom.type === "node"
        ? yield* sql<{
            readonly thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_nodes WHERE node_id = ${forkedFrom.nodeId}`
        : yield* sql<{
            readonly thread_id: string;
          }>`SELECT COALESCE(p.thread_id, n.thread_id) AS thread_id FROM orchestration_v2_projection_provider_threads p LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id WHERE p.provider_thread_id = ${forkedFrom.providerThreadId}`;
    const ownerIds = owners.map((owner) => ThreadId.make(owner.thread_id));
    const allowed = yield* permittedCollectionThreadIds(sql, ownerIds, grant);
    if (ownerIds.length === 0 || ownerIds.some((owner) => !allowed.has(owner))) forkedFrom = null;
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
  const providerIds =
    event.type === "provider-thread.updated" && event.payload.forkedFrom !== null
      ? [event.payload.forkedFrom.providerThreadId]
      : event.type === "context-handoff.updated"
        ? event.payload.fromProviderThreadIds
        : [];
  for (const providerId of providerIds) {
    const owners = yield* sql<{
      readonly thread_id: string;
    }>`SELECT COALESCE(p.thread_id, n.thread_id) AS thread_id FROM orchestration_v2_projection_provider_threads p LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id WHERE p.provider_thread_id = ${providerId}`;
    if (owners.length === 0) return yield* threadReadDenied();
    references.push(...owners.map((row) => ThreadId.make(row.thread_id)));
  }
  if (
    event.type === "turn-item.updated" &&
    event.payload.type === "fork" &&
    event.payload.source.type !== "run"
  ) {
    const source = event.payload.source;
    const owners =
      source.type === "node"
        ? yield* sql<{
            readonly thread_id: string;
          }>`SELECT thread_id FROM orchestration_v2_projection_nodes WHERE node_id = ${source.nodeId}`
        : yield* sql<{
            readonly thread_id: string;
          }>`SELECT COALESCE(p.thread_id, n.thread_id) AS thread_id FROM orchestration_v2_projection_provider_threads p LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = p.owner_node_id WHERE p.provider_thread_id = ${source.providerThreadId}`;
    if (owners.length === 0) return yield* threadReadDenied();
    references.push(...owners.map((row) => ThreadId.make(row.thread_id)));
  }
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
  const permitted = yield* permittedCollectionThreadIds(
    sql,
    [...sourceReferences, ...references],
    current.resourceGrant,
  );
  if ([...sourceReferences, ...references].some((id) => !permitted.has(id)))
    return yield* threadReadDenied();
});
