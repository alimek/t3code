import { type AuthResourceGrant, type ProjectId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/sql/SqlClient";

/** Absent/legacy grants retain the existing environment-wide collection. */
export function projectCollectionOptions(
  grant?: AuthResourceGrant,
  requested?: ReadonlyArray<ProjectId>,
) {
  const ids = grant?.projects.mode === "explicit" ? grant.projects.ids : requested;
  if (ids === undefined) return {};
  const selected = new Set(ids);
  return { projectIds: requested === undefined ? ids : requested.filter((id) => selected.has(id)) };
}

/** Includes tombstones so a permitted deletion can still remove a cached row. */
export const permittedCollectionThreadIds = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  threadIds: ReadonlyArray<ThreadId>,
  grant?: AuthResourceGrant,
) {
  const ids = [...new Set(threadIds)];
  if (grant?.projects.mode !== "explicit") return new Set(ids);
  const allowed = new Set<ThreadId>();
  if (grant.projects.ids.length === 0) return allowed;
  // Batch large shell/reference sets; grants use the same selector as shell queries.
  for (let offset = 0; offset < ids.length; offset += 500) {
    const rows = yield* sql<{ readonly thread_id: string }>`
      SELECT thread_id FROM orchestration_v2_projection_threads
      WHERE ${sql.in("thread_id", ids.slice(offset, offset + 500))}
        AND ${sql.in("project_id", grant.projects.ids)}
    `;
    for (const row of rows) allowed.add(ThreadId.make(row.thread_id));
  }
  return allowed;
});
