import {
  legacyThreadPullRequestKey,
  normalizeThreadPullRequestKey,
  threadPullRequestKeysEqual,
} from "@t3tools/shared/threadPullRequests";
import {
  AuthOrchestrationReadScope,
  EnvironmentAuthorizationError,
  ProjectId,
  resourceGrantAllowsProject,
  PullRequestLinkedThreadsResult,
  PullRequestOperationError,
  type ThreadPullRequestKey,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";

const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
const decodeLinkedThreads = Schema.decodeUnknownEffect(PullRequestLinkedThreadsResult);

export const listLinkedPullRequestThreads = Effect.fn("listLinkedPullRequestThreads")(
  function* (authority: ThreadReadAuthority, input: ThreadPullRequestKey) {
    const denied = () =>
      new EnvironmentAuthorizationError({
        requiredScope: AuthOrchestrationReadScope,
        message: "This connection cannot read linked pull request threads.",
      });
    const current = Effect.gen(function* () {
      if (authority === trustedReadAuthority) return undefined;
      if (!authority?.current) return yield* denied();
      const value = yield* authority.current;
      if (!value.resourceGrant || !value.scopes.includes(AuthOrchestrationReadScope))
        return yield* denied();
      return value.resourceGrant;
    });
    const grant = yield* current;
    const key = normalizeThreadPullRequestKey(input);
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      id: string;
      projectId: string;
      title: string;
      archivedAt: string | null;
      host: string | null;
      repository: string;
      number: number;
      url: string;
    }>`
      WITH visible_threads AS MATERIALIZED (
        SELECT t.* FROM orchestration_v2_projection_threads t
        JOIN projection_projects p ON p.project_id = t.project_id
        WHERE t.deleted_at IS NULL AND p.deleted_at IS NULL
          AND ${grant?.projects.mode !== "explicit" ? sql`1 = 1` : sql.in("t.project_id", grant.projects.ids)}
      )
      SELECT t.thread_id AS id, t.project_id AS "projectId", t.title,
        t.archived_at AS "archivedAt", json_extract(link.value, '$.host') AS host,
        json_extract(link.value, '$.repository') AS repository,
        json_extract(link.value, '$.number') AS number,
        json_extract(link.value, '$.url') AS url,
        t.updated_at AS "updatedAt"
      FROM visible_threads AS t
      JOIN json_each(t.payload_json, '$.pullRequests') AS link
      WHERE json_extract(link.value, '$.number') = ${key.number}
        AND json_extract(link.value, '$.source') != 'stack-dismissed'
        AND t.deleted_at IS NULL
      UNION ALL
      SELECT t.thread_id AS id, t.project_id AS "projectId", t.title,
        t.archived_at AS "archivedAt", NULL AS host,
        json_extract(t.payload_json, '$.linkedPullRequest.repository') AS repository,
        json_extract(t.payload_json, '$.linkedPullRequest.number') AS number,
        json_extract(t.payload_json, '$.linkedPullRequest.url') AS url,
        t.updated_at AS "updatedAt"
      FROM visible_threads AS t
      WHERE json_type(t.payload_json, '$.pullRequests') IS NULL
        AND json_type(t.payload_json, '$.linkedPullRequest') = 'object'
        AND json_extract(t.payload_json, '$.linkedPullRequest.number') = ${key.number}
        AND t.deleted_at IS NULL
      ORDER BY "updatedAt" DESC, id ASC
    `;
    const latest = yield* current;
    const actual =
      rows.length === 0
        ? []
        : yield* sql<{ id: string; project: string }>`
      SELECT t.thread_id AS id, t.project_id AS project
      FROM orchestration_v2_projection_threads t JOIN projection_projects p ON p.project_id = t.project_id
      WHERE ${sql.in(
        "t.thread_id",
        rows.map((row) => row.id),
      )} AND t.deleted_at IS NULL AND p.deleted_at IS NULL
    `;
    const live = new Map(actual.map((row) => [row.id, row.project]));
    const finalGrant = yield* current;
    const threads = rows
      .filter(
        (row) =>
          live.get(row.id) === row.projectId &&
          resourceGrantAllowsProject(latest, ProjectId.make(row.projectId)) &&
          resourceGrantAllowsProject(finalGrant, ProjectId.make(row.projectId)),
      )
      .filter((row) =>
        threadPullRequestKeysEqual(
          row.host === null ? legacyThreadPullRequestKey(row) : { ...row, host: row.host },
          key,
        ),
      )
      .map(({ id, projectId, title, archivedAt }) => ({ id, projectId, title, archivedAt }));
    return yield* decodeLinkedThreads({ threads });
  },
  Effect.mapError((cause) =>
    isAuthorizationError(cause)
      ? cause
      : new PullRequestOperationError({
          operation: "linkedThreads",
          detail: "Could not load linked threads.",
          cause,
        }),
  ),
);
