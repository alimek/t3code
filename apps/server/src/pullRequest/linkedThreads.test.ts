import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import { AuthOrchestrationReadScope } from "@t3tools/contracts";
import { assert, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { listLinkedPullRequestThreads } from "./linkedThreads.ts";
import {
  collectionStoreLayer,
  makeCollectionFixture,
  grantA,
  grantNone,
  projectA,
  projectB,
  activeA,
  activeB,
  archiveA,
  archiveB,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";

it.effect("filters active, archived and legacy linked thread references in SQL", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    for (const [id, projectId] of [
      [activeA, projectA],
      [activeB, projectB],
      [archiveA, projectA],
      [archiveB, projectB],
    ] as const) {
      yield* fixture.updateThread("thread.metadata-updated", id, {
        linkedPullRequest: {
          projectId,
          repository: "acme/shared",
          number: 7,
          url: "https://github.com/acme/shared/pull/7",
        },
      });
    }
    const sql = yield* SqlClient.SqlClient;
    // Exercise the modern array and the legacy single-link branch together.
    yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.pullRequests', json('[{"host":"github.com","repository":"acme/shared","number":7,"url":"https://github.com/acme/shared/pull/7","source":"manual"}]')) WHERE thread_id IN (${activeA}, ${activeB})`;
    const key = { host: "github.com", repository: "acme/shared", number: 7 };
    const restricted = yield* listLinkedPullRequestThreads(
      { current: Effect.succeed({ scopes: [AuthOrchestrationReadScope], resourceGrant: grantA }) },
      key,
    );
    assert.deepEqual(
      restricted.threads.map((thread) => thread.id).toSorted(),
      [activeA, archiveA].toSorted(),
    );
    assert.notInclude(JSON.stringify(restricted), "b-secret");
    assert.deepEqual(
      (yield* listLinkedPullRequestThreads(
        {
          current: Effect.succeed({
            scopes: [AuthOrchestrationReadScope],
            resourceGrant: grantNone,
          }),
        },
        key,
      )).threads,
      [],
    );
    assert.equal(
      (yield* listLinkedPullRequestThreads(trustedReadAuthority, key)).threads.length,
      4,
    );
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect(
  "finds active and archived threads for exactly one pull request, excluding deleted and dismissed links",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const createdAt = "2026-09-01T00:00:00.000Z";
      const archivedAt = "2026-09-03T00:00:00.000Z";
      const fixtures = [
        {
          id: "forgejo-old",
          host: "forge.example",
          repository: "acme/web",
          number: 7,
          source: "manual",
          url: "http://forge.example:3000/acme/web/pulls/7",
        },
        {
          id: "forgejo-other-port",
          host: "forge.example:4000",
          repository: "acme/web",
          number: 7,
          source: "manual",
          url: "http://forge.example:4000/acme/web/pulls/7",
        },
        {
          id: "azure",
          host: "dev.azure.com",
          repository: "org/project/_git/web",
          number: 7,
          source: "manual",
        },
        {
          id: "other-org",
          host: "dev.azure.com",
          repository: "other/project/_git/web",
          number: 7,
          source: "manual",
        },
        { id: "active", host: "github.com", repository: "acme/web", number: 7, source: "manual" },
        {
          id: "archived",
          host: "github.com",
          repository: "acme/web",
          number: 7,
          source: "created",
        },
        { id: "deleted", host: "github.com", repository: "acme/web", number: 7, source: "manual" },
        {
          id: "dismissed",
          host: "github.com",
          repository: "acme/web",
          number: 7,
          source: "stack-dismissed",
        },
        {
          id: "other-host",
          host: "github.example.com",
          repository: "acme/web",
          number: 7,
          source: "manual",
        },
        {
          id: "other-repository",
          host: "github.com",
          repository: "acme/api",
          number: 7,
          source: "manual",
        },
        {
          id: "other-number",
          host: "github.com",
          repository: "acme/web",
          number: 8,
          source: "manual",
        },
      ];
      for (const fixture of fixtures) {
        const payload = yield* encodePayload({
          linkedPullRequest: null,
          pullRequests: [{ ...fixture, linkedAt: createdAt, snapshot: null, stack: null }],
        });
        yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, archived_at, deleted_at, payload_json
        ) VALUES (
          ${fixture.id}, 'project-1', ${fixture.id}, 'codex', 'full-access', 'default',
          ${createdAt}, ${fixture.id === "archived" ? archivedAt : createdAt},
          ${fixture.id === "archived" ? archivedAt : null},
          ${fixture.id === "deleted" ? archivedAt : null}, ${payload}
        )
      `;
      }
      yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, payload_json
        ) VALUES (
          'legacy-single', 'project-1', 'legacy-single', 'codex', 'full-access', 'default',
          ${createdAt}, ${createdAt},
          '{"linkedPullRequest":{"projectId":"project-1","repository":"acme/web","number":7,"url":"https://github.com/acme/web/pull/7"}}'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, payload_json
        ) VALUES (
          'explicitly-unlinked', 'project-1', 'explicitly-unlinked', 'codex', 'full-access', 'default',
          ${createdAt}, ${createdAt},
          '{"linkedPullRequest":{"projectId":"project-1","repository":"acme/web","number":7,"url":"https://github.com/acme/web/pull/7"},"pullRequests":[]}'
        )
      `;
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, scripts_json, created_at, updated_at
        ) VALUES ('project-1', 'Project', '/tmp/project', '[]', ${createdAt}, ${createdAt})
      `;
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, created_at, updated_at
        ) VALUES ('v1-only', 'project-1', 'v1-only',
          '{"instanceId":"codex","model":"gpt-5.4"}', ${createdAt}, ${createdAt})
      `;
      yield* sql`
        INSERT INTO projection_thread_pull_requests (
          thread_id, host, repository, number, url, source, linked_at
        ) VALUES ('v1-only', 'github.com', 'acme/web', 7,
          'https://github.com/acme/web/pull/7', 'manual', ${createdAt})
      `;

      expect(
        (yield* listLinkedPullRequestThreads(trustedReadAuthority, {
          host: "forge.example:3000",
          repository: "acme/web",
          number: 7,
        })).threads.map((thread) => thread.id),
      ).toEqual(["forgejo-old"]);
      expect(
        (yield* listLinkedPullRequestThreads(trustedReadAuthority, {
          host: "forge.example:4000",
          repository: "acme/web",
          number: 7,
        })).threads.map((thread) => thread.id),
      ).toEqual(["forgejo-other-port"]);
      expect(
        (yield* listLinkedPullRequestThreads(trustedReadAuthority, {
          host: "org.visualstudio.com",
          repository: "project/_git/web",
          number: 7,
        })).threads.map((thread) => thread.id),
      ).toEqual(["azure"]);
      const result = yield* listLinkedPullRequestThreads(trustedReadAuthority, {
        host: "GitHub.Com",
        repository: "ACME/WEB",
        number: 7,
      });
      expect(result).toEqual({
        threads: [
          { id: "archived", projectId: "project-1", title: "archived", archivedAt },
          { id: "active", projectId: "project-1", title: "active", archivedAt: null },
          { id: "legacy-single", projectId: "project-1", title: "legacy-single", archivedAt: null },
        ],
      });
      assert.deepStrictEqual(
        yield* listLinkedPullRequestThreads(trustedReadAuthority, {
          host: "github.com",
          repository: "acme/web",
          number: 99,
        }),
        { threads: [] },
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect(
  "excludes malformed hidden metadata and cuts actual thread owners after a suspended current check",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCollectionFixture;
      const sql = yield* SqlClient.SqlClient;
      const key = { host: "github.com", repository: "acme/shared", number: 7 };
      yield* fixture.updateThread("thread.metadata-updated", activeA, {
        linkedPullRequest: {
          projectId: projectA,
          ...key,
          url: "https://github.com/acme/shared/pull/7",
        },
      });
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'malformed hidden JSON' WHERE project_id = ${projectB}`;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let checks = 0;
      const authority: ThreadReadAuthority = {
        current: Effect.gen(function* () {
          if (++checks === 2) {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          }
          return { scopes: [AuthOrchestrationReadScope], resourceGrant: grantA };
        }),
      };
      const fiber = yield* Effect.forkChild(listLinkedPullRequestThreads(authority, key));
      yield* Deferred.await(started);
      yield* sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      yield* Deferred.succeed(release, undefined);
      assert.deepEqual((yield* Fiber.join(fiber)).threads, []);
    }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);
