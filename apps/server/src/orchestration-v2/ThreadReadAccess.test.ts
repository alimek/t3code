import * as SessionStore from "../auth/SessionStore.ts";
import { assert, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSessionId,
  ContextTransferId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EventId,
  MessageId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { Etag, HttpRouter } from "effect/http";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import { subscribeOrchestrationV2Thread } from "../ws.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Legacy from "./legacy/LegacyV1ThreadImporter.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as Http from "./http.ts";
import { encodeThreadHistoryCursor } from "./threadHistoryPaging.ts";
import {
  collectionStoreLayer,
  makeCollectionFixture,
  grantA,
  grantNone,
  activeA,
  activeB,
  projectA,
  projectB,
  now,
} from "./projectCollectionTestFixtures.ts";

const authority: ThreadReadAuthority = {
  current: Effect.succeed({
    scopes: [AuthOrchestrationReadScope],
    resourceGrant: grantA,
    authorityRevision: 1,
  }),
};
const makeFixture = Effect.gen(function* () {
  const fixture = yield* makeCollectionFixture;
  const imported: ThreadId[] = [];
  const p = fixture.projections;
  const reads = Layer.mock(Orchestrator.OrchestratorV2)({
    getThreadProjection: (id) => p.getThreadProjection(id).pipe(Effect.orDie),
    getThreadSnapshot: (id) => p.getThreadSnapshot(id).pipe(Effect.orDie),
    getThreadSnapshotWindow: (id, options) =>
      p.getThreadSnapshotWindow(id, options).pipe(Effect.orDie),
    getThreadHistoryPage: (id, ...args) => p.getThreadHistoryPage(id, ...args).pipe(Effect.orDie),
    getThreadRecords: (id, fields, filter) =>
      p.getThreadRecords(id, fields, filter).pipe(Effect.orDie),
    getTurnItem: (input) => p.getTurnItem(input).pipe(Effect.orDie),
    getCheckpointContext: (id) => p.getCheckpointContext(id).pipe(Effect.orDie),
    searchThread: (input) => p.searchThread(input).pipe(Effect.orDie),
    searchThreadStream: (input) => p.searchThreadStream(input).pipe(Stream.orDie),
    streamStoredEventsFrom: () => Stream.empty,
  });
  const threads = yield* Threads.ThreadManagementService.pipe(
    Effect.provide(
      Threads.layerWithLegacyImporter.pipe(
        Layer.provide(reads),
        Layer.provide(
          Layer.mock(Legacy.LegacyV1ThreadImporter)({
            ensureTranscript: (id) =>
              Effect.sync(() => {
                imported.push(id);
                return { importedThreadCount: 0, importedMessageCount: 0 };
              }),
          }),
        ),
      ),
    ),
  );
  return { ...fixture, threads, imported };
});

it.effect(
  "denies hidden, missing-scope and omitted authority before hydration on every direct service read",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const denied: ReadonlyArray<Effect.Effect<unknown, Orchestrator.OrchestratorV2Error>> = [
        f.threads.getThreadProjection(authority, activeB),
        f.threads.getThreadSnapshot(authority, activeB),
        f.threads.getThreadSnapshotWindow(authority, activeB, { rowLimit: 10 }),
        f.threads.getTurnItem(authority, { threadId: activeB, itemId: TurnItemId.make("guessed") }),
        f.threads.searchThread(authority, { threadId: activeB, query: "secret" }),
        f.threads
          .searchThreadStream(authority, { threadId: activeB, query: "secret" })
          .pipe(Stream.runCollect),
        f.threads.getCheckpointContext(authority, activeB),
        f.threads.getThreadSnapshot(
          { current: Effect.succeed({ scopes: [], resourceGrant: grantA }) },
          activeA,
        ),
        f.threads.getThreadSnapshot(
          {
            current: Effect.succeed({ scopes: [AuthOrchestrationReadScope] }),
          } as unknown as ThreadReadAuthority,
          activeA,
        ),
        // Deliberate malformed internal fixture: runtime JS/older callers cannot omit authority.
        f.threads.getThreadSnapshot(undefined as unknown as ThreadReadAuthority, activeA),
      ];
      for (const read of denied) {
        const error = yield* read.pipe(
          Effect.match({
            onFailure: (error) => error,
            onSuccess: () => assert.fail("read unexpectedly succeeded"),
          }),
        );
        assert.equal(error._tag, "OrchestratorProjectionError");
        assert.equal((error.cause as { _tag: string })._tag, "EnvironmentAuthorizationError");
        assert.notInclude(JSON.stringify(error.cause), "secret");
      }
      assert.deepEqual(f.imported, []);
      assert.equal(
        (yield* f.threads.getThreadSnapshot(authority, activeA)).projection.thread.projectId,
        projectA,
      );
      assert.equal(
        (yield* f.threads.getThreadSnapshot(trustedReadAuthority, activeB)).projection.thread.id,
        activeB,
      );
    }).pipe(Effect.provide(collectionStoreLayer)),
);

it.effect(
  "refuses imported hidden ancestry and forged cursor before target or source hydration",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const sql = yield* SqlClient.SqlClient;
      const forged = encodeThreadHistoryCursor({
        snapshotSequence: 0,
        sourceThreadId: activeB,
        sourceItemId: "guessed",
        position: 0,
      });
      assert.equal(
        (yield* Effect.flip(f.threads.getThreadHistoryPage(authority, activeA, forged)))._tag,
        "OrchestratorProjectionError",
      );
      assert.equal(
        (yield* Effect.flip(
          f.threads.getThreadSnapshotWindow(authority, activeA, {
            rowLimit: 10,
            anchorThreadId: activeB,
            anchorItemId: TurnItemId.make("guessed"),
          }),
        ))._tag,
        "OrchestratorProjectionError",
      );
      yield* f.updateThread("thread.metadata-updated", activeA, {
        forkedFrom: { type: "run", threadId: activeB, runId: RunId.make("hidden-run") },
      });
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'invalid hidden JSON' WHERE thread_id = ${activeB}`;
      const error = yield* Effect.flip(
        f.threads.getThreadSnapshotWindow(authority, activeA, { rowLimit: 10 }),
      );
      assert.equal((error.cause as { _tag: string })._tag, "EnvironmentAuthorizationError");
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(collectionStoreLayer)),
);

it.effect("redacts hidden navigation lineage and observes current grant on the next read", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* f.updateThread("thread.metadata-updated", activeA, {
      lineage: { parentThreadId: activeB, relationshipToParent: "subagent", rootThreadId: activeB },
    });
    let grant = grantA;
    const changing: ThreadReadAuthority = {
      current: Effect.sync(() => ({ scopes: [AuthOrchestrationReadScope], resourceGrant: grant })),
    };
    const visible = yield* f.threads.getThreadSnapshot(changing, activeA);
    assert.deepEqual(visible.projection.thread.lineage, {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: activeA,
    });
    assert.notInclude(JSON.stringify(visible), String(activeB));
    grant = grantNone;
    assert.equal(
      (yield* Effect.flip(f.threads.getThreadSnapshot(changing, activeA)))._tag,
      "OrchestratorProjectionError",
    );
    assert.deepEqual(f.imported, [activeA]);
  }).pipe(Effect.provide(collectionStoreLayer)),
);

it.effect("denies copied cross-project result references before transcript hydration", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const sql = yield* SqlClient.SqlClient;
    // Persisted transfer fixture includes deliberately invalid source material; guard reads IDs only.
    yield* sql`INSERT INTO orchestration_v2_projection_context_transfers
      (context_transfer_id, source_thread_id, target_thread_id, type, status, updated_at, payload_json)
      VALUES ('cross-transfer', ${activeB}, ${activeA}, 'merge_back', 'consumed', '2026-10-11', '{}')`;
    assert.equal(
      (yield* Effect.flip(f.threads.getThreadProjection(authority, activeA)))._tag,
      "OrchestratorProjectionError",
    );
    assert.deepEqual(f.imported, []);
  }).pipe(Effect.provide(collectionStoreLayer)),
);

it.effect(
  "WS guessed ID fails before import; restricted resume replaces broader stored events",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const sql = yield* SqlClient.SqlClient;
      const services = Layer.mergeAll(
        f.services(),
        Layer.succeed(Threads.ThreadManagementService, f.threads),
        Layer.succeed(SqlClient.SqlClient, sql),
        Layer.succeed(ProjectStore.ProjectStoreV2, f.projects),
      );
      const denied = yield* subscribeOrchestrationV2Thread(authority, {
        threadId: activeB,
        afterSequence: 0,
      }).pipe(Effect.flip, Effect.provide(services));
      assert.equal(denied._tag, "OrchestrationV2GetThreadProjectionError");
      assert.deepEqual(f.imported, []);
      const frames = yield* subscribeOrchestrationV2Thread(authority, {
        threadId: activeA,
        afterSequence: 0,
        requestCompletionMarker: true,
      }).pipe(
        Effect.flatMap((stream) => stream.pipe(Stream.runCollect)),
        Effect.provide(services),
      );
      assert.equal(frames[0]?.kind, "snapshot");
      assert.equal(frames[1]?.kind, "synchronized");
      assert.notInclude(JSON.stringify(frames), String(activeB));
    }).pipe(Effect.scoped, Effect.provide(collectionStoreLayer)),
);

it.effect("HTTP full/bounded/history deny hidden IDs and preserve allowed snapshots", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    class Api extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.orchestration) {}
    const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
      effect.pipe(
        Effect.provideService(EnvironmentAuthenticatedPrincipal, {
          sessionId: AuthSessionId.make("internal-http-fixture"),
          subject: "fixture",
          method: "browser-session-cookie",
          scopes: new Set([AuthOrchestrationReadScope]),
          resourceGrant: grantA,
        }),
      ),
    );
    const sql = yield* SqlClient.SqlClient;
    const app = HttpApiBuilder.layer(Api).pipe(
      Layer.provide(Http.layer),
      Layer.provide(auth),
      Layer.provide(
        Layer.mock(SessionStore.SessionStore)({
          cookieName: "fixture",
          legacyCookieName: undefined,
          getAuthority: () =>
            Effect.succeed({
              deviceId: AuthSessionId.make("internal-http-fixture"),
              scopes: [AuthOrchestrationReadScope],
              resourceGrant: grantA,
              authorityRevision: 1,
              expiresAt: now,
            }),
        }),
      ),
      Layer.provide(
        Layer.mergeAll(
          f.services(),
          Layer.succeed(Threads.ThreadManagementService, f.threads),
          Layer.succeed(SqlClient.SqlClient, sql),
          Layer.succeed(ProjectStore.ProjectStoreV2, f.projects),
        ),
      ),
      Layer.provide(NodeHttpPlatform.layer),
      Layer.provide(Etag.layerWeak),
      Layer.provide(NodeServices.layer),
    );
    const handler = HttpRouter.toWebHandler(app, { disableLogger: true });
    const web = yield* Effect.acquireRelease(Effect.succeed(handler), (h) =>
      Effect.promise(() => h.dispose()),
    );
    for (const suffix of [
      "",
      "/bounded",
      "/history?cursor=" +
        encodeURIComponent(
          encodeThreadHistoryCursor({
            snapshotSequence: 0,
            sourceThreadId: activeB,
            sourceItemId: "guessed",
            position: 0,
          }),
        ),
    ]) {
      const response = yield* Effect.promise(() =>
        web.handler(
          new Request(`http://localhost/api/orchestration/threads/${activeB}${suffix}`, {
            headers: { [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION) },
          }),
        ),
      );
      assert.equal(response.status, 404);
      const body = yield* Effect.promise(() => response.text());
      assert.notInclude(body, String(activeB));
    }
    assert.deepEqual(f.imported, []);
    const response = yield* Effect.promise(() =>
      web.handler(
        new Request(`http://localhost/api/orchestration/threads/${activeA}/bounded`, {
          headers: { [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION) },
        }),
      ),
    );
    assert.equal(response.status, 200);
  }).pipe(Effect.scoped, Effect.provide(collectionStoreLayer)),
);

it.effect("allowed turn detail, history and search never resolve an unrelated hidden item", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    for (const id of [activeA, activeB]) {
      yield* f.appendThread({
        id: EventId.make(`item:${id}`),
        type: "turn-item.updated",
        threadId: id,
        occurredAt: now,
        payload: {
          type: "assistant_message",
          id: TurnItemId.make(`tool:${id}`),
          threadId: id,
          runId: null,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 1,
          status: "completed",
          title: null,
          startedAt: now,
          completedAt: now,
          updatedAt: now,
          messageId: MessageId.make(`message:${id}`),
          text: id === activeB ? "searchable hidden secret output" : "searchable allowed output",
          streaming: false,
        },
      });
    }
    const own = yield* f.threads.getTurnItem(authority, {
      threadId: activeA,
      itemId: TurnItemId.make(`tool:${activeA}`),
    });
    assert.include(JSON.stringify(own), "allowed output");
    const guessed = yield* f.threads.getTurnItem(authority, {
      threadId: activeA,
      itemId: TurnItemId.make(`tool:${activeB}`),
    });
    assert.equal(guessed.item, null);
    const found = yield* f.threads.searchThread(authority, {
      threadId: activeA,
      query: "searchable",
    });
    assert.isAbove(found.totalMatches, 0);
    const stream = yield* f.threads
      .searchThreadStream(authority, { threadId: activeA, query: "searchable" })
      .pipe(Stream.runCollect);
    assert.isAbove(stream.length, 0);
    const cursor = encodeThreadHistoryCursor({
      snapshotSequence: 0,
      sourceThreadId: activeA,
      sourceItemId: `tool:${activeA}`,
      position: 1,
    });
    const history = yield* f.threads.getThreadHistoryPage(authority, activeA, cursor);
    assert.notInclude(JSON.stringify([own, guessed, found, stream, history]), "hidden secret");
  }).pipe(Effect.provide(collectionStoreLayer)),
);

it.effect("live WS frames recheck current grants and source participants before emission", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const sql = yield* SqlClient.SqlClient;
    let grant = grantA;
    const changing: ThreadReadAuthority = {
      current: Effect.sync(() => ({ scopes: [AuthOrchestrationReadScope], resourceGrant: grant })),
    };
    const snapshot = yield* f.threads.getThreadProjection(authority, activeA);
    const event = {
      id: EventId.make("live-thread"),
      type: "thread.metadata-updated" as const,
      threadId: activeA,
      occurredAt: now,
      payload: {
        ...snapshot.thread,
        linkedPullRequest: {
          projectId: projectB,
          repository: "hidden/repository",
          number: 1,
          url: "https://github.com/hidden/repository/pull/1",
        },
        lineage: {
          parentThreadId: activeB,
          relationshipToParent: "subagent" as const,
          rootThreadId: activeB,
        },
      },
    };
    const services = (
      stream: Stream.Stream<{
        readonly sequence: number;
        readonly commandId: null;
        readonly event: OrchestrationV2DomainEvent;
      }>,
    ) =>
      Layer.mergeAll(
        f.services(),
        Layer.succeed(Threads.ThreadManagementService, {
          ...f.threads,
          streamStoredEventsFrom: () => stream,
        }),
        Layer.succeed(SqlClient.SqlClient, sql),
        Layer.succeed(ProjectStore.ProjectStoreV2, f.projects),
      );
    const visible = yield* subscribeOrchestrationV2Thread(changing, { threadId: activeA }).pipe(
      Effect.flatMap((stream) => Stream.runCollect(stream)),
      Effect.provide(services(Stream.make({ sequence: 100, commandId: null, event }))),
    );
    assert.equal(visible[1]?.kind, "event");
    assert.notInclude(JSON.stringify(visible), String(activeB));
    assert.notInclude(JSON.stringify(visible), "hidden/repository");
    const revoked = yield* subscribeOrchestrationV2Thread(changing, { threadId: activeA }).pipe(
      Effect.flatMap((stream) => Stream.runCollect(stream)),
      Effect.flip,
      Effect.provide(
        services(
          Stream.fromEffect(
            Effect.sync(() => {
              grant = grantNone;
              return { sequence: 101, commandId: null, event };
            }),
          ),
        ),
      ),
    );
    assert.equal(revoked._tag, "OrchestrationV2GetThreadProjectionError");
    grant = grantA;
    const copied: OrchestrationV2DomainEvent = {
      id: EventId.make("live-hidden-sender"),
      type: "turn-item.updated",
      threadId: activeA,
      occurredAt: now,
      payload: {
        type: "user_message",
        id: TurnItemId.make("hidden-result"),
        threadId: activeA,
        runId: null,
        nodeId: null,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: null,
        parentItemId: null,
        ordinal: 2,
        status: "completed",
        title: null,
        startedAt: now,
        completedAt: now,
        updatedAt: now,
        messageId: MessageId.make("hidden-result-message"),
        text: "hidden source text",
        senderThreadId: activeB,
        inputIntent: "turn_start",
        attachments: [],
        createdBy: "agent",
        creationSource: "mcp",
      },
    };
    const forbidden = yield* subscribeOrchestrationV2Thread(changing, { threadId: activeA }).pipe(
      Effect.flatMap((stream) => Stream.runCollect(stream)),
      Effect.flip,
      Effect.provide(services(Stream.make({ sequence: 102, commandId: null, event: copied }))),
    );
    assert.equal(forbidden._tag, "OrchestrationV2GetThreadProjectionError");
    for (const type of ["context-transfer.created", "context-transfer.updated"] as const) {
      const transfer: OrchestrationV2DomainEvent = {
        id: EventId.make(type),
        type,
        threadId: activeA,
        occurredAt: now,
        payload: {
          id: ContextTransferId.make("hidden-live-transfer"),
          type: "fork",
          sourceThreadId: activeB,
          targetThreadId: activeA,
          sourcePoint: { threadId: activeB },
          basePoint: null,
          sourceProviderInstanceId: null,
          targetProviderInstanceId: null,
          targetRunId: null,
          error: null,
          consumedAt: null,
          status: "pending",
          resolution: null,
          createdBy: "user",
          createdAt: now,
          updatedAt: now,
        },
      };
      const denied = yield* subscribeOrchestrationV2Thread(changing, { threadId: activeA }).pipe(
        Effect.flatMap((stream) => Stream.runCollect(stream)),
        Effect.flip,
        Effect.provide(services(Stream.make({ sequence: 103, commandId: null, event: transfer }))),
      );
      assert.equal(denied._tag, "OrchestrationV2GetThreadProjectionError");
    }
  }).pipe(Effect.scoped, Effect.provide(collectionStoreLayer)),
);

it.effect("legacy context and provider forks cannot import hidden-source content", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO projection_thread_messages
    (message_id, thread_id, role, text, is_streaming, created_at, updated_at, context_json)
    VALUES ('legacy-hidden-context', ${activeA}, 'user', 'copied hidden text', 0, '2026-10-11', '2026-10-11',
      ${JSON.stringify({ records: [{ kind: "thread", threadId: activeB }] })})`;
    assert.equal(
      (yield* Effect.flip(f.threads.getThreadSnapshot(authority, activeA)))._tag,
      "OrchestratorProjectionError",
    );
    assert.deepEqual(f.imported, []);
    yield* sql`DELETE FROM projection_thread_messages WHERE message_id = 'legacy-hidden-context'`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads
    (provider_thread_id, thread_id, provider, status, updated_at, payload_json)
    VALUES ('provider-hidden', ${activeB}, 'codex', 'idle', '2026-10-11', 'invalid hidden JSON'),
      ('provider-fork', ${activeA}, 'codex', 'idle', '2026-10-11', '{"forkedFrom":{"providerThreadId":"provider-hidden"}}')`;
    const error = yield* Effect.flip(f.threads.getThreadSnapshot(authority, activeA));
    assert.equal((error.cause as { _tag: string })._tag, "EnvironmentAuthorizationError");
    assert.deepEqual(f.imported, []);
    yield* sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = 'provider-fork'`;
    yield* sql`INSERT INTO orchestration_v2_projection_subagents
      (subagent_id, thread_id, parent_node_id, provider, driver, provider_instance_id, provider_thread_id, origin, status, updated_at, payload_json)
      VALUES ('linked-provider', ${activeA}, 'own-parent', 'codex', 'codex', 'codex', 'provider-hidden', 'provider_native', 'completed', '2026-10-11', '{}')`;
    const linked = yield* Effect.flip(f.threads.getThreadSnapshot(authority, activeA));
    assert.equal((linked.cause as { _tag: string })._tag, "EnvironmentAuthorizationError");
    assert.deepEqual(f.imported, []);
  }).pipe(Effect.provide(collectionStoreLayer)),
);
