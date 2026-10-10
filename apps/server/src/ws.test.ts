import { assert, it } from "@effect/vitest";
import {
  ORCHESTRATION_PROTOCOL_VERSION,
  AuthAllResourceGrant,
  EnvironmentId,
  EventId,
  ProjectId,
  ThreadId,
  type ServerConfig,
  type ServerConfigStreamEvent,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as PubSub from "effect/PubSub";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ServerConfigService from "./config.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as SessionStore from "./auth/SessionStore.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import { ChildProcessSpawner } from "effect/process";

import * as ExternalLauncher from "./process/externalLauncher.ts";
import {
  hasCompatibleOrchestrationProtocol,
  resolveAvailableEditorsForConfig,
  shouldUseBoundedThreadSnapshot,
  withLateEditorConfig,
  subscribeOrchestrationV2Shell,
  getOrchestrationV2ArchivedShellSnapshot,
  subscribeOrchestrationV2ArchivedShell,
} from "./ws.ts";
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
  now,
  threadCreated,
} from "./orchestration-v2/projectCollectionTestFixtures.ts";

it.effect(
  "uses current fixture device grant and revision while production verification remains closed",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCollectionFixture;
      const sessionLayer = SessionStore.layer.pipe(
        Layer.provide(ServerSecretStore.layer),
        Layer.provide(
          Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
            getEnvironmentId: Effect.succeed(EnvironmentId.make("collection-authority-fixture")),
          }),
        ),
        Layer.provide(
          ServerConfigService.layerTest(process.cwd(), { prefix: "collection-authority-" }),
        ),
        Layer.provide(NodeServices.layer),
      );
      yield* Effect.gen(function* () {
        const sessions = yield* SessionStore.SessionStore;
        const sql = yield* SqlClient.SqlClient;
        const root = yield* sessions.issue();
        // Internal persistence fixture only. Guarded issuance/edit/verification stay closed.
        yield* sql`UPDATE auth_sessions SET resource_grant = ${JSON.stringify(grantA)}, authority_revision = 1 WHERE session_id = ${root.sessionId}`;
        const authority = yield* sessions.getAuthority(root.sessionId);
        assert.equal(
          (yield* sessions.verify(root.token).pipe(Effect.flip))._tag,
          "ResourceGrantsNotActivatedError",
        );
        const started = yield* Deferred.make<void>();
        const cleaned = yield* Deferred.make<void>();
        const connection = yield* sessions
          .withConnection(
            { ...root, ...authority },
            subscribeOrchestrationV2Shell(
              { afterSequence: 0, requestCompletionMarker: true },
              authority.resourceGrant,
              authority.authorityRevision,
            ).pipe(
              Effect.flatMap((stream) =>
                stream.pipe(
                  Stream.runForEach((item) => {
                    assert.notInclude(JSON.stringify(item), "b-secret");
                    return item.kind === "synchronized"
                      ? Deferred.succeed(started, undefined).pipe(Effect.asVoid)
                      : Effect.void;
                  }),
                ),
              ),
              Effect.ensuring(Deferred.succeed(cleaned, undefined)),
              Effect.provide(fixture.services()),
            ),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* sessions.revoke(root.sessionId);
        yield* Deferred.await(cleaned);
        yield* Fiber.await(connection);
        const fresh = yield* sessions.issue();
        yield* sql`UPDATE auth_sessions SET resource_grant = ${JSON.stringify(grantNone)}, authority_revision = 2 WHERE session_id = ${fresh.sessionId}`;
        const current = yield* sessions.getAuthority(fresh.sessionId);
        const resumed = yield* subscribeOrchestrationV2Shell(
          {
            afterSequence: yield* fixture.events.latestApplicationSequence,
            requestCompletionMarker: true,
          },
          current.resourceGrant,
          current.authorityRevision,
        ).pipe(
          Effect.flatMap((stream) => stream.pipe(Stream.take(2), Stream.runCollect)),
          Effect.provide(fixture.services()),
        );
        assert.equal(resumed[0]?.kind, "snapshot");
        if (resumed[0]?.kind === "snapshot") {
          assert.deepEqual(resumed[0].snapshot.projects, []);
          assert.deepEqual(resumed[0].snapshot.threads, []);
        }
        assert.equal(resumed[1]?.kind, "synchronized");
      }).pipe(Effect.provide(sessionLayer));
    }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect(
  "filters initial and resumed shells and archives before enrichment, keeping the global head",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCollectionFixture;
      const head = yield* fixture.events.latestApplicationSequence;
      for (const grant of [grantA, grantNone, AuthAllResourceGrant, undefined]) {
        for (const afterSequence of [undefined, 0, head]) {
          fixture.enrichedRoots.length = 0;
          const items = yield* subscribeOrchestrationV2Shell(
            {
              ...(afterSequence === undefined ? {} : { afterSequence }),
              requestCompletionMarker: true,
            },
            grant,
            1,
          ).pipe(
            Effect.flatMap((stream) =>
              stream.pipe(Stream.take(grant === grantNone ? 2 : 3), Stream.runCollect),
            ),
            Effect.provide(fixture.services()),
          );
          const first = items[0];
          assert.equal(first?.kind, "snapshot");
          if (first?.kind !== "snapshot") return yield* Effect.die("Missing snapshot");
          const expectedProjects =
            grant === grantNone ? [] : grant === grantA ? [projectA] : [projectA, projectB];
          assert.deepEqual(
            first.snapshot.projects.map((project) => project.id),
            expectedProjects,
          );
          assert.deepEqual(
            first.snapshot.threads.map((thread) => thread.id),
            grant === grantNone ? [] : grant === grantA ? [activeA] : [activeA, activeB],
          );
          assert.deepEqual(first.snapshot.archivedThreads, []);
          assert.equal(first.snapshot.snapshotSequence, head);
          assert.equal(items.at(-1)?.kind, "synchronized");
          assert.deepEqual(
            fixture.enrichedRoots,
            expectedProjects.map((id) => `/work/${id}`),
          );
          if (grant === grantA || grant === grantNone)
            assert.notInclude(JSON.stringify(items), "b-secret");
        }
        fixture.enrichedRoots.length = 0;
        const archive = yield* getOrchestrationV2ArchivedShellSnapshot(grant).pipe(
          Effect.provide(fixture.services()),
        );
        assert.deepEqual(
          archive.threads.map((thread) => thread.id),
          grant === grantNone ? [] : grant === grantA ? [archiveA] : [archiveA, archiveB],
        );
        assert.equal(archive.snapshotSequence, head);
        if (grant === grantA || grant === grantNone)
          assert.notInclude(JSON.stringify(archive), "b-secret");
      }
    }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("keeps revision-zero owner replay metadata-only and resets edited authority", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const head = yield* fixture.events.latestApplicationSequence;
    const replay = yield* subscribeOrchestrationV2Shell({
      afterSequence: 0,
      requestCompletionMarker: true,
    }).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(8), Stream.runCollect)),
      Effect.provide(fixture.services()),
    );
    assert.equal(replay[0]?.kind, "snapshot");
    if (replay[0]?.kind === "snapshot") {
      assert.deepEqual(replay[0].snapshot.threads, []);
      assert.isDefined(replay[0].resolvedRepositoryIdentityRoots);
    }
    assert.equal(replay.at(-1)?.kind, "synchronized");
    const reset = yield* subscribeOrchestrationV2Shell(
      { afterSequence: head, requestCompletionMarker: true },
      AuthAllResourceGrant,
      2,
    ).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)),
      Effect.provide(fixture.services()),
    );
    assert.equal(reset[0]?.kind, "snapshot");
    if (reset[0]?.kind === "snapshot") {
      assert.isUndefined(reset[0].resolvedRepositoryIdentityRoots);
      assert.equal(reset[0].snapshot.threads.length, 2);
    }
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("filters live batches before shell reads and permits future all-project additions", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const future = ProjectId.make("project:collection-future");
    const futureThread = ThreadId.make("thread:collection-future");
    const live = Stream.unwrap(
      Effect.gen(function* () {
        const created = yield* fixture.appendProject(future);
        const createdThread = yield* fixture.appendThread(threadCreated(futureThread, future));
        const allowed = yield* fixture.updateThread("thread.metadata-updated", activeA, {
          title: "allowed live",
        });
        const hidden = yield* fixture.updateThread("thread.metadata-updated", activeB, {
          title: "b-secret live",
        });
        return Stream.fromIterable([created, createdThread, allowed, hidden]);
      }),
    );
    const items = yield* subscribeOrchestrationV2Shell(
      { requestCompletionMarker: true },
      grantA,
    ).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(4), Stream.runCollect)),
      Effect.provide(fixture.services(live)),
    );
    const delta = items.at(-1);
    assert.equal(delta?.kind, "thread.updated");
    if (delta?.kind === "thread.updated") {
      assert.equal(delta.thread.id, activeA);
      assert.equal(delta.thread.title, "allowed live");
      assert.equal(delta.sequence, (yield* fixture.events.latestApplicationSequence) - 1);
    }
    assert.deepEqual(fixture.shellReads, [activeA]);
    assert.notInclude(JSON.stringify(items), "b-secret");
    assert.notInclude(JSON.stringify(items), "collection-future");
    assert.notInclude(fixture.enrichedRoots, `/work/${projectB}`);
    const owner = yield* subscribeOrchestrationV2Shell(
      { requestCompletionMarker: true },
      AuthAllResourceGrant,
    ).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(1), Stream.runCollect)),
      Effect.provide(fixture.services()),
    );
    if (owner[0]?.kind === "snapshot")
      assert.include(
        owner[0].snapshot.projects.map((project) => project.id),
        future,
      );
    else return yield* Effect.die("Missing owner snapshot");
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("omits hidden enrichment roots from project refreshes", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const items = yield* subscribeOrchestrationV2Shell(
      { requestCompletionMarker: true },
      grantA,
    ).pipe(
      Effect.flatMap((stream) =>
        stream.pipe(
          Stream.tap((item) =>
            item.kind === "synchronized"
              ? Effect.gen(function* () {
                  // A full batch flushes without advancing virtual time or polling.
                  for (let index = 0; index < 64; index += 1) {
                    const id = index % 2 === 0 ? projectA : projectB;
                    yield* PubSub.publish(fixture.changes, {
                      workspaceRoot: `/work/${id}`,
                      enrichment: {
                        ...fixture.enrichment,
                        repositoryIdentity: {
                          canonicalKey: `github.com/fixture/${id}`,
                          rootPath: `/work/${id}`,
                          locator: {
                            source: "git-remote",
                            remoteName: "origin",
                            remoteUrl: `https://github.com/fixture/${id}`,
                          },
                        },
                      },
                      repositoryIdentityResolved: true,
                    });
                  }
                })
              : Effect.void,
          ),
          Stream.take(4),
          Stream.runCollect,
        ),
      ),
      Effect.provide(fixture.services()),
    );
    const refresh = items.at(-1);
    assert.equal(refresh?.kind, "snapshot");
    if (refresh?.kind === "snapshot") {
      assert.deepEqual(refresh.resolvedRepositoryIdentityRoots, [`/work/${projectA}`]);
      assert.deepEqual(
        refresh.snapshot.projects.map((project) => project.id),
        [projectA],
      );
      assert.deepEqual(refresh.snapshot.threads, []);
    }
    assert.notInclude(JSON.stringify(items), "b-secret");
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("archive streams emit permitted deletion IDs and omit hidden tombstones", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const live = Stream.unwrap(
      Effect.gen(function* () {
        const hidden = yield* fixture.updateThread("thread.deleted", archiveB, { deletedAt: now });
        const allowed = yield* fixture.updateThread("thread.deleted", archiveA, { deletedAt: now });
        return Stream.make(hidden, allowed);
      }),
    );
    const items = yield* subscribeOrchestrationV2ArchivedShell(grantA).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(2), Stream.runCollect)),
      Effect.provide(fixture.services(live)),
    );
    assert.deepEqual(items.at(-1), {
      kind: "thread.removed",
      sequence: yield* fixture.events.latestApplicationSequence,
      threadId: archiveA,
    });
    assert.deepEqual(fixture.shellReads, [archiveA]);
    assert.notInclude(JSON.stringify(items), "b-secret");
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("project update/removal collections filter before project enrichment", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const live = Stream.unwrap(
      Effect.gen(function* () {
        const iso = "2026-10-11T00:01:00Z";
        const events = [];
        for (const id of [projectA, projectB]) {
          const updated = yield* fixture.events.appendProjectEvent({
            eventId: EventId.make(`updated:${id}`),
            aggregateKind: "project",
            aggregateId: id,
            occurredAt: iso,
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.meta-updated",
            payload: { projectId: id, title: `${id}:refreshed`, updatedAt: iso },
          });
          yield* fixture.projects.apply(updated);
          events.push(updated);
        }
        return Stream.fromIterable(events);
      }),
    );
    const items = yield* subscribeOrchestrationV2Shell(
      { requestCompletionMarker: true },
      grantA,
    ).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(4), Stream.runCollect)),
      Effect.provide(fixture.services(live)),
    );
    const delta = items.at(-1);
    assert.equal(delta?.kind, "project.updated");
    if (delta?.kind === "project.updated") assert.equal(delta.project.id, projectA);
    assert.notInclude(JSON.stringify(items), "b-secret");
    assert.deepEqual(fixture.enrichedRoots, [`/work/${projectA}`, `/work/${projectA}`]);

    const deleted = Stream.unwrap(
      Effect.gen(function* () {
        const events = [];
        for (const id of [projectA, projectB]) {
          const stored = yield* fixture.events.appendProjectEvent({
            eventId: EventId.make(`deleted:${id}`),
            aggregateKind: "project",
            aggregateId: id,
            occurredAt: "2026-10-11T00:02:00Z",
            commandId: null,
            causationEventId: null,
            correlationId: null,
            metadata: {},
            type: "project.deleted",
            payload: { projectId: id, deletedAt: "2026-10-11T00:02:00Z" },
          });
          yield* fixture.projects.apply(stored);
          events.push(stored);
        }
        return Stream.fromIterable(events);
      }),
    );
    const removed = yield* subscribeOrchestrationV2Shell(
      { requestCompletionMarker: true },
      grantA,
    ).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(4), Stream.runCollect)),
      Effect.provide(fixture.services(deleted)),
    );
    assert.equal(removed.at(-1)?.kind, "project.removed");
    assert.notInclude(JSON.stringify(removed), "b-secret");
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it.effect("archive live updates admit A and suppress B before materialization", () =>
  Effect.gen(function* () {
    const fixture = yield* makeCollectionFixture;
    const live = Stream.unwrap(
      Effect.gen(function* () {
        const allowed = yield* fixture.updateThread("thread.archived", activeA, {
          archivedAt: now,
        });
        const hidden = yield* fixture.updateThread("thread.archived", activeB, { archivedAt: now });
        return Stream.make(allowed, hidden);
      }),
    );
    const items = yield* subscribeOrchestrationV2ArchivedShell(grantA).pipe(
      Effect.flatMap((stream) => stream.pipe(Stream.take(2), Stream.runCollect)),
      Effect.provide(fixture.services(live)),
    );
    const delta = items.at(-1);
    assert.equal(delta?.kind, "thread.updated");
    if (delta?.kind === "thread.updated") assert.equal(delta.thread.id, activeA);
    assert.deepEqual(fixture.shellReads, [activeA]);
    assert.notInclude(JSON.stringify(items), "b-secret");
  }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

it("accepts only the current orchestration protocol before websocket RPC setup", () => {
  assert.isTrue(
    hasCompatibleOrchestrationProtocol(
      new URL(`https://host.test/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION}`),
    ),
  );
  assert.isFalse(hasCompatibleOrchestrationProtocol(new URL("https://host.test/ws")));
  assert.isFalse(
    hasCompatibleOrchestrationProtocol(
      new URL(`https://host.test/ws?orchestrationProtocol=${ORCHESTRATION_PROTOCOL_VERSION - 1}`),
    ),
  );
});

it("keeps full thread snapshot fallback unless the client opts into bounded history", () => {
  assert.isFalse(shouldUseBoundedThreadSnapshot({}));
  assert.isFalse(shouldUseBoundedThreadSnapshot({ acceptBoundedSnapshot: false }));
  assert.isTrue(shouldUseBoundedThreadSnapshot({ acceptBoundedSnapshot: true }));
});

it.effect("does not block server config when editor discovery never resolves", () =>
  Effect.gen(function* () {
    const discoveryInterrupted = yield* Deferred.make<void>();
    const responseFiber = yield* resolveAvailableEditorsForConfig(
      Effect.never.pipe(
        Effect.onInterrupt(() => Deferred.succeed(discoveryInterrupted, undefined)),
      ),
    ).pipe(Effect.forkChild);

    yield* TestClock.adjust(Duration.seconds(5));

    const availableEditors = yield* Fiber.join(responseFiber);
    yield* Deferred.await(discoveryInterrupted);
    assert.deepEqual(availableEditors, []);
  }),
);

// Only the fields the late-editor fold reads or rewrites.
const snapshotConfig = (fields: Partial<ServerConfig>) =>
  ({ availableEditors: [], settings: {}, ...fields }) as unknown as ServerConfig;

const settingsUpdated = (settings: object): ServerConfigStreamEvent => ({
  version: 1,
  type: "settingsUpdated",
  payload: { settings: settings as ServerConfig["settings"] },
});

it.effect("resends late editors without rolling back updates already sent", () =>
  Effect.gen(function* () {
    const settingsSent = yield* Deferred.make<void>();
    const events = yield* withLateEditorConfig(
      snapshotConfig({ settings: { enableProviderUpdateChecks: true } as never }),
      Stream.make(settingsUpdated({ enableProviderUpdateChecks: false })),
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        // Holds the late snapshot until the settings change has gone out.
        resolveFileManagerRevealKind: () =>
          Deferred.await(settingsSent).pipe(Effect.as("file-explorer" as const)),
      },
    ).pipe(
      Stream.tap((event) =>
        event.type === "settingsUpdated" ? Deferred.succeed(settingsSent, undefined) : Effect.void,
      ),
      Stream.runCollect,
    );

    const [first, second] = Array.from(events);
    assert.equal(events.length, 2);
    assert.equal(first?.type, "settingsUpdated");
    assert.equal(second?.type, "snapshot");
    if (second?.type === "snapshot") {
      assert.deepEqual(second.config.availableEditors, ["file-manager"]);
      assert.equal(second.config.shellRevealInFileManagerKind, "file-explorer");
      assert.deepEqual(second.config.settings, { enableProviderUpdateChecks: false } as never);
    }
  }),
);

it.effect("sends no late snapshot when the scan matches the snapshot", () =>
  Effect.gen(function* () {
    const events = yield* withLateEditorConfig(
      snapshotConfig({ availableEditors: ["vscode"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["vscode"]),
        resolveFileManagerRevealKind: () => Effect.succeed(undefined),
      },
    ).pipe(Stream.runCollect);

    assert.equal(events.length, 0);
  }),
);

it.effect("resends a file manager reveal kind that missed the snapshot", () =>
  Effect.gen(function* () {
    const events = yield* withLateEditorConfig(
      snapshotConfig({ availableEditors: ["file-manager"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: () => Effect.succeed("file-explorer"),
      },
    ).pipe(Stream.runCollect);

    const [late] = Array.from(events);
    assert.equal(events.length, 1);
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.shellRevealInFileManagerKind, "file-explorer");
    }
  }),
);

// The real launcher on Windows over a filesystem whose probes park until
// released, like a host too busy to finish discovery inside the snapshot timeout.
const makeParkedWindowsLauncher = Effect.gen(function* () {
  const parkedProbes = yield* Queue.unbounded<void>();
  const release = yield* Deferred.make<void>();
  const launcher = yield* ExternalLauncher.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        FileSystem.layerNoop({
          stat: () =>
            Queue.offer(parkedProbes, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as({ type: "File" } as FileSystem.File.Info),
            ),
        }),
        Path.layer,
        Layer.succeed(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("unexpected spawn")),
        ),
      ),
    ),
  );
  const onWindows = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(HostProcessPlatform, "win32"),
      Effect.provide(
        ConfigProvider.layer(
          ConfigProvider.fromEnv({
            env: { PATH: "C:\\t3-late-editors-test", PATHEXT: ".EXE" },
          }),
        ),
      ),
    );
  return {
    editors: {
      resolveAvailableEditors: () => onWindows(launcher.resolveAvailableEditors()),
      resolveFileManagerRevealKind: () => onWindows(launcher.resolveFileManagerRevealKind()),
    },
    probeParked: Queue.take(parkedProbes),
    releaseProbes: Deferred.succeed(release, undefined),
  };
});

it.effect("recovers editors after a real scan outlasts the config timeout", () =>
  Effect.gen(function* () {
    const { editors, probeParked, releaseProbes } = yield* makeParkedWindowsLauncher;

    const snapshotFiber = yield* resolveAvailableEditorsForConfig(
      editors.resolveAvailableEditors(),
    ).pipe(Effect.forkChild);
    yield* probeParked;
    yield* TestClock.adjust(Duration.seconds(5));
    const snapshotEditors = yield* Fiber.join(snapshotFiber);
    assert.deepEqual(snapshotEditors, []);

    const lateFiber = yield* withLateEditorConfig(
      snapshotConfig({ availableEditors: snapshotEditors }),
      Stream.empty,
      editors,
    ).pipe(Stream.runCollect, Effect.forkChild);
    yield* releaseProbes;

    const [late] = Array.from(yield* Fiber.join(lateFiber));
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.availableEditors.includes("vscode"), true);
    }
  }).pipe(Effect.scoped),
);

it.effect("recovers a reveal kind whose real probe outlasts the config timeout", () =>
  Effect.gen(function* () {
    const { editors, probeParked, releaseProbes } = yield* makeParkedWindowsLauncher;

    // The snapshot's bounded probe timed out: file manager, but no reveal kind.
    const lateFiber = yield* withLateEditorConfig(
      snapshotConfig({ availableEditors: ["file-manager"] }),
      Stream.empty,
      {
        resolveAvailableEditors: () => Effect.succeed(["file-manager"]),
        resolveFileManagerRevealKind: editors.resolveFileManagerRevealKind,
      },
    ).pipe(Stream.runCollect, Effect.forkChild);
    yield* probeParked;
    yield* TestClock.adjust(Duration.seconds(6));
    yield* releaseProbes;

    const [late] = Array.from(yield* Fiber.join(lateFiber));
    assert.equal(late?.type, "snapshot");
    if (late?.type === "snapshot") {
      assert.equal(late.config.shellRevealInFileManagerKind, "file-explorer");
    }
  }).pipe(Effect.scoped),
);
