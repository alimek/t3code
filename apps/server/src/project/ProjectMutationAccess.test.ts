import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthSessionId,
  EnvironmentHttpApi,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentScopeRequiredError,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION,
  AuthAllResourceGrant,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  CommandId,
  EnvironmentAuthorizationError,
  EventId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { Etag, HttpRouter } from "effect/http";
import * as SessionStore from "../auth/SessionStore.ts";
import * as Startup from "../serverRuntimeStartup.ts";
import * as ProjectHttp from "./http.ts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { planProjectCommand } from "../orchestration-v2/ProjectCommands.ts";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  authorizeCommand,
  trustedMutationAuthority,
  type MutationAuthority,
} from "../auth/CommandAuthorization.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Sink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Replay from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as Registry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as IdAllocator from "../orchestration-v2/IdAllocator.ts";
import * as Legacy from "../orchestration-v2/legacy/LegacyV1ThreadImporter.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadCommands from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as Enrichment from "./ProjectEnrichmentService.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectService from "./ProjectService.ts";
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
  provider,
  threadCreated,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";

const isLiveProjectRequired = Schema.is(Sink.ThreadLiveProjectRequiredError);
const isProjectCommandMismatch = Schema.is(Sink.ProjectCommandEventMismatchError);
const authority: MutationAuthority = {
  current: Effect.succeed({ scopes: [AuthOrchestrationOperateScope], resourceGrant: grantA }),
};
const owner: MutationAuthority = {
  current: Effect.succeed({
    scopes: [AuthOrchestrationOperateScope],
    resourceGrant: AuthAllResourceGrant,
  }),
};
const layerTest = Layer.mergeAll(
  collectionStoreLayer,
  EventStore.layer.pipe(Layer.provide(Sqlite.layerMemory)),
  EffectOutbox.layer.pipe(Layer.provide(Sqlite.layerMemory)),
  Receipts.layer.pipe(Layer.provide(Sqlite.layerMemory)),
  Replay.layerWithRegistry({ name: "project-mutation-access" }, Registry.layerFromAdapters([]), {
    databaseLayer: Sqlite.layerMemory,
    runEffectWorker: false,
  }),
);
const makeFixture = Effect.gen(function* () {
  const f = yield* makeCollectionFixture;
  const sql = yield* SqlClient.SqlClient;
  const sink = yield* Sink.EventSinkV2;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const normalized: string[] = [];
  const imported: ThreadId[] = [];
  const enriched: string[] = [];
  const external = Effect.fnUntraced(function* <T extends string>(path: T, calls: T[]) {
    assert.isTrue((yield* Effect.serviceOption(sql.transactionService))._tag === "None");
    yield* sql`SELECT 1`;
    calls.push(path);
  });
  const workspacePaths = WorkspacePaths.WorkspacePaths.of({
    normalizeWorkspaceRoot: (root) =>
      external(root, normalized).pipe(Effect.as(root), Effect.orDie),
    resolveRelativePathWithinRoot: () => Effect.die("unused path resolver"),
  });
  const importer = Layer.mock(Legacy.LegacyV1ThreadImporter)({
    ensureTranscript: (id) =>
      external(id, imported).pipe(
        Effect.as({ importedThreadCount: 0, importedMessageCount: 0 }),
        Effect.orDie,
      ),
  });
  const threads = yield* Threads.ThreadManagementService.pipe(
    Effect.provide(Threads.layerWithLegacyImporter.pipe(Layer.provide(importer))),
  );
  const deps = Layer.mergeAll(
    ThreadCommands.layer,
    IdAllocator.layer,
    Layer.succeed(WorkspacePaths.WorkspacePaths, workspacePaths),
    importer,
    Layer.mock(Enrichment.ProjectEnrichmentService)({
      getAvailable: (root) => external(root, enriched).pipe(Effect.as(f.enrichment), Effect.orDie),
      peek: (root) => external(root, enriched).pipe(Effect.as(f.enrichment), Effect.orDie),
      invalidate: () => Effect.void,
    }),
  );
  const makeService = (customSink = sink) =>
    ProjectService.make.pipe(
      Effect.provide(deps),
      Effect.provideService(Sink.EventSinkV2, customSink),
    );
  const service = yield* makeService();
  return {
    ...f,
    sql,
    sink,
    receipts,
    service,
    makeService,
    imported,
    threads,
    workspacePaths,
    normalized,
    enriched,
  };
});
const isDispatchError = Schema.is(Orchestrator.OrchestratorDispatchError);
const isSinkError = Schema.is(Sink.EventSinkWriteError);
const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
const decodeScopeError = Schema.decodeUnknownSync(EnvironmentScopeRequiredError);
const denied = Effect.fnUntraced(function* <A, E>(effect: Effect.Effect<A, E>) {
  let error: unknown = yield* Effect.flip(effect.pipe(Effect.asVoid));
  if (isSinkError(error) || isDispatchError(error)) error = error.cause;
  assert.isTrue(isAuthorizationError(error));
  if (isAuthorizationError(error)) {
    assert.equal(error.requiredScope, AuthOrchestrationOperateScope);
    assert.notInclude(JSON.stringify(error), projectB);
  }
});
const updateInput = (id: string, projectId = projectA) => ({
  commandId: CommandId.make(id),
  projectId,
  title: "Allowed",
});

it.effect(
  "raw hidden/missing IDs and restricted root/scripts/bootstrap deny before effects or decoding",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* f.sql`UPDATE projection_projects SET scripts_json = 'malformed' WHERE project_id = ${projectB}`;
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'malformed' WHERE thread_id = ${activeB}`;
      for (const projectId of [projectB, ProjectId.make("missing"), ProjectId.make("forged")]) {
        yield* denied(f.service.update(authority, updateInput(`update:${projectId}`, projectId)));
        yield* denied(
          f.service.delete(authority, {
            commandId: CommandId.make(`delete:${projectId}`),
            projectId,
            force: true,
          }),
        );
        yield* denied(
          f.service.create(authority, {
            ...updateInput(`create:${projectId}`, projectId),
            workspaceRoot: "/forbidden",
            createWorkspaceRootIfMissing: true,
          }),
        );
      }
      for (const patch of [
        { workspaceRoot: "/forbidden" },
        { workspaceRoot: `/work/${projectA}` },
        { scripts: [] },
      ])
        yield* denied(
          f.service.update(authority, {
            ...updateInput(`retarget:${JSON.stringify(patch)}`),
            ...patch,
          }),
        );
      yield* denied(
        f.service.bootstrap(authority, {
          ...updateInput("bootstrap"),
          workspaceRoot: "/forbidden",
        }),
      );
      const view: MutationAuthority = {
        current: Effect.succeed({ scopes: [AuthOrchestrationReadScope], resourceGrant: grantA }),
      };
      yield* denied(f.service.update(view, updateInput("view")));
      assert.deepEqual(f.normalized, []);
      assert.deepEqual(f.imported, []);
      assert.deepEqual(f.enriched, []);
      assert.lengthOf(yield* f.sql`SELECT command_id FROM orchestration_command_receipts`, 0);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "operate-only metadata, force cascade, replay and own tombstones work without external I/O in SQL",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const input = updateInput("allowed-metadata");
      const project = yield* f.service.update(authority, {
        ...input,
        defaultModelSelection: { instanceId: provider, model: "fixture" },
        defaultThreadEnvMode: "worktree",
      });
      assert.equal(project.title, "Allowed");
      assert.equal(project.defaultThreadEnvMode, "worktree");
      yield* f.service.update(authority, input);
      const deletion = {
        commandId: CommandId.make("allowed-delete"),
        projectId: projectA,
        force: true,
      };
      const deleted = yield* f.service.delete(authority, deletion);
      assert.isNotNull(deleted.deletedAt);
      const imports = [...f.imported];
      assert.deepEqual(yield* f.service.delete(authority, deletion), deleted);
      assert.deepEqual(f.imported, imports);
      assert.lengthOf(
        yield* f.sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id = ${projectA} AND deleted_at IS NULL`,
        0,
      );
      assert.lengthOf(
        yield* f.sql`SELECT command_id FROM orchestration_events WHERE command_id = ${deletion.commandId}`,
        1,
      );
      assert.isTrue((yield* f.projections.getThread(activeB)).deletedAt === null);
      const failure = yield* Effect.flip(
        f.service.delete(authority, { ...deletion, commandId: CommandId.make("other-delete") }),
      );
      assert.equal(failure._tag, "ProjectOperationError");
      const created = yield* f.service.bootstrap(owner, {
        ...updateInput("owner-bootstrap", ProjectId.make("new-owner")),
        workspaceRoot: "/owner/new",
      });
      assert.isTrue(created.created);
      assert.isFalse(
        (yield* f.service.bootstrap(trustedMutationAuthority, {
          ...updateInput("trusted-bootstrap", ProjectId.make("other-owner")),
          workspaceRoot: "/owner/new",
        })).created,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "original hidden project/thread receipts deny before decoding or target effects, including malformed receipts",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      for (const kind of ["project", "thread"] as const)
        for (const status of ["accepted", "rejected"] as const)
          for (const malformed of [false, true]) {
            const id = CommandId.make(`hidden-receipt:${kind}:${status}:${malformed}`);
            yield* f.sql`INSERT INTO orchestration_command_receipts (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
        VALUES (${id}, ${kind}, ${kind === "project" ? projectB : activeB}, 'secret-command', ${malformed ? "bad" : "2026-10-11T00:00:00Z"}, 0, ${status}, 'secret payload')`;
            yield* denied(f.service.update(authority, updateInput(id)));
            yield* denied(
              f.service.delete(authority, { commandId: id, projectId: projectA, force: true }),
            );
            yield* denied(
              f.service.create(authority, { ...updateInput(id), workspaceRoot: "/forbidden" }),
            );
          }
      assert.deepEqual(f.normalized, []);
      assert.deepEqual(f.imported, []);
      assert.deepEqual(f.enriched, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "fresh and competing rejected project receipts redact unknown original diagnostics while owners retain them",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const diagnostic = `Conflict from ${projectB} at /hidden/root`;
      for (const caller of [authority, owner, trustedMutationAuthority] as const) {
        const id = CommandId.make(
          `rejected:${caller === authority ? "restricted" : caller === owner ? "owner" : "trusted"}`,
        );
        const command = { ...updateInput(id), type: "project.meta.update" as const };
        const result = yield* f.sink.commitRejectedProjectCommand(
          { authority: caller, command },
          {
            commandId: id,
            projectId: projectA,
            commandType: command.type,
            rejectedAt: now,
            error: diagnostic,
          },
        );
        assert.equal(result.error, caller === authority ? "Previously rejected." : diagnostic);
        const replay = yield* f.sink.commitRejectedProjectCommand(
          { authority: caller, command },
          {
            commandId: id,
            projectId: projectA,
            commandType: command.type,
            rejectedAt: now,
            error: "new rejection",
          },
        );
        assert.equal(replay.error, result.error);
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "competing hidden accepted/rejected receipts are admitted before decode and roll back the new reservation",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      for (const kind of ["project", "thread"] as const)
        for (const rejected of [false, true])
          for (const malformed of [false, true]) {
            const id = CommandId.make(`competing:${kind}:${rejected}:${malformed}`);
            const command = { ...updateInput(id), type: "project.meta.update" as const };
            const spy = vi.spyOn(f.receipts, "insertIfAbsent").mockImplementation(() =>
              f.sql`INSERT INTO orchestration_command_receipts (command_id, aggregate_kind, aggregate_id, command_type, accepted_at, result_sequence, status, error)
          VALUES (${id}, ${kind}, ${kind === "project" ? projectB : activeB}, 'secret-command', ${malformed ? "bad" : "2026-10-11T00:00:00Z"}, 0, ${rejected ? "rejected" : "accepted"}, 'secret')`.pipe(
                Effect.as(false),
                Effect.orDie,
              ),
            );
            try {
              yield* denied(f.service.update(authority, updateInput(id)));
            } finally {
              spy.mockRestore();
            }
            assert.lengthOf(
              yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${id}`,
              0,
            );
            assert.lengthOf(
              yield* f.sql`SELECT command_id FROM orchestration_events WHERE command_id = ${id}`,
              0,
            );
            // Fresh rejected transaction has the same collision guard.
            const second = vi.spyOn(f.receipts, "insertIfAbsent").mockImplementation(() =>
              f.receipts
                .upsert({
                  commandId: id,
                  ...(kind === "project" ? { projectId: projectB } : { threadId: activeB }),
                  commandType: command.type,
                  acceptedAt: now,
                  resultSequence: 0,
                  status: "rejected",
                  error: "secret",
                })
                .pipe(Effect.as(false)),
            );
            try {
              yield* denied(
                f.sink.commitRejectedProjectCommand(
                  { authority, command },
                  {
                    commandId: id,
                    projectId: projectA,
                    commandType: command.type,
                    rejectedAt: now,
                    error: "own",
                  },
                ),
              );
            } finally {
              second.mockRestore();
            }
          }
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["grant", "scope", "membership"] as const)(
  "child deletion rechecks current %s at its actual commit before any new write",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const current = yield* Ref.make({
        scopes: [AuthOrchestrationOperateScope],
        resourceGrant: grantA,
      });
      const caller: MutationAuthority = { current: Ref.get(current) };
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let child: ThreadId | undefined;
      const service = yield* f.makeService(
        Sink.EventSinkV2.of({
          ...f.sink,
          commitCommand: (admission, input) =>
            Effect.gen(function* () {
              child = input.threadId;
              yield* Deferred.succeed(reached, undefined);
              yield* Deferred.await(release);
              return yield* f.sink.commitCommand(admission, input);
            }),
        }),
      );
      const id = CommandId.make(`child-cut:${cut}`);
      const pending = yield* service
        .delete(caller, { commandId: id, projectId: projectA, force: true })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(reached);
      if (cut === "grant")
        yield* Ref.set(current, {
          scopes: [AuthOrchestrationOperateScope],
          resourceGrant: grantNone,
        });
      if (cut === "scope") yield* Ref.set(current, { scopes: [], resourceGrant: grantA });
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${child!}`;
      yield* Deferred.succeed(release, undefined);
      assert.equal((yield* Fiber.join(pending))._tag, "EnvironmentAuthorizationError");
      assert.lengthOf(
        yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id LIKE ${`${id}%`}`,
        0,
      );
      assert.lengthOf(yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox`, 0);
      assert.isNull((yield* f.projections.getThread(child!)).deletedAt);
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["grant", "scope"] as const)(
  "project write rechecks current %s at its actual accepted and rejected transaction",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      for (const rejected of [false, true]) {
        const current = yield* Ref.make({
          scopes: [AuthOrchestrationOperateScope],
          resourceGrant: grantA,
        });
        const caller: MutationAuthority = { current: Ref.get(current) };
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const gate = <A, E>(operation: Effect.Effect<A, E>) =>
          Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(operation),
          );
        const service = yield* f.makeService(
          Sink.EventSinkV2.of({
            ...f.sink,
            commitProjectCommand: (admission, input) =>
              gate(f.sink.commitProjectCommand(admission, input)),
            commitRejectedProjectCommand: (admission, input) =>
              gate(f.sink.commitRejectedProjectCommand(admission, input)),
          }),
        );
        const id = CommandId.make(`project-cut:${cut}:${rejected}`);
        const pending = yield* service
          .update(caller, {
            ...updateInput(id),
            ...(rejected
              ? { projectIcon: { kind: "monogram", text: "too long", color: "red" } as const }
              : {}),
          })
          .pipe(Effect.flip, Effect.forkChild);
        yield* Deferred.await(reached);
        yield* Ref.set(current, {
          scopes: cut === "scope" ? [] : [AuthOrchestrationOperateScope],
          resourceGrant: cut === "grant" ? grantNone : grantA,
        });
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* Fiber.join(pending))._tag, "EnvironmentAuthorizationError");
        assert.lengthOf(
          yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${id}`,
          0,
        );
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["create", "move"] as const)(
  "late child %s prevents deletion; deletion winning prevents new live children",
  (operation) =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      // Remove initial A children; pause only the project commit, outside its SQL transaction.
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET deleted_at = '2026-10-11' WHERE project_id = ${projectA}`;
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const service = yield* f.makeService(
        Sink.EventSinkV2.of({
          ...f.sink,
          commitProjectCommand: (admission, input) =>
            Deferred.succeed(reached, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(f.sink.commitProjectCommand(admission, input)),
            ),
        }),
      );
      const deletion = {
        commandId: CommandId.make("concurrent-delete"),
        projectId: projectA,
        force: true,
      };
      const pending = yield* service
        .delete(authority, deletion)
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(reached);
      const newChild = ThreadId.make("late-child");
      if (operation === "create")
        yield* f.sink.write({ events: [threadCreated(newChild, projectA)] });
      else {
        const moving = yield* f.projections.getThread(activeB);
        yield* f.sink.write({
          events: [
            {
              id: EventId.make("move-before-delete"),
              type: "thread.metadata-updated",
              threadId: activeB,
              occurredAt: now,
              payload: { ...moving, projectId: projectA },
            },
          ],
        });
      }
      yield* Deferred.succeed(release, undefined);
      assert.equal((yield* Fiber.join(pending))._tag, "ProjectNotEmptyError");
      assert.lengthOf(
        yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${deletion.commandId}`,
        0,
      );
      yield* f.service.delete(authority, deletion);
      const attempt = threadCreated(ThreadId.make("after-delete"), projectA);
      const failure = yield* Effect.flip(f.sink.write({ events: [attempt] }));
      assert.equal(failure._tag, "EventSinkWriteError");
      assert.isTrue(isLiveProjectRequired(failure.cause));
      assert.deepInclude(failure.cause, { threadId: attempt.threadId, projectId: projectA });
      const threadB = yield* f.projections.getThread(activeB);
      yield* Effect.flip(
        f.sink.write({
          events: [
            {
              id: EventId.make("move-after-delete"),
              type: "thread.metadata-updated",
              threadId: activeB,
              occurredAt: now,
              payload: { ...threadB, projectId: projectA, deletedAt: null },
            },
          ],
        }),
      );
      assert.equal(
        (yield* f.projections.getThread(activeB)).projectId,
        operation === "create" ? projectB : projectA,
      );
      assert.lengthOf(
        yield* f.sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id = ${projectA} AND deleted_at IS NULL`,
        0,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "raw thread create/metadata workspace bindings reject hidden/unrelated paths and preserve registered own roots",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const create = {
        type: "thread.create",
        commandId: CommandId.make("binding-create"),
        threadId: ThreadId.make("new-binding"),
        projectId: projectA,
        title: "New",
        modelSelection: { instanceId: provider, model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      } as const;
      for (const path of [`/work/${projectB}`, "/unrelated"]) {
        yield* denied(f.threads.dispatch(authority, { ...create, worktreePath: path }));
        yield* denied(
          authorizeCommand(f.sql, authority, {
            type: "thread.metadata.update",
            commandId: CommandId.make("binding-metadata"),
            threadId: activeA,
            worktreePath: path,
          }),
        );
      }
      yield* authorizeCommand(f.sql, authority, { ...create, worktreePath: `/work/${projectA}` });
      yield* authorizeCommand(f.sql, authority, {
        type: "thread.metadata.update",
        commandId: CommandId.make("own-binding"),
        threadId: activeA,
        worktreePath: `/work/${projectA}`,
      });
      yield* authorizeCommand(f.sql, trustedMutationAuthority, {
        ...create,
        worktreePath: "/server/prepared/worktree",
      });
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "raw cascade receipts and stored project retarget receipts deny before import/enrichment",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const deletion = {
        commandId: CommandId.make("foreign-cascade"),
        projectId: projectA,
        force: true,
      };
      yield* f.receipts.upsert({
        commandId: CommandId.make(`${deletion.commandId}:delete-thread:${activeA}`),
        threadId: activeB,
        commandType: "thread.delete",
        acceptedAt: now,
        resultSequence: 0,
        status: "accepted",
        error: null,
      });
      yield* denied(f.service.delete(authority, deletion));
      assert.deepEqual(f.imported, []);
      const retarget = updateInput("old-owner-retarget");
      yield* f.service.update(owner, { ...retarget, workspaceRoot: "/hidden/root" });
      f.normalized.length = 0;
      f.enriched.length = 0;
      yield* denied(f.service.update(authority, retarget));
      assert.deepEqual(f.normalized, []);
      assert.deepEqual(f.enriched, []);
      yield* denied(
        Reflect.apply(f.service.update, undefined, [undefined, updateInput("no-authority")]),
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect.each(["grant", "scope", "membership"] as const)(
  "final project deletion rechecks %s after its durable cascade",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const current = yield* Ref.make({
        scopes: [AuthOrchestrationOperateScope],
        resourceGrant: grantA,
      });
      const caller: MutationAuthority = { current: Ref.get(current) };
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const service = yield* f.makeService(
        Sink.EventSinkV2.of({
          ...f.sink,
          commitProjectCommand: (admission, input) =>
            Deferred.succeed(reached, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(f.sink.commitProjectCommand(admission, input)),
            ),
        }),
      );
      const id = CommandId.make(`final-delete-cut:${cut}`);
      const pending = yield* service
        .delete(caller, { commandId: id, projectId: projectA, force: true })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(reached);
      if (cut === "grant")
        yield* Ref.set(current, {
          scopes: [AuthOrchestrationOperateScope],
          resourceGrant: grantNone,
        });
      if (cut === "scope") yield* Ref.set(current, { scopes: [], resourceGrant: grantA });
      if (cut === "membership")
        yield* f.sql`DELETE FROM projection_projects WHERE project_id = ${projectA}`;
      yield* Deferred.succeed(release, undefined);
      assert.equal((yield* Fiber.join(pending))._tag, "EnvironmentAuthorizationError");
      assert.lengthOf(
        yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${id}`,
        0,
      );
      assert.lengthOf(
        yield* f.sql`SELECT command_id FROM orchestration_events WHERE command_id = ${id}`,
        0,
      );
      assert.lengthOf(
        yield* f.sql`SELECT thread_id FROM orchestration_v2_projection_threads WHERE project_id = ${projectA} AND deleted_at IS NULL`,
        0,
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "cascade publication follows actual child/project commits and replay publishes nothing",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const eventStore = yield* EventStore.EventStoreV2;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      const publish = eventStore.publishCommitted;
      const notify = outbox.notifyAvailable;
      const sequences: number[] = [];
      let notifications = 0;
      const published = vi.spyOn(eventStore, "publishCommitted").mockImplementation((events) =>
        Effect.gen(function* () {
          assert.isTrue((yield* Effect.serviceOption(f.sql.transactionService))._tag === "None");
          for (const event of events) {
            assert.lengthOf(
              yield* f.sql`SELECT sequence FROM orchestration_events WHERE sequence = ${event.sequence}`,
              1,
            );
            sequences.push(event.sequence);
          }
          return yield* publish(events);
        }).pipe(Effect.orDie),
      );
      const notified = vi.spyOn(outbox, "notifyAvailable").mockImplementation((count = 1) =>
        Effect.gen(function* () {
          assert.isTrue((yield* Effect.serviceOption(f.sql.transactionService))._tag === "None");
          assert.isAtLeast(
            (yield* f.sql`SELECT effect_id FROM orchestration_v2_effect_outbox`).length,
            count,
          );
          notifications += count;
          return yield* notify(count);
        }).pipe(Effect.orDie),
      );
      try {
        const deletion = {
          commandId: CommandId.make("published-cascade"),
          projectId: projectA,
          force: true,
        };
        yield* f.service.delete(authority, deletion);
        assert.lengthOf(sequences, 3);
        assert.deepEqual(
          sequences,
          [...sequences].sort((a, b) => a - b),
        );
        assert.equal(notifications, 2);
        yield* f.service.delete(authority, deletion);
        assert.lengthOf(sequences, 3);
        assert.equal(notifications, 2);
      } finally {
        published.mockRestore();
        notified.mockRestore();
      }
    }).pipe(Effect.provide(layerTest)),
);

class ProjectApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.projects) {}

it.effect(
  "HTTP mutations reload current session authority with the principal ceiling and retain typed denial",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const sessionId = AuthSessionId.make("project-http-access");
      const principal = {
        sessionId,
        subject: "fixture",
        method: "browser-session-cookie" as const,
        scopes: new Set([AuthOrchestrationOperateScope]),
        resourceGrant: grantA,
      };
      const grant = yield* Ref.make(grantA);
      let calls = 0;
      const routes = HttpApiBuilder.layer(ProjectApi).pipe(
        Layer.provide(ProjectHttp.layer),
        Layer.provide(Layer.succeed(ProjectService.ProjectService, f.service)),
        Layer.provide(
          Layer.mock(SessionStore.SessionStore)({
            cookieName: "fixture",
            legacyCookieName: undefined,
            getAuthority: (id, ceiling) =>
              Ref.get(grant).pipe(
                Effect.map((resourceGrant) => {
                  assert.equal(id, sessionId);
                  assert.deepEqual(ceiling?.scopes, [AuthOrchestrationOperateScope]);
                  assert.deepEqual(ceiling?.resourceGrant, grantA);
                  calls++;
                  return {
                    deviceId: sessionId,
                    authorityRevision: 1,
                    expiresAt: now,
                    scopes: [AuthOrchestrationOperateScope],
                    resourceGrant,
                  };
                }),
              ),
          }),
        ),
        Layer.provide(
          Layer.mock(Startup.ServerRuntimeStartup)({ enqueueCommand: (operation) => operation }),
        ),
        Layer.provide(
          Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
            effect.pipe(Effect.provideService(EnvironmentAuthenticatedPrincipal, principal)),
          ),
        ),
        Layer.provide(NodeHttpPlatform.layer),
        Layer.provide(Etag.layerWeak),
        Layer.provide(NodeServices.layer),
      );
      yield* Effect.acquireUseRelease(
        Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
        ({ handler }) =>
          Effect.gen(function* () {
            const request = (id: string, projectId = projectA) =>
              Effect.promise(() =>
                handler(
                  new Request("http://fixture/api/projects/mutate", {
                    method: "POST",
                    headers: {
                      "content-type": "application/json",
                      [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION),
                    },
                    body: JSON.stringify({ type: "project.update", ...updateInput(id, projectId) }),
                  }),
                ),
              );
            assert.equal((yield* request("http-allowed")).status, 200);
            assert.isAbove(calls, 1);
            const hidden = yield* request("http-hidden", projectB);
            assert.equal(hidden.status, 403);
            assert.notInclude(yield* Effect.promise(() => hidden.text()), projectB);
            yield* Ref.set(grant, grantNone);
            const cut = yield* request("http-cut");
            assert.equal(cut.status, 403);
            const body = yield* Effect.promise(() => cut.json());
            assert.equal(decodeScopeError(body).requiredScope, AuthOrchestrationOperateScope);
            assert.lengthOf(
              yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = 'http-cut'`,
              0,
            );
          }),
        ({ dispose }) => Effect.promise(dispose),
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "bootstrap rechecks a cut during normalization before decoding an existing root owner or enriching it",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* f.sql`UPDATE projection_projects SET scripts_json = 'malformed' WHERE project_id = ${projectB}`;
      const current = yield* Ref.make(AuthAllResourceGrant);
      const caller: MutationAuthority = {
        current: Ref.get(current).pipe(
          Effect.map((resourceGrant) => ({
            scopes: [AuthOrchestrationOperateScope],
            resourceGrant,
          })),
        ),
      };
      const normalize = f.workspacePaths.normalizeWorkspaceRoot;
      const spy = vi
        .spyOn(f.workspacePaths, "normalizeWorkspaceRoot")
        .mockImplementation((root, options) =>
          Ref.set(current, grantA).pipe(Effect.andThen(normalize(root, options))),
        );
      try {
        yield* denied(
          f.service.bootstrap(caller, {
            ...updateInput("bootstrap-cut", ProjectId.make("new-root")),
            workspaceRoot: `/work/${projectB}`,
          }),
        );
      } finally {
        spy.mockRestore();
      }
      assert.lengthOf(f.normalized, 1);
      assert.deepEqual(f.enriched, []);
      assert.lengthOf(yield* f.sql`SELECT command_id FROM orchestration_command_receipts`, 0);
    }).pipe(Effect.provide(layerTest)),
);

it.effect("a forged project event cannot delete a different project with live children", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* f.sql`UPDATE orchestration_v2_projection_threads SET deleted_at = '2026-10-11' WHERE project_id = ${projectA}`;
    const command = {
      type: "project.delete" as const,
      commandId: CommandId.make("forged-event"),
      projectId: projectA,
    };
    const planned = planProjectCommand({
      command,
      state: {
        project: Option.getOrThrow(yield* f.projects.get(projectA)),
        workspaceOwner: undefined,
      },
      eventId: EventId.make("forged-event"),
      now,
    });
    if (Result.isFailure(planned) || planned.success.type !== "project.deleted")
      return assert.fail("Expected a deletion plan");
    const failure = yield* Effect.flip(
      f.sink.commitProjectCommand(
        { authority: trustedMutationAuthority, command },
        {
          commandId: command.commandId,
          projectId: projectA,
          commandType: command.type,
          acceptedAt: now,
          event: {
            ...planned.success,
            payload: { ...planned.success.payload, projectId: projectB },
          },
        },
      ),
    );
    assert.equal(failure._tag, "EventSinkWriteError");
    assert.isTrue(isProjectCommandMismatch(failure.cause));
    assert.deepInclude(failure.cause, { commandId: command.commandId, projectId: projectA });
    assert.isNull(Option.getOrThrow(yield* f.projects.get(projectB)).deletedAt);
    assert.lengthOf(
      yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`,
      0,
    );
  }).pipe(Effect.provide(layerTest)),
);
