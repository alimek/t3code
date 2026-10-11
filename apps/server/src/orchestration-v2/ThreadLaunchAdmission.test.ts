import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  ChatAttachmentId,
  CommandId,
  ComposerContextId,
  EnvironmentAuthorizationError,
  EnvironmentId,
  EventId,
  ProviderDriverKind,
  ProjectId,
  MessageId,
  PlanId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type ChatAttachment,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { trustedMutationAuthority, type MutationAuthority } from "../auth/CommandAuthorization.ts";
import { createPendingAttachmentId, toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ManagedFolders from "../project/ManagedProjectFolders.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as SetupScripts from "../project/ProjectSetupScriptRunner.ts";
import * as SetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProviderRegistryMock from "../provider/testUtils/providerRegistryMock.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as Claims from "./AttachmentClaims.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as Sink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as Legacy from "./legacy/LegacyV1ThreadImporter.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
import * as Launch from "./ThreadLaunchService.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Intake from "./ThreadMessageIntake.ts";
import * as Replay from "./testkit/ProviderReplayHarness.ts";
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
} from "./projectCollectionTestFixtures.ts";

const authority: MutationAuthority = {
  current: Effect.succeed({ scopes: [AuthOrchestrationOperateScope], resourceGrant: grantA }),
};
const modelSelection = { instanceId: provider, model: "fixture" };
const provenance = { createdBy: "user", creationSource: "web" } as const;
const claimedB = {
  type: "file",
  id: ChatAttachmentId.make(
    `${toSafeThreadAttachmentSegment(activeB)}-00000000-0000-4000-8000-000000000001`,
  ),
  name: "hidden.txt",
  mimeType: "text/plain",
  sizeBytes: 1,
} satisfies ChatAttachment;
const contextB = {
  version: 1 as const,
  records: [
    {
      version: 1 as const,
      contextId: ComposerContextId.make("hidden"),
      kind: "thread" as const,
      threadId: activeB,
      title: "hidden",
      label: "hidden",
      environmentId: EnvironmentId.make("fixture"),
    },
  ],
};
const layerTest = Layer.mergeAll(
  collectionStoreLayer,
  Receipts.layer.pipe(Layer.provide(Sqlite.layerMemory)),
  Replay.layerWithRegistry(
    { name: "launch-admission" },
    Registry.layerFromAdapters([
      {
        instanceId: provider,
        driver: ProviderDriverKind.make("codex"),
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
        openSession: () => Effect.die("provider execution disabled"),
      } as ProviderAdapterV2Shape,
    ]),
    {
      databaseLayer: Sqlite.layerMemory,
      runEffectWorker: false,
    },
  ),
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-launch-admission-" }).pipe(
    Layer.provideMerge(NodeServices.layer),
  ),
);

const makeFixture = Effect.gen(function* () {
  const f = yield* makeCollectionFixture;
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const sink = yield* Sink.EventSinkV2;
  const fileSystem = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig.ServerConfig;
  const imported: ThreadId[] = [];
  const threads = yield* Threads.ThreadManagementService.pipe(
    Effect.provide(
      Threads.layerWithLegacyImporter.pipe(
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
  const stat = vi.fn(fileSystem.stat);
  const copyFile = vi.fn(fileSystem.copyFile);
  const fs = FileSystem.FileSystem.of({ ...fileSystem, stat, copyFile });
  const folder = vi.fn(() => Effect.succeed(Option.none<string>()));
  const setupStarted = yield* Deferred.make<void>();
  const runSetup = vi.fn(() =>
    Deferred.succeed(setupStarted, undefined).pipe(Effect.as({ status: "no-script" as const })),
  );
  const createWorktree = vi.fn(() =>
    Effect.succeed({ worktree: { path: "/fixture/worktree", refName: "fixture", headSha: "abc" } }),
  );
  const launchContext = yield* Layer.build(
    Launch.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(ProjectService.ProjectService, f.projectService),
          Layer.succeed(Threads.ThreadManagementService, threads),
          Layer.mock(ManagedFolders.ManagedProjectFolders)({
            folderForThread: folder,
            namedProjectsRoot: "/projects",
          }),
          Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
          Layer.mock(TerminalManager.TerminalManager)({ close: () => Effect.void }),
          SetupTracker.layer,
          Layer.mock(GitWorkflow.GitWorkflowService)({
            createWorktree,
            remoteExists: () => Effect.succeed(false),
            hasCommit: () => Effect.succeed(false),
          }),
          Layer.succeed(SetupScripts.ProjectSetupScriptRunner, { runForThread: runSetup }),
          Layer.mock(TextGeneration.TextGeneration)({}),
          ServerSettings.layerTest(),
          ProviderRegistryMock.layer(),
          IdAllocator.layer,
        ),
      ),
    ),
  );
  const launches = Context.get(launchContext, Launch.ThreadLaunchService);
  const intakeLayer = Layer.mergeAll(
    Layer.succeed(Threads.ThreadManagementService, threads),
    Layer.succeed(Launch.ThreadLaunchService, launches),
    Layer.succeed(FileSystem.FileSystem, fs),
  );
  const pending = yield* Effect.gen(function* () {
    const id = ChatAttachmentId.make(createPendingAttachmentId("txt")!);
    yield* fileSystem.writeFile(`${config.attachmentsDir}/${id}.txt`, new Uint8Array([1]));
    return { type: "file" as const, id, name: "pending.txt", mimeType: "text/plain", sizeBytes: 1 };
  });
  const noEffects = () => {
    assert.equal(stat.mock.calls.length, 0);
    assert.equal(copyFile.mock.calls.length, 0);
    assert.equal(folder.mock.calls.length, 0);
    assert.equal(createWorktree.mock.calls.length, 0);
    assert.equal(runSetup.mock.calls.length, 0);
    assert.deepEqual(imported, []);
  };
  return {
    ...f,
    sql,
    receipts,
    sink,
    threads,
    launches,
    fs,
    stat,
    copyFile,
    folder,
    createWorktree,
    runSetup,
    imported,
    intakeLayer,
    pending,
    noEffects,
    setupStarted,
  };
});

const launchInput = (id: string): Launch.ThreadLaunchInput => ({
  ...provenance,
  commandId: CommandId.make(id),
  threadId: ThreadId.make(`${id}:thread`),
  projectId: projectA,
  title: "Allowed",
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  workspaceStrategy: { type: "root" },
});
const message = (id: string, attachments: ReadonlyArray<ChatAttachment> = []) =>
  ({
    ...provenance,
    type: "message.dispatch",
    commandId: CommandId.make(id),
    threadId: activeA,
    messageId: MessageId.make(id),
    text: "hello",
    attachments,
    dispatchMode: { type: "start_immediately" },
  }) satisfies Extract<OrchestrationV2ServerCommand, { type: "message.dispatch" }>;
const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
const denied = Effect.fnUntraced(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
  const error = yield* Effect.flip(effect);
  assert.isTrue(
    isAuthorizationError(error) ||
      (typeof error === "object" &&
        error !== null &&
        "cause" in error &&
        isAuthorizationError(error.cause)),
  );
});

it.effect(
  "intake and direct claims deny all supplied targets/sources before files or importer",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* Effect.gen(function* () {
        yield* f.sql`INSERT INTO orchestration_v2_projection_nodes (node_id, thread_id, root_node_id, kind, status, payload_json)
          VALUES ('own-node', ${activeA}, 'own-node', 'turn', 'running', '{}')`;
        yield* f.sql`INSERT INTO orchestration_v2_projection_runtime_requests
          (runtime_request_id, thread_id, node_id, kind, status, created_at, payload_json)
          VALUES ('own-request', ${activeA}, 'own-node', 'user_input', 'pending', '2026-10-11T00:00:00Z', '{}')`;
        yield* f.sql`INSERT INTO orchestration_v2_projection_plans
          (plan_id, thread_id, node_id, kind, status, payload_json)
          VALUES ('hidden-plan', ${activeB}, 'hidden-node', 'plan', 'completed', 'hidden malformed body')`;
        for (const patch of [
          { threadId: activeB },
          { threadId: ThreadId.make("missing") },
          { senderThreadId: activeB },
          { context: contextB },
          { attachments: [f.pending, claimedB] },
          { sourcePlanRef: { threadId: activeB, planId: PlanId.make("missing") } },
          { sourcePlanRef: { threadId: activeA, planId: PlanId.make("hidden-plan") } },
        ])
          yield* denied(
            Intake.dispatchCommand(authority, { ...message("denied", [f.pending]), ...patch }),
          );
        yield* denied(
          Intake.dispatchCommand(authority, {
            type: "queued-run.edit",
            commandId: CommandId.make("edit"),
            threadId: activeA,
            runId: RunId.make("missing"),
            text: "edit",
            context: contextB,
            attachments: [f.pending],
          }),
        );
        yield* denied(
          Intake.dispatchCommand(authority, {
            type: "runtime-request.respond",
            commandId: CommandId.make("answer"),
            threadId: activeA,
            requestId: RuntimeRequestId.make("own-request"),
            attachmentsByQuestionId: { a: [f.pending], b: [claimedB] },
          }),
        );
        for (const patch of [
          { threadId: activeB },
          { projectId: projectB },
          { senderThreadId: activeB },
          { attachments: [claimedB] },
        ])
          yield* denied(
            Intake.sendToThread(authority, {
              ...provenance,
              ...message("send", [f.pending]),
              projectId: projectA,
              mode: "auto",
              ...patch,
            }),
          );
        yield* denied(
          Claims.claimPendingAttachments(authority, {
            threadId: activeA,
            attachments: [claimedB],
            command: message("fast-path", [claimedB]),
          }),
        );
        yield* denied(
          Claims.claimPendingAttachments(authority, {
            threadId: activeA,
            attachments: [f.pending],
          }),
        );
        f.noEffects();
      }).pipe(Effect.provide(f.intakeLayer));
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "launch and pending intake admit project, actual target and initial sources before folders",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* Effect.gen(function* () {
        for (const patch of [
          { projectId: projectB },
          { projectId: ProjectId.make("missing-project") },
          { projectId: projectA, threadId: activeB },
          { projectId: projectB, threadId: activeA },
          { reuseExistingThread: true, threadId: ThreadId.make("missing") },
          { initialMessage: { text: "hidden", attachments: [f.pending], context: contextB } },
          { initialMessage: { text: "hidden", attachments: [f.pending], senderThreadId: activeB } },
          { initialMessage: { text: "hidden", attachments: [f.pending, claimedB] } },
        ]) {
          const input = { ...launchInput("denied-launch"), ...patch };
          yield* denied(f.launches.launch(authority, input));
          yield* denied(Intake.launchThread(authority, input));
        }
        f.noEffects();
      }).pipe(Effect.provide(f.intakeLayer));
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "original and initial-message receipts admit actual owners before claims or recovery",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* Effect.gen(function* () {
        for (const suffix of ["", ":initial-message"]) {
          const input = {
            ...launchInput(`receipt${suffix}`),
            initialMessage: { text: "hello", attachments: [f.pending] },
          };
          yield* f.receipts.upsert({
            commandId: CommandId.make(`${input.commandId}${suffix}`),
            threadId: activeB,
            commandType: suffix === "" ? "thread.create" : "message.dispatch",
            status: "accepted",
            error: null,
            acceptedAt: now,
            resultSequence: 0,
          });
          yield* denied(Intake.launchThread(authority, input));
          const { threadId: _omitted, ...withoutId } = input;
          yield* denied(f.launches.launch(authority, withoutId));
        }
        yield* f.receipts.upsert({
          commandId: CommandId.make("intake-receipt"),
          threadId: activeB,
          commandType: "message.dispatch",
          status: "accepted",
          error: null,
          acceptedAt: now,
          resultSequence: 0,
        });
        yield* denied(Intake.dispatchCommand(authority, message("intake-receipt", [f.pending])));
        f.noEffects();
      }).pipe(Effect.provide(f.intakeLayer));
    }).pipe(Effect.provide(layerTest)),
);

it.effect("rejected launch receipts redact old source detail before pending intake effects", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* Effect.gen(function* () {
      for (const suffix of ["", ":initial-message"]) {
        const input = {
          ...launchInput(`rejected${suffix}`),
          initialMessage: { text: "hello", attachments: [f.pending] },
        };
        yield* f.receipts.upsert({
          commandId: CommandId.make(`${input.commandId}${suffix}`),
          threadId: activeA,
          commandType: suffix === "" ? "thread.create" : "message.dispatch",
          status: "rejected",
          error: `old source ${activeB}`,
          acceptedAt: now,
          resultSequence: 0,
        });
        const error = yield* Effect.flip(Intake.launchThread(authority, input));
        assert.notInclude(JSON.stringify(error), activeB);
        assert.include(JSON.stringify(error), "Previously rejected.");
      }
      f.noEffects();
    }).pipe(Effect.provide(f.intakeLayer));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("claim rechecks current authority after pending stat, before copying", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const current = yield* Ref.make(grantA);
    const caller: MutationAuthority = {
      current: Ref.get(current).pipe(
        Effect.map((resourceGrant) => ({ resourceGrant, scopes: [AuthOrchestrationOperateScope] })),
      ),
    };
    const statReached = yield* Deferred.make<void>();
    const resume = yield* Deferred.make<void>();
    const original = f.stat.getMockImplementation()!;
    f.stat.mockImplementation((...args) =>
      original(...args).pipe(
        Effect.tap(() => Deferred.succeed(statReached, undefined)),
        Effect.tap(() => Deferred.await(resume)),
      ),
    );
    const fiber = yield* Claims.claimPendingAttachments(caller, {
      threadId: activeA,
      attachments: [f.pending],
      command: message("claim-cut", [f.pending]),
    }).pipe(Effect.provide(f.intakeLayer), Effect.forkChild);
    yield* Deferred.await(statReached);
    yield* Ref.set(current, grantNone);
    yield* Deferred.succeed(resume, undefined);
    yield* denied(Fiber.join(fiber));
    assert.equal(f.copyFile.mock.calls.length, 0);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "launch rechecks cuts after project resolution before reuse materialization or folders",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const current = yield* Ref.make(grantA);
      const caller: MutationAuthority = {
        current: Ref.get(current).pipe(
          Effect.map((resourceGrant) => ({
            resourceGrant,
            scopes: [AuthOrchestrationOperateScope],
          })),
        ),
      };
      const getProject = f.projectService.getById;
      const spy = vi
        .spyOn(f.projectService, "getById")
        .mockImplementation((id) =>
          getProject(id).pipe(Effect.tap(() => Ref.set(current, grantNone))),
        );
      try {
        yield* denied(
          f.launches.launch(caller, {
            ...launchInput("launch-cut"),
            threadId: activeA,
            reuseExistingThread: true,
          }),
        );
        f.noEffects();
      } finally {
        spy.mockRestore();
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect("operate-only launch/reuse/replay and trusted bootstrap remain usable", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const first = yield* f.launches.launch(authority, launchInput("permitted"));
    yield* Deferred.await(f.setupStarted);
    const { threadId: _omitted, ...withoutId } = launchInput("permitted");
    const replayed = yield* f.launches.launch(authority, withoutId);
    assert.equal(replayed.threadId, first.threadId);
    assert.isTrue(replayed.resumed);
    yield* f.launches.launch(authority, {
      ...launchInput("reuse"),
      threadId: activeA,
      reuseExistingThread: true,
    });
    yield* f.launches.launch(trustedMutationAuthority, {
      ...launchInput("trusted"),
      projectId: projectB,
    });
    const viewOnly: MutationAuthority = {
      current: Effect.succeed({ scopes: [AuthOrchestrationReadScope], resourceGrant: grantA }),
    };
    yield* denied(f.launches.launch(viewOnly, launchInput("view-only")));
    assert.isAtLeast(f.folder.mock.calls.length, 3);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "operate-only new launch claims pending uploads before persistence without adding read scope",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const result = yield* Intake.launchThread(authority, {
        ...launchInput("pending-allowed"),
        initialMessage: { text: "hello", attachments: [f.pending] },
      }).pipe(Effect.provide(f.intakeLayer));
      yield* Deferred.await(f.setupStarted);
      assert.equal(f.copyFile.mock.calls.length, 1);
      assert.equal(result.projection.messages.length, 1);
      assert.notEqual(result.projection.messages[0]!.attachments[0]!.id, f.pending.id);
    }).pipe(Effect.provide(layerTest)),
);

const failRun = Effect.fnUntraced(function* (
  f: Effect.Success<typeof makeFixture>,
  threadId: ThreadId,
  id: string,
  workspaceStrategy: Launch.ThreadLaunchWorkspaceStrategy = { type: "root" },
) {
  const dispatched = yield* f.threads.dispatch(trustedMutationAuthority, {
    ...message(id),
    threadId,
    dispatchMode: { type: "defer_start", workspaceStrategy },
  });
  const event = dispatched.storedEvents.find(({ event }) => event.type === "run.created")!.event;
  assert.equal(event.type, "run.created");
  if (event.type !== "run.created") return yield* Effect.die("run not created");
  const runId = event.payload.id;
  yield* f.threads.dispatch(trustedMutationAuthority, {
    type: "prepared-run.fail",
    commandId: CommandId.make(`${id}:fail`),
    threadId,
    runId,
    failure: {
      class: "validation_error",
      message: "preparation failed",
      code: null,
      retryable: false,
    },
  });
  f.imported.length = 0;
  return runId;
});

it.effect(
  "retry admits actual run and stored workspace before projection, commit or scheduling",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const hiddenRun = yield* failRun(f, activeB, "hidden-run");
      const ownRun = yield* failRun(f, activeA, "own-run", {
        type: "existing_worktree",
        worktreePath: "/unregistered/hidden",
      });
      for (const input of [
        { threadId: activeB, runId: hiddenRun },
        { threadId: activeA, runId: hiddenRun },
        { threadId: activeA, runId: RunId.make("missing") },
        { threadId: activeA, runId: ownRun },
      ])
        yield* denied(
          f.launches.retryPreparation(authority, {
            ...input,
            commandId: CommandId.make("denied-retry"),
          }),
        );
      const ownRootRun = yield* failRun(f, activeA, "own-root-run");
      yield* f.receipts.upsert({
        commandId: CommandId.make("hidden-retry-receipt"),
        threadId: activeB,
        commandType: "prepared-run.retry",
        status: "accepted",
        error: null,
        acceptedAt: now,
        resultSequence: 0,
      });
      yield* denied(
        f.launches.retryPreparation(authority, {
          commandId: CommandId.make("hidden-retry-receipt"),
          threadId: activeA,
          runId: ownRootRun,
        }),
      );
      f.noEffects();
      assert.isTrue(
        Option.isNone(yield* f.receipts.getByCommandId(CommandId.make("denied-retry"))),
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "retry cut after permitted projection denies scheduling and leaves lifecycle to durable-origin slice",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const runId = yield* failRun(f, activeA, "retry-cut-run");
      const current = yield* Ref.make(grantA);
      const caller: MutationAuthority = {
        current: Ref.get(current).pipe(
          Effect.map((resourceGrant) => ({
            resourceGrant,
            scopes: [AuthOrchestrationOperateScope],
          })),
        ),
      };
      const getProjection = f.threads.getThreadProjection;
      const spy = vi
        .spyOn(f.threads, "getThreadProjection")
        .mockImplementation((...args) =>
          getProjection(...args).pipe(Effect.tap(() => Ref.set(current, grantNone))),
        );
      try {
        yield* denied(
          f.launches.retryPreparation(caller, {
            threadId: activeA,
            runId,
            commandId: CommandId.make("retry-cut"),
          }),
        );
        assert.equal(f.runSetup.mock.calls.length, 0);
        assert.equal(f.createWorktree.mock.calls.length, 0);
      } finally {
        spy.mockRestore();
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect("operate-only retry schedules once and replay stays idempotent", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const runId = yield* failRun(f, activeA, "retry-allowed-run");
    const retry = { threadId: activeA, runId, commandId: CommandId.make("retry-allowed") };
    yield* f.launches.retryPreparation(authority, retry);
    yield* Deferred.await(f.setupStarted);
    yield* f.launches.retryPreparation(authority, retry);
    assert.equal(f.runSetup.mock.calls.length, 1);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("historical receipt sources stay guarded after current projection removes them", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* Effect.gen(function* () {
      for (const suffix of ["", ":initial-message"]) {
        for (const [index, patch] of [
          { context: contextB },
          { attachments: [claimedB] },
          { senderThreadId: activeB },
        ].entries()) {
          const input = {
            ...launchInput(`history-${suffix}-${index}`),
            threadId: activeA,
            initialMessage: { text: "hello", attachments: [f.pending] },
          };
          const commandId = CommandId.make(`${input.commandId}${suffix}`);
          yield* f.sink.commitCommand(trustedMutationAuthority, {
            commandId,
            threadId: activeA,
            commandType: suffix === "" ? "thread.create" : "message.dispatch",
            acceptedAt: now,
            effects: [],
            events: [
              {
                id: EventId.make(`${commandId}:event`),
                type: "message.updated",
                threadId: activeA,
                occurredAt: now,
                payload: {
                  ...provenance,
                  id: MessageId.make(`${commandId}:message`),
                  threadId: activeA,
                  runId: null,
                  nodeId: null,
                  role: "user",
                  text: "old hidden body",
                  attachments: [],
                  streaming: false,
                  createdAt: now,
                  updatedAt: now,
                  ...patch,
                },
              },
            ],
          });
          yield* f.sql`DELETE FROM orchestration_v2_projection_messages WHERE thread_id = ${activeA}`;
          yield* denied(Intake.launchThread(authority, input));
        }
      }
      f.noEffects();
    }).pipe(Effect.provide(f.intakeLayer));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("operate-only intake send and queued edit keep claim cleanup and replay usable", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    yield* Effect.gen(function* () {
      const sent = yield* Intake.sendToThread(authority, {
        ...provenance,
        projectId: projectA,
        threadId: activeA,
        commandId: CommandId.make("allowed-send"),
        messageId: MessageId.make("allowed-send"),
        text: "hello",
        attachments: [f.pending],
        mode: "queue",
      });
      const replayed = yield* Intake.sendToThread(authority, {
        ...provenance,
        projectId: projectA,
        threadId: activeA,
        commandId: CommandId.make("allowed-send"),
        messageId: MessageId.make("allowed-send"),
        text: "hello",
        attachments: [f.pending],
        mode: "queue",
      });
      assert.equal(replayed.message.attachments[0]!.id, sent.message.attachments[0]!.id);
      const claimed = yield* Claims.claimPendingAttachments(authority, {
        threadId: activeA,
        attachments: sent.message.attachments,
        command: message("allowed-fast-path", sent.message.attachments),
      });
      assert.deepEqual(claimed.claimedPaths, []);
      assert.equal(f.copyFile.mock.calls.length, 2);
      const queued = yield* Intake.sendToThread(authority, {
        ...provenance,
        projectId: projectA,
        threadId: activeA,
        commandId: CommandId.make("allowed-queued"),
        messageId: MessageId.make("allowed-queued"),
        text: "queued",
        attachments: [],
        mode: "queue",
      });
      yield* Intake.dispatchCommand(authority, {
        type: "queued-run.edit",
        commandId: CommandId.make("allowed-edit"),
        threadId: activeA,
        runId: queued.run.id,
        text: "edited",
        attachments: [f.pending],
      });
      assert.equal(f.copyFile.mock.calls.length, 3);
    }).pipe(Effect.provide(f.intakeLayer));
  }).pipe(Effect.provide(layerTest)),
);

it.effect("launch cut after preparing-run read denies scheduling before background work", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const current = yield* Ref.make(grantA);
    const caller: MutationAuthority = {
      current: Ref.get(current).pipe(
        Effect.map((resourceGrant) => ({ resourceGrant, scopes: [AuthOrchestrationOperateScope] })),
      ),
    };
    const getRecords = f.threads.getThreadRecords;
    const spy = vi
      .spyOn(f.threads, "getThreadRecords")
      .mockImplementation((...args) =>
        getRecords(...args).pipe(
          Effect.tap(() =>
            args[3]?.runIds === undefined ? Effect.void : Ref.set(current, grantNone),
          ),
        ),
      );
    try {
      yield* denied(
        f.launches.launch(caller, {
          ...launchInput("schedule-cut"),
          initialMessage: { text: "hello", attachments: [] },
        }),
      );
      assert.equal(f.runSetup.mock.calls.length, 0);
      assert.equal(f.createWorktree.mock.calls.length, 0);
    } finally {
      spy.mockRestore();
    }
  }).pipe(Effect.provide(layerTest)),
);

it.effect("existing-workspace launch uses registered roots and preserves operate-only access", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const allowedRoot = yield* f.fs.makeTempDirectoryScoped({ prefix: "t3-admission-a-" });
    const hiddenRoot = yield* f.fs.makeTempDirectoryScoped({ prefix: "t3-admission-b-" });
    yield* f.sql`UPDATE projection_projects SET workspace_root = ${allowedRoot} WHERE project_id = ${projectA}`;
    yield* f.sql`UPDATE projection_projects SET workspace_root = ${hiddenRoot} WHERE project_id = ${projectB}`;
    yield* denied(
      f.launches.launch(authority, {
        ...launchInput("hidden-workspace"),
        workspaceStrategy: { type: "existing_worktree", worktreePath: hiddenRoot },
      }),
    );
    f.noEffects();
    yield* f.launches.launch(authority, {
      ...launchInput("allowed-workspace"),
      workspaceStrategy: { type: "existing_worktree", worktreePath: allowedRoot },
    });
    yield* Deferred.await(f.setupStarted);
    assert.equal(f.runSetup.mock.calls.length, 1);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("claim and folder I/O release the SQL transaction before external work", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const outsideTransaction = Effect.gen(function* () {
      assert.isTrue(Option.isNone(yield* Effect.serviceOption(f.sql.transactionService)));
      yield* f.sql`SELECT 1`.pipe(Effect.orDie);
    });
    const stat = f.stat.getMockImplementation()!;
    const copy = f.copyFile.getMockImplementation()!;
    f.stat.mockImplementation((...args) => outsideTransaction.pipe(Effect.andThen(stat(...args))));
    f.copyFile.mockImplementation((...args) =>
      outsideTransaction.pipe(Effect.andThen(copy(...args))),
    );
    f.folder.mockImplementation(() => outsideTransaction.pipe(Effect.as(Option.none<string>())));

    yield* Claims.claimPendingAttachments(authority, {
      threadId: activeA,
      attachments: [f.pending],
      command: message("claim-without-sql-lock", [f.pending]),
    }).pipe(Effect.provide(f.intakeLayer));
    yield* f.launches.launch(authority, launchInput("folder-without-sql-lock"));
    yield* f.launches.launch(
      trustedMutationAuthority,
      launchInput("trusted-folder-without-sql-lock"),
    );
    assert.equal(f.stat.mock.calls.length, 1);
    assert.equal(f.copyFile.mock.calls.length, 1);
    assert.equal(f.folder.mock.calls.length, 2);
  }).pipe(Effect.provide(layerTest)),
);
