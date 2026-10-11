import { assert, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthAllResourceGrant,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  ChatAttachmentId,
  CheckpointId,
  CheckpointScopeId,
  CommandId,
  ComposerContextId,
  EnvironmentId,
  EnvironmentAuthorizationError,
  EventId,
  MessageId,
  NodeId,
  PlanId,
  ProviderDriverKind,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import { trustedMutationAuthority, type MutationAuthority } from "../auth/CommandAuthorization.ts";
import { toSafeThreadAttachmentSegment } from "../attachmentStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Legacy from "./legacy/LegacyV1ThreadImporter.ts";
import * as Sink from "./EventSink.ts";
import * as Receipts from "./CommandReceiptStore.ts";
import * as Replay from "./testkit/ProviderReplayHarness.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
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
const commandId = CommandId.make("mutation-fixture");
const messageId = MessageId.make("missing");
const runId = RunId.make("missing");
const nodeId = NodeId.make("missing");
const requestId = RuntimeRequestId.make("missing");
const pr = { host: "github.com", repository: "example/repo", number: 1 };
const prSnapshot = {
  state: "open",
  title: "PR",
  headBranch: "head",
  baseBranch: "base",
  isDraft: false,
  updatedAt: null,
  syncedAt: "2026-10-11T00:00:00Z",
  closedAt: null,
  mergedAt: null,
} as const;
const base = { commandId, threadId: activeB };

// Adding a server command requires a real denial case, including controller-only variants.
const hiddenCommands = {
  "thread.create": {
    type: "thread.create",
    ...base,
    ...provenance,
    projectId: projectB,
    title: "hidden",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
  },
  "thread.archive": { type: "thread.archive", ...base },
  "thread.unarchive": { type: "thread.unarchive", ...base },
  "thread.delete": { type: "thread.delete", ...base },
  "thread.settle": { type: "thread.settle", ...base },
  "thread.auto-settle": { type: "thread.auto-settle", ...base, snapshotAt: now },
  "thread.unsettle": { type: "thread.unsettle", ...base, reason: "user" },
  "thread.snooze": { type: "thread.snooze", ...base, snoozedUntil: "2026-10-12T00:00:00Z" },
  "thread.unsnooze": { type: "thread.unsnooze", ...base, reason: "user" },
  "thread.auto-settle.set": { type: "thread.auto-settle.set", ...base, enabled: true },
  "thread.pin": { type: "thread.pin", ...base },
  "thread.unpin": { type: "thread.unpin", ...base },
  "thread.pin.reorder": { type: "thread.pin.reorder", ...base, orderKey: "a" },
  "thread.active.reorder": { type: "thread.active.reorder", ...base, orderKey: "a" },
  "thread.visit": { type: "thread.visit", ...base, visitedAt: "2026-10-11T00:00:00Z" },
  "thread.mark-unread": { type: "thread.mark-unread", ...base },
  "thread.metadata.update": { type: "thread.metadata.update", ...base },
  "thread.pull-request.link": {
    type: "thread.pull-request.link",
    ...base,
    ...pr,
    url: "https://github.com/example/repo/pull/1",
    source: "manual",
  },
  "thread.pull-request.unlink": { type: "thread.pull-request.unlink", ...base, ...pr },
  "thread.pull-request-link.sync": {
    type: "thread.pull-request-link.sync",
    ...base,
    ...pr,
    snapshot: prSnapshot,
    stack: null,
  },
  "thread.pull-request.watch": {
    type: "thread.pull-request.watch",
    ...base,
    ...pr,
    watching: true,
  },
  "thread.pull-request.sync": {
    type: "thread.pull-request.sync",
    ...base,
    projectId: projectB,
    snapshotSequence: 0,
    expected: {
      workspaceRoot: "/hidden",
      branch: null,
      worktreePath: null,
      linkedPullRequest: null,
      branchPullRequest: null,
    },
    branchPullRequest: null,
  },
  "thread.title.regeneration.complete": {
    type: "thread.title.regeneration.complete",
    ...base,
    requestId: commandId,
  },
  "thread.runtime-mode.set": {
    type: "thread.runtime-mode.set",
    ...base,
    runtimeMode: "full-access",
  },
  "thread.interaction-mode.set": {
    type: "thread.interaction-mode.set",
    ...base,
    interactionMode: "default",
  },
  "thread.model-selection.set": { type: "thread.model-selection.set", ...base, modelSelection },
  "provider-session.detach": {
    type: "provider-session.detach",
    ...base,
    providerSessionId: ProviderSessionId.make("missing"),
  },
  "message.dispatch": {
    type: "message.dispatch",
    ...base,
    ...provenance,
    messageId,
    text: "hello",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
  },
  "prepared-run.release": { type: "prepared-run.release", ...base, runId },
  "notification.delivery.accept": { type: "notification.delivery.accept", ...base, messageId },
  "prepared-run.progress": { type: "prepared-run.progress", ...base, runId, phase: "setup" },
  "prepared-run.fail": {
    type: "prepared-run.fail",
    ...base,
    runId,
    failure: { class: "unknown", message: "failed", code: null, retryable: false },
  },
  "prepared-run.retry": { type: "prepared-run.retry", ...base, runId },
  "run.interrupt": { type: "run.interrupt", ...base, runId },
  "queued-message.promote-to-steer": {
    type: "queued-message.promote-to-steer",
    ...base,
    queuedRunId: runId,
    targetRunId: runId,
  },
  "queue.resume": { type: "queue.resume", ...base },
  "queued-run.reorder": { type: "queued-run.reorder", ...base, runId, beforeRunId: null },
  "queued-run.cancel": { type: "queued-run.cancel", ...base, runId },
  "queued-run.edit": { type: "queued-run.edit", ...base, runId, text: "hello" },
  "runtime-request.respond": { type: "runtime-request.respond", ...base, requestId },
  "thread.user-input.dismiss": { type: "thread.user-input.dismiss", ...base, requestId },
  "checkpoint.rollback": {
    type: "checkpoint.rollback",
    ...base,
    scopeId: CheckpointScopeId.make("missing"),
    checkpointId: CheckpointId.make("missing"),
  },
  "thread.fork": {
    type: "thread.fork",
    commandId,
    ...provenance,
    sourceThreadId: activeB,
    targetThreadId: activeA,
    sourcePoint: { type: "latest_stable" },
  },
  "thread.merge_back": {
    type: "thread.merge_back",
    commandId,
    ...provenance,
    sourceThreadId: activeB,
    targetThreadId: activeA,
    sourcePoint: { type: "latest_stable" },
  },
  "delegated_task.request": {
    type: "delegated_task.request",
    commandId,
    ...provenance,
    parentThreadId: activeB,
    parentRunId: runId,
    parentNodeId: nodeId,
    task: "task",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
  },
  "delegated_task.wake-policy": {
    type: "delegated_task.wake-policy",
    commandId,
    parentThreadId: activeB,
    taskId: nodeId,
    completionWake: "always",
  },
  "delegated_task.completion-delivery.acknowledge": {
    type: "delegated_task.completion-delivery.acknowledge",
    commandId,
    parentThreadId: activeB,
    taskId: nodeId,
    observedByRunId: null,
  },
  "delegated_task.completion-delivery.dispose": {
    type: "delegated_task.completion-delivery.dispose",
    commandId,
    parentThreadId: activeB,
    taskId: nodeId,
  },
  "thread.created.record": {
    type: "thread.created.record",
    commandId,
    parentThreadId: activeB,
    parentRunId: runId,
    parentNodeId: nodeId,
    targetThreadId: activeA,
    targetRunId: null,
  },
  "provider.switch": { type: "provider.switch", ...base, modelSelection },
  "thread.pull-request-watch.sync": {
    type: "thread.pull-request-watch.sync",
    ...base,
    ...pr,
    startedAt: "2026-10-11T00:00:00Z",
    watch: null,
  },
  "checkpoint.rollback.fail": {
    type: "checkpoint.rollback.fail",
    ...base,
    requestId: commandId,
    message: "failed",
  },
  "thread.background-work.settle": {
    type: "thread.background-work.settle",
    ...base,
    providerThreadId: ProviderThreadId.make("missing"),
    providerTurnId: ProviderTurnId.make("missing"),
  },
  "thread.stop": { type: "thread.stop", ...base },
  "secret_request.record": {
    type: "secret_request.record",
    ...base,
    runId,
    nodeId,
    turnItemId: TurnItemId.make("missing"),
    label: "secret",
    reason: "needed",
    secretStatus: "pending",
  },
} satisfies {
  [K in OrchestrationV2ServerCommand["type"]]: Extract<OrchestrationV2ServerCommand, { type: K }>;
};

const layerTest = Layer.mergeAll(
  collectionStoreLayer,
  Receipts.layer.pipe(Layer.provide(Sqlite.layerMemory)),
  Replay.layerWithRegistry({ name: "mutation-admission" }, Registry.layerFromAdapters([]), {
    databaseLayer: Sqlite.layerMemory,
    runEffectWorker: false,
  }),
);
const makeFixture = Effect.gen(function* () {
  const seeded = yield* makeCollectionFixture;
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
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* Sink.EventSinkV2;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const sql = yield* SqlClient.SqlClient;
  return { ...seeded, threads, orchestrator, sink, receipts, sql, imported };
});

const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
const isDispatchError = Schema.is(Orchestrator.OrchestratorDispatchError);
const isProjectionError = Schema.is(Orchestrator.OrchestratorProjectionError);
const isSinkError = Schema.is(Sink.EventSinkWriteError);
const hiddenSourceRejection = Effect.fnUntraced(function* () {
  const f = yield* makeFixture;
  const original = {
    ...hiddenCommands["thread.merge_back"],
    commandId: CommandId.make("rejected-hidden-source"),
  };
  yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'malformed B' WHERE thread_id = ${activeB}`;
  yield* Effect.flip(f.orchestrator.dispatch(trustedMutationAuthority, original));
  const rows = yield* f.sql<{
    readonly error: string;
  }>`SELECT error FROM orchestration_command_receipts WHERE command_id = ${original.commandId}`;
  assert.lengthOf(rows, 1);
  const diagnostic = rows[0]!.error;
  assert.include(diagnostic, activeB);
  return { ...f, original, diagnostic };
});
const denied = Effect.fnUntraced(function* <A, E>(action: Effect.Effect<A, E>) {
  let error: unknown = yield* Effect.flip(action.pipe(Effect.asVoid));
  for (let i = 0; i < 3; i++) {
    if (isAuthorizationError(error)) {
      assert.equal(error.requiredScope, AuthOrchestrationOperateScope);
      assert.notInclude(error.message, "secret");
      return;
    }
    if (isDispatchError(error) || isProjectionError(error) || isSinkError(error))
      error = error.cause;
  }
  assert.fail("Expected a resource authorization denial");
});

it.effect(
  "historical rejected A receipts hide original B diagnostics from restricted callers",
  () =>
    Effect.gen(function* () {
      const f = yield* hiddenSourceRejection();
      const replay = rename(f.original.commandId);
      for (const result of [
        yield* Effect.flip(f.orchestrator.dispatch(authority, replay)),
        yield* Effect.flip(f.threads.dispatch(authority, replay)),
      ]) {
        assert.notInclude(result.message, activeB);
        assert.notInclude(JSON.stringify(result), activeB);
      }
      const unrestricted: MutationAuthority = {
        current: Effect.succeed({
          scopes: [AuthOrchestrationOperateScope],
          resourceGrant: AuthAllResourceGrant,
        }),
      };
      for (const caller of [trustedMutationAuthority, unrestricted] as const) {
        const error = yield* Effect.flip(f.orchestrator.dispatch(caller, replay));
        assert.equal(error._tag, "OrchestratorCommandPreviouslyRejectedError");
        assert.include(error.message, f.diagnostic);
      }
      const persisted = yield* f.sql<{
        readonly error: string;
      }>`SELECT error FROM orchestration_command_receipts WHERE command_id = ${replay.commandId}`;
      assert.equal(persisted[0]!.error, f.diagnostic);
      yield* f.threads.dispatch(authority, rename("permitted-after-rejected-replay"));
      assert.equal((yield* f.projections.getThread(activeA)).title, "Allowed rename");
      const ownCommand = rename("permitted-own-rejection");
      const own = yield* f.sink.commitRejectedCommand(
        { authority, command: ownCommand },
        {
          commandId: ownCommand.commandId,
          threadId: activeA,
          commandType: ownCommand.type,
          rejectedAt: now,
          error: "Own A rejection",
        },
      );
      assert.equal(own.status, "rejected");
      assert.equal(own.error, "Previously rejected.");
    }).pipe(Effect.provide(layerTest)),
);

it.effect("competing rejected receipts redact opaque detail at both shared EventSink returns", () =>
  Effect.gen(function* () {
    const f = yield* hiddenSourceRejection();
    const thread = yield* f.projections.getThread(activeA);
    for (const path of ["orchestrator", "commit", "reject"] as const) {
      const command = rename(`competing-rejection-detail-${path}`);
      const insert = vi.spyOn(f.receipts, "insertIfAbsent").mockImplementation(() =>
        f.receipts
          .upsert({
            commandId: command.commandId,
            threadId: activeA,
            commandType: f.original.type,
            acceptedAt: now,
            resultSequence: 0,
            status: "rejected",
            error: f.diagnostic,
          })
          .pipe(Effect.as(false), Effect.orDie),
      );
      try {
        if (path === "orchestrator") {
          const error = yield* Effect.flip(f.orchestrator.dispatch(authority, command));
          assert.equal(error._tag, "OrchestratorCommandPreviouslyRejectedError");
          assert.notInclude(error.message, activeB);
          assert.notInclude(JSON.stringify(error), activeB);
        } else {
          const admission = { authority, command };
          const input = {
            commandId: command.commandId,
            threadId: activeA,
            commandType: command.type,
          };
          const receipt =
            path === "commit"
              ? (yield* f.sink.commitCommand(admission, {
                  ...input,
                  acceptedAt: now,
                  effects: [],
                  events: [
                    {
                      id: EventId.make("rejected-competing-metadata"),
                      type: "thread.metadata-updated",
                      threadId: activeA,
                      occurredAt: now,
                      payload: { ...thread, title: "Must not commit" },
                    },
                  ],
                })).receipt
              : yield* f.sink.commitRejectedCommand(admission, {
                  ...input,
                  rejectedAt: now,
                  error: "Own rejected command",
                });
          assert.equal(receipt.status, "rejected");
          assert.equal(receipt.error, "Previously rejected.");
          assert.notInclude(JSON.stringify(receipt), activeB);
        }
      } finally {
        insert.mockRestore();
      }
      assert.equal((yield* f.projections.getThread(activeA)).title, thread.title);
      const rows = yield* f.sql<{
        readonly error: string;
      }>`SELECT error FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`;
      assert.equal(rows[0]!.error, f.diagnostic);
    }
  }).pipe(Effect.provide(layerTest)),
);
const rename = (
  id: string,
  threadId = activeA,
): Extract<OrchestrationV2ServerCommand, { type: "thread.metadata.update" }> => ({
  type: "thread.metadata.update",
  commandId: CommandId.make(id),
  threadId,
  title: "Allowed rename",
});

const claimedB = {
  id: ChatAttachmentId.make(
    `${toSafeThreadAttachmentSegment(activeB)}-00000000-0000-4000-8000-000000000001`,
  ),
  type: "file",
  name: "hidden.txt",
  mimeType: "text/plain",
  sizeBytes: 1,
} as const;
const contextB = {
  version: 1,
  records: [
    {
      version: 1,
      kind: "thread",
      contextId: ComposerContextId.make("hidden-context"),
      environmentId: EnvironmentId.make("same-environment"),
      threadId: activeB,
      title: "Hidden title",
      label: "Hidden title",
    },
  ],
} as const;

it.effect(
  "supplied source/target, actual record owners, contexts, plans, PRs and claimed attachments deny before importer",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const sql = f.sql;
      yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
    VALUES ('own-run', ${activeA}, 1, 'codex', 'completed', '2026-10-11', '{}'),
      ('hidden-run', ${activeB}, 1, 'codex', 'completed', '2026-10-11', 'hidden malformed JSON')`;
      yield* sql`INSERT INTO orchestration_v2_projection_nodes (node_id, thread_id, root_node_id, kind, status, payload_json)
    VALUES ('hidden-node', ${activeB}, 'hidden-node', 'turn', 'completed', 'hidden malformed JSON')`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, provider, status, updated_at, payload_json)
    VALUES ('hidden-provider', ${activeB}, 'codex', 'idle', '2026-10-11', 'hidden malformed JSON')`;
      yield* sql`INSERT INTO orchestration_v2_projection_plans (plan_id, thread_id, node_id, kind, status, payload_json)
    VALUES ('hidden-plan', ${activeB}, 'hidden-node', 'implementation', 'completed', 'hidden malformed JSON')`;
      yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id, thread_id, role, streaming, created_at, updated_at, payload_json)
    VALUES ('hidden-message', ${activeB}, 'user', 0, '2026-10-11', '2026-10-11', 'hidden malformed JSON')`;
      const message = { ...hiddenCommands["message.dispatch"], threadId: activeA };
      const cases: ReadonlyArray<OrchestrationV2ServerCommand> = [
        { ...hiddenCommands["thread.fork"], sourceThreadId: activeA, targetThreadId: activeB },
        {
          ...hiddenCommands["thread.merge_back"],
          sourceThreadId: activeA,
          targetThreadId: activeB,
        },
        {
          ...hiddenCommands["thread.fork"],
          sourceThreadId: activeA,
          targetThreadId: ThreadId.make("new-target"),
          sourcePoint: { type: "run", runId: RunId.make("hidden-run") },
        },
        {
          ...hiddenCommands["thread.created.record"],
          parentThreadId: activeA,
          targetThreadId: activeA,
          parentRunId: RunId.make("own-run"),
          parentNodeId: NodeId.make("hidden-node"),
        },
        {
          ...hiddenCommands["thread.created.record"],
          parentThreadId: activeA,
          targetThreadId: activeA,
          parentRunId: RunId.make("own-run"),
          parentNodeId: NodeId.make("missing-node"),
        },
        {
          ...hiddenCommands["thread.background-work.settle"],
          threadId: activeA,
          providerThreadId: ProviderThreadId.make("hidden-provider"),
        },
        {
          ...hiddenCommands["thread.background-work.settle"],
          threadId: activeA,
          providerThreadId: ProviderThreadId.make("missing-provider"),
        },
        { ...message, senderThreadId: activeB },
        { ...message, context: contextB },
        { ...message, sourcePlanRef: { threadId: activeA, planId: PlanId.make("hidden-plan") } },
        { ...message, sourcePlanRef: { threadId: activeA, planId: PlanId.make("missing-plan") } },
        { ...message, attachments: [claimedB] },
        { ...message, messageId: MessageId.make("hidden-message") },
        {
          ...rename("hidden-pr"),
          linkedPullRequest: {
            projectId: projectB,
            repository: "example/repo",
            number: 1,
            url: "https://github.com/example/repo/pull/1",
          },
        },
      ];
      for (const command of cases) yield* denied(f.threads.dispatch(authority, command));
      assert.deepEqual(f.imported, []);
      assert.lengthOf(yield* sql`SELECT command_id FROM orchestration_command_receipts`, 0);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "stored legacy context, claimed attachments, copied plan and linked PR sources deny before importer",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const sql = f.sql;
      for (const context of [{ records: contextB.records }, { records: [] }]) {
        yield* sql`INSERT INTO projection_thread_messages (message_id, thread_id, role, text, is_streaming, created_at, updated_at, context_json, attachments_json)
      VALUES ('legacy-source', ${activeA}, 'user', 'copied payload', 0, '2026-10-11', '2026-10-11', ${JSON.stringify(context)}, ${JSON.stringify([claimedB])})`;
        yield* denied(f.threads.dispatch(authority, rename("stored-legacy")));
        yield* sql`DELETE FROM projection_thread_messages WHERE message_id = 'legacy-source'`;
      }
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.linkedPullRequest', json(${JSON.stringify({ projectId: projectB, repository: "example/repo", number: 1, url: "https://github.com/example/repo/pull/1" })})) WHERE thread_id = ${activeA}`;
      yield* denied(f.threads.dispatch(authority, rename("stored-pr")));
      yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_remove(payload_json, '$.linkedPullRequest') WHERE thread_id = ${activeA}`;
      yield* sql`INSERT INTO orchestration_v2_projection_plans (plan_id, thread_id, node_id, kind, status, payload_json)
    VALUES ('hidden-plan', ${activeB}, 'hidden-node', 'implementation', 'completed', 'hidden malformed JSON')`;
      yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
    VALUES ('copied-plan', ${activeA}, 1, 'codex', 'completed', '2026-10-11', ${JSON.stringify({ sourcePlanRef: { threadId: activeA, planId: "hidden-plan" } })})`;
      yield* denied(f.threads.dispatch(authority, rename("stored-plan")));
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "a guessed hidden commandId cannot replay B events or rejection payload through submitted A",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const cmd = rename("hidden-receipt", activeB);
      yield* f.orchestrator.dispatch(trustedMutationAuthority, cmd);
      yield* f.sql`UPDATE orchestration_events SET payload_json = 'hidden malformed event' WHERE command_id = ${cmd.commandId}`;
      yield* denied(f.threads.dispatch(authority, { ...cmd, threadId: activeA }));
      yield* f.receipts.upsert({
        commandId: CommandId.make("hidden-rejected"),
        threadId: activeB,
        commandType: cmd.type,
        acceptedAt: now,
        resultSequence: 0,
        status: "rejected",
        error: "Hidden rejection payload",
      });
      yield* denied(f.threads.dispatch(authority, rename("hidden-rejected")));
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "receipt admission checks original PR, node, context and attachment participants after current references disappear",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* f.sql`INSERT INTO orchestration_v2_projection_nodes (node_id, thread_id, root_node_id, kind, status, payload_json)
    VALUES ('historical-hidden-node', ${activeB}, 'historical-hidden-node', 'turn', 'completed', 'hidden malformed JSON')`;
      const thread = yield* f.projections.getThread(activeA);
      const cmd = rename("receipt-pr-source");
      yield* f.sink.commitCommand(trustedMutationAuthority, {
        commandId: cmd.commandId,
        threadId: activeA,
        commandType: cmd.type,
        acceptedAt: now,
        effects: [],
        events: [
          {
            id: EventId.make("receipt-pr-source"),
            type: "thread.metadata-updated",
            threadId: activeA,
            occurredAt: now,
            payload: {
              ...thread,
              linkedPullRequest: {
                projectId: projectB,
                repository: "example/repo",
                number: 1,
                url: "https://github.com/example/repo/pull/1",
              },
            },
          },
        ],
      });
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_remove(payload_json, '$.linkedPullRequest') WHERE thread_id = ${activeA}`;
      yield* denied(f.threads.dispatch(authority, cmd));
      for (const [index, patch] of [
        { nodeId: NodeId.make("historical-hidden-node") },
        { context: contextB },
        { attachments: [claimedB] },
      ].entries()) {
        const replay = rename(`receipt-message-source-${index}`);
        yield* f.sink.commitCommand(trustedMutationAuthority, {
          commandId: replay.commandId,
          threadId: activeA,
          commandType: replay.type,
          acceptedAt: now,
          effects: [],
          events: [
            {
              id: EventId.make(`receipt-message-source-${index}`),
              type: "message.updated",
              threadId: activeA,
              occurredAt: now,
              payload: {
                ...provenance,
                id: MessageId.make(`receipt-message-source-${index}`),
                threadId: activeA,
                runId: null,
                nodeId: null,
                role: "user",
                text: "Hidden copied payload",
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
        yield* denied(f.threads.dispatch(authority, replay));
      }
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "EventSink accepted and rejected competing receipts recheck original ownership before decoding payload",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const thread = yield* f.projections.getThread(activeA);
      const hidden = yield* f.projections.getThread(activeB);
      for (const status of ["accepted", "rejected"] as const) {
        const cmd = rename(`competing-hidden-${status}`);
        const insert = vi.spyOn(f.receipts, "insertIfAbsent").mockImplementation(() =>
          Effect.gen(function* () {
            yield* f.receipts.upsert({
              commandId: cmd.commandId,
              threadId: activeB,
              commandType: cmd.type,
              acceptedAt: now,
              resultSequence: 1,
              status,
              error: status === "rejected" ? "Hidden rejection payload" : null,
            });
            if (status === "accepted") {
              yield* f.events.appendAgentEvents({
                commandId: cmd.commandId,
                events: [
                  {
                    id: EventId.make(`competing-hidden-${status}`),
                    type: "thread.metadata-updated",
                    threadId: activeB,
                    occurredAt: now,
                    payload: hidden,
                  },
                ],
              });
              yield* f.sql`UPDATE orchestration_events SET payload_json = 'hidden malformed event' WHERE command_id = ${cmd.commandId}`;
            }
            return false;
          }).pipe(Effect.orDie),
        );
        const admission = { authority, command: cmd };
        if (status === "accepted")
          yield* denied(
            f.sink.commitCommand(admission, {
              commandId: cmd.commandId,
              threadId: activeA,
              commandType: cmd.type,
              acceptedAt: now,
              effects: [],
              events: [
                {
                  id: EventId.make("competing-allowed-event"),
                  type: "thread.metadata-updated",
                  threadId: activeA,
                  occurredAt: now,
                  payload: { ...thread, title: "Must not commit" },
                },
              ],
            }),
          );
        else
          yield* denied(
            f.sink.commitRejectedCommand(admission, {
              commandId: cmd.commandId,
              threadId: activeA,
              commandType: cmd.type,
              rejectedAt: now,
              error: "Allowed rejection",
            }),
          );
        insert.mockRestore();
        assert.equal((yield* f.projections.getThread(activeA)).title, thread.title);
        assert.lengthOf(
          yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${cmd.commandId}`,
          0,
        );
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "final transaction rechecks changed device grant and raw membership after planning without writing",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const grant = yield* Ref.make(grantA);
      const scopes = yield* Ref.make<ReadonlyArray<typeof AuthOrchestrationOperateScope>>([
        AuthOrchestrationOperateScope,
      ]);
      const current: MutationAuthority = {
        current: Effect.gen(function* () {
          return { resourceGrant: yield* Ref.get(grant), scopes: yield* Ref.get(scopes) };
        }),
      };
      const before = yield* f.projections.getThread(activeA);
      for (const cut of ["grant", "membership", "scope"] as const) {
        const cmd = rename(`serialized-cut-${cut}`);
        const ready = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const commit = f.sink.commitCommand;
        const spy = vi
          .spyOn(f.sink, "commitCommand")
          .mockImplementation((admission, input) =>
            Deferred.succeed(ready, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(commit(admission, input)),
            ),
          );
        const dispatch = yield* f.orchestrator.dispatch(current, cmd).pipe(Effect.forkChild);
        yield* Deferred.await(ready);
        if (cut === "grant") yield* Ref.set(grant, grantNone);
        else if (cut === "scope") yield* Ref.set(scopes, []);
        else
          yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
        yield* Deferred.succeed(release, undefined);
        yield* denied(Fiber.join(dispatch));
        spy.mockRestore();
        assert.lengthOf(
          yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${cmd.commandId}`,
          0,
        );
        assert.lengthOf(
          yield* f.sql`SELECT event_id FROM orchestration_events WHERE command_id = ${cmd.commandId}`,
          0,
        );
        assert.equal((yield* f.projections.getThread(activeA)).title, before.title);
        yield* Ref.set(grant, grantA);
        yield* Ref.set(scopes, [AuthOrchestrationOperateScope]);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectA} WHERE thread_id = ${activeA}`;
      }
    }).pipe(Effect.provide(layerTest)),
);

it.effect("permitted create/delete and deletion receipt replay retain operate-only semantics", () =>
  Effect.gen(function* () {
    const f = yield* makeFixture;
    const created = ThreadId.make("permitted-created-thread");
    yield* f.threads.dispatch(authority, {
      ...hiddenCommands["thread.create"],
      commandId: CommandId.make("create-permitted"),
      projectId: projectA,
      threadId: created,
    });
    assert.equal((yield* f.projections.getThread(created)).projectId, projectA);
    const deletion = {
      type: "thread.delete",
      threadId: created,
      commandId: CommandId.make("delete-permitted"),
    } as const;
    const accepted = yield* f.threads.dispatch(authority, deletion);
    assert.isNotNull((yield* f.projections.getThread(created)).deletedAt);
    assert.deepEqual(yield* f.threads.dispatch(authority, deletion), accepted);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "controllers admit sender, claimed attachments, explicit run and hidden children before hydration",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const send = {
        ...provenance,
        projectId: projectA,
        threadId: activeA,
        commandId: CommandId.make("controller-send"),
        messageId: MessageId.make("controller-send"),
        text: "hello",
        attachments: [],
        mode: "queue",
      } as const;
      yield* denied(f.threads.sendToThread(authority, { ...send, senderThreadId: activeB }));
      yield* denied(f.threads.sendToThread(authority, { ...send, attachments: [claimedB] }));
      yield* f.sql`INSERT INTO orchestration_v2_projection_runs (run_id, thread_id, ordinal, provider, status, requested_at, payload_json)
    VALUES ('controller-hidden-run', ${activeB}, 1, 'codex', 'completed', '2026-10-11', 'hidden malformed JSON')`;
      yield* denied(
        f.threads.interruptThread(authority, {
          projectId: projectA,
          threadId: activeA,
          commandId: CommandId.make("controller-interrupt"),
          runId: RunId.make("controller-hidden-run"),
        }),
      );
      yield* denied(
        f.threads.settleThread(authority, {
          threadId: activeB,
          commandId: CommandId.make("controller-settle"),
          byOwnAgent: true,
        }),
      );
      yield* denied(
        f.threads.stopDelegatedTasks(authority, {
          threadId: activeB,
          commandId: CommandId.make("controller-stop-children"),
        }),
      );
      yield* denied(
        f.threads.dispatch(authority, {
          ...hiddenCommands["thread.create"],
          threadId: ThreadId.make("native-import"),
          projectId: projectA,
          importedNativeThread: {
            ref: {
              driver: ProviderDriverKind.make("codex"),
              nativeId: "hidden-native",
              strength: "strong",
            },
          },
        }),
      );
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "all 55 server commands deny hidden and missing targets before importer or command writes",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'hidden malformed JSON' WHERE thread_id = ${activeB}`;
      assert.lengthOf(Object.values(hiddenCommands), 55);
      for (const command of Object.values(hiddenCommands))
        yield* denied(f.threads.dispatch(authority, command));
      yield* denied(
        f.threads.dispatch(authority, rename("missing-target", ThreadId.make("missing-target"))),
      );
      yield* denied(
        f.threads.dispatch(undefined as unknown as MutationAuthority, rename("omitted-authority")),
      );
      assert.deepEqual(f.imported, []);
      const rows = yield* f.sql`SELECT command_id FROM orchestration_command_receipts`;
      assert.lengthOf(rows, 0);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "operate-only authority mutates A and replays A; missing operate scope never hydrates",
  () =>
    Effect.gen(function* () {
      const f = yield* makeFixture;
      const cmd = rename("allowed-operate-only");
      const accepted = yield* f.threads.dispatch(authority, cmd);
      assert.isAbove(accepted.sequence, 0);
      assert.equal((yield* f.projections.getThread(activeA)).title, "Allowed rename");
      const replay = yield* f.threads.dispatch(authority, cmd);
      assert.deepEqual(replay, accepted);
      f.imported.length = 0;
      yield* denied(
        f.threads.dispatch(
          {
            current: Effect.succeed({
              scopes: [AuthOrchestrationReadScope],
              resourceGrant: grantA,
            }),
          },
          rename("read-only"),
        ),
      );
      assert.deepEqual(f.imported, []);
    }).pipe(Effect.provide(layerTest)),
);
