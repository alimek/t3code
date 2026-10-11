import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it, vi } from "@effect/vitest";
import {
  AuthSourceControlWriteScope,
  CommandId,
  EnvironmentAuthorizationError,
  ProviderDriverKind,
  type AuthEnvironmentScope,
  type AuthResourceGrant,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../config.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Access from "../auth/workspaceReadAccess.ts";
import { checkedSourceControlLinkAuthority } from "../auth/CommandAuthorization.ts";
import { linkCreatedPullRequest } from "../git/linkCreatedPullRequest.ts";
import { refreshPushedPullRequests } from "../git/refreshPushedPullRequests.ts";
import * as Projects from "../project/ProjectService.ts";
import * as PullRequests from "../pullRequest/PullRequestService.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
import type { ProviderAdapterV2Shape } from "./ProviderAdapter.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as Replay from "./testkit/ProviderReplayHarness.ts";
import {
  activeA,
  activeB,
  collectionStoreLayer,
  grantA,
  grantNone,
  makeCollectionFixture,
  projectA,
  projectB,
  provider,
} from "./projectCollectionTestFixtures.ts";

const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

const layer = Layer.mergeAll(
  collectionStoreLayer,
  NodeServices.layer,
  Config.layerTest(process.cwd(), { prefix: "t3-git-link-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
  Replay.layerWithRegistry(
    { name: "git-action-link-admission" },
    Registry.layerFromAdapters([
      {
        instanceId: provider,
        driver: ProviderDriverKind.make("codex"),
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
        openSession: () => Effect.die("provider execution disabled"),
      } as ProviderAdapterV2Shape,
    ]),
    { databaseLayer: Sqlite.layerMemory, runEffectWorker: false },
  ),
);
const denied = <A, E, R>(operation: Effect.Effect<A, E, R>) =>
  operation.pipe(
    Effect.result,
    Effect.map((result) => {
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
    }),
  );
const fixture = Effect.gen(function* () {
  const f = yield* makeCollectionFixture;
  const fs = yield* FileSystem.FileSystem,
    sql = yield* SqlClient.SqlClient;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-link-a-" }),
    b = `${root}/hidden-b`;
  yield* fs.makeDirectory(b);
  yield* sql`UPDATE projection_projects SET workspace_root = ${root} WHERE project_id = ${projectA}`;
  yield* sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`;
  let current: { scopes: ReadonlyArray<AuthEnvironmentScope>; resourceGrant: AuthResourceGrant } = {
    scopes: [AuthSourceControlWriteScope],
    resourceGrant: grantA,
  };
  const caller = { current: Effect.sync(() => current) };
  const access = yield* Access.make;
  const admission = yield* access.capture(
    caller,
    { cwd: root, threadId: activeA },
    AuthSourceControlWriteScope,
  );
  const engine = yield* Orchestrator.OrchestratorV2,
    sink = yield* EventSink.EventSinkV2;
  const project = yield* f.projects.getShell(projectA);
  const projectLayer = Layer.mock(Projects.ProjectService)({
    getShell: () => Effect.succeed(project),
  });
  const link = (commandId: CommandId) =>
    linkCreatedPullRequest({
      admission,
      threadId: activeA,
      result: { pr: { status: "created", number: 42, url: "https://github.com/a/repo/pull/42" } },
      commandId: Effect.succeed(commandId),
    }).pipe(Effect.provide(projectLayer));
  return {
    ...f,
    sql,
    root,
    b,
    caller,
    admission,
    engine,
    sink,
    link,
    set: (
      resourceGrant = grantA,
      scopes: ReadonlyArray<AuthEnvironmentScope> = [AuthSourceControlWriteScope],
    ) => {
      current = { resourceGrant, scopes };
    },
  };
});

it.effect(
  "write-only checked link commits/replays through actual SQL; generic commands still need operate",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        id = CommandId.make("source-control-link:allowed");
      yield* f.link(id);
      yield* f.link(id);
      const rows = yield* f.sql<{
        readonly type: string;
      }>`SELECT command_type AS type FROM orchestration_command_receipts WHERE command_id = ${id}`;
      expect(rows).toEqual([{ type: "thread.pull-request.link" }]);
      const shell = yield* f.engine.getThreadShell(activeA);
      expect(shell?.pullRequests).toHaveLength(1);
      const original = {
        type: "thread.pull-request.link" as const,
        commandId: CommandId.make("source-control-link:bound-command"),
        threadId: activeA,
        host: "github.com",
        repository: "a/repo",
        number: 43,
        url: "https://github.com/a/repo/pull/43",
        source: "created" as const,
      };
      const authority = checkedSourceControlLinkAuthority(f.admission, original);
      yield* denied(f.engine.dispatch(authority, { ...original, threadId: activeB }));
      yield* denied(
        f.engine.dispatch(authority, {
          type: "thread.archive",
          commandId: original.commandId,
          threadId: activeA,
        }),
      );
      const generic = yield* f.engine
        .dispatch(f.caller, {
          type: "thread.archive",
          commandId: CommandId.make("source-control-link:generic"),
          threadId: activeA,
        })
        .pipe(Effect.result);
      expect(Result.isFailure(generic)).toBe(true);
      f.set(grantNone);
      yield* denied(f.link(id));
    }).pipe(Effect.provide(layer)),
);
it.effect.each(["grant", "scope", "root", "membership"] as const)(
  "checked link %s cut at serialized commit leaves no receipt/event",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        id = CommandId.make(`source-control-link:cut:${cut}`);
      const reached = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const commit = f.sink.commitCommand;
      const spy = vi
        .spyOn(f.sink, "commitCommand")
        .mockImplementation((admission, input) =>
          input.commandId !== id
            ? commit(admission, input)
            : Deferred.succeed(reached, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(commit(admission, input)),
              ),
        );
      try {
        const pending = yield* f.link(id).pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(reached);
        if (cut === "grant") f.set(grantNone);
        if (cut === "scope") f.set(grantA, []);
        if (cut === "root")
          yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.b} WHERE project_id = ${projectA}`;
        if (cut === "membership")
          yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending);
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result))
          expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
        const receipts =
          yield* f.sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = ${id}`;
        const events =
          yield* f.sql`SELECT command_id FROM orchestration_events WHERE command_id = ${id}`;
        expect(receipts).toEqual([]);
        expect(events).toEqual([]);
      } finally {
        spy.mockRestore();
      }
    }).pipe(Effect.provide(layer)),
);
it.effect(
  "post-push callback cannot refresh a retargeted hidden thread or fall back to hidden project",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const reached = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const get = f.engine.getThreadShell;
      let refreshed = 0;
      const spy = vi
        .spyOn(f.engine, "getThreadShell")
        .mockImplementation((id) =>
          Deferred.succeed(reached, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.andThen(get(id)),
          ),
        );
      try {
        const pending = yield* refreshPushedPullRequests(
          f.admission,
          { cwd: f.root, threadId: activeA },
          { push: { status: "pushed" } },
        ).pipe(
          Effect.provide(
            Layer.mock(PullRequests.PullRequestService)({
              refreshAfterTurn: () =>
                Effect.sync(() => {
                  refreshed++;
                }),
            }),
          ),
          Effect.result,
          Effect.forkChild,
        );
        yield* Deferred.await(reached);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(pending);
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result))
          expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
        expect(refreshed).toBe(0);
      } finally {
        spy.mockRestore();
      }
    }).pipe(Effect.provide(layer)),
);
