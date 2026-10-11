import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthFilesystemReadScope,
  AuthOrchestrationReadScope,
  type AuthEnvironmentScope,
  type AuthResourceGrant,
  EnvironmentAuthorizationError,
} from "@t3tools/contracts";
import { WS_METHODS } from "@t3tools/contracts";
import { workspaceReadHandlers } from "../ws.ts";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import {
  activeA,
  activeB,
  collectionStoreLayer,
  grantA,
  grantNone,
  makeCollectionFixture,
  projectA,
  projectB,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as GitManager from "../git/GitManager.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as Registry from "../vcs/VcsDriverRegistry.ts";
import * as Broadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as Review from "../review/ReviewService.ts";
import * as Tracker from "../project/WorktreeSetupTracker.ts";
import * as ServerConfig from "../config.ts";
import * as Settings from "../serverSettings.ts";
import * as Hosting from "../sourceControl/SourceControlProviderRegistry.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import { trustedReadAuthority, type ThreadReadAuthority } from "./ResourceAuthorization.ts";
import * as Access from "./workspaceReadAccess.ts";

const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);
const testLayer = Layer.mergeAll(collectionStoreLayer, NodeServices.layer);
const denied = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(isAuthorizationError(result.failure)).toBe(true);
    }),
  );
const fixture = Effect.gen(function* () {
  yield* makeCollectionFixture;
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const base = yield* fs.makeTempDirectoryScoped({ prefix: "t3-workspace-read-" });
  const a = `${base}/a`,
    b = `${a}/hidden`,
    worktree = `${base}/worktree-a`,
    foreignWorktree = `${a}/foreign-worktree`,
    alias = `${base}/alias-a`,
    child = `${a}/child`;
  for (const root of [a, b, worktree, foreignWorktree, child])
    yield* fs.makeDirectory(root, { recursive: true });
  yield* fs.symlink(a, alias);
  yield* sql`UPDATE projection_projects SET workspace_root = ${a} WHERE project_id = ${projectA}`;
  yield* sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`;
  yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${worktree}) WHERE thread_id = ${activeA}`;
  yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${foreignWorktree}) WHERE thread_id = ${activeB}`;
  let current: { scopes: ReadonlyArray<AuthEnvironmentScope>; resourceGrant: AuthResourceGrant } = {
    scopes: [AuthOrchestrationReadScope],
    resourceGrant: grantA,
  };
  const authority: ThreadReadAuthority = { current: Effect.sync(() => current) };
  const set = (
    grant: AuthResourceGrant = grantA,
    scopes: ReadonlyArray<AuthEnvironmentScope> = [AuthOrchestrationReadScope],
  ) => {
    current = { scopes, resourceGrant: grant };
  };
  const calls: string[] = [];
  const registry = Layer.mock(Registry.VcsDriverRegistry)({
    detect: ({ cwd }) =>
      Effect.sync(() => {
        calls.push(`detect:${cwd}`);
        return null;
      }),
    resolve: ({ cwd }) =>
      Effect.sync(() => {
        calls.push(`resolve:${cwd}`);
        return { kind: "git" } as Registry.VcsDriverHandle;
      }),
  });
  const workflow = yield* GitWorkflow.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        registry,
        Layer.mock(Git.GitVcsDriver)({}),
        Layer.mock(GitManager.GitManager)({
          invalidateLocalStatus: () => Effect.void,
          invalidateStatus: () => Effect.void,
        }),
      ),
    ),
  );
  const review = yield* Review.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        registry,
        Layer.mock(Git.GitVcsDriver)({}),
        ServerConfig.layerTest(base, base),
        Settings.ServerSettingsService.layerTest(),
      ),
    ),
  );
  const tracker = yield* Tracker.make;
  const broadcaster = yield* Broadcaster.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(GitWorkflow.GitWorkflowService, workflow),
        Layer.mock(BackgroundPolicy.BackgroundPolicy)({
          streamChanges: Stream.empty,
          shouldRunScopeWork: () =>
            Effect.sync(() => {
              calls.push("policy");
              return false;
            }),
        }),
      ),
    ),
  );
  return {
    sql,
    fs,
    a,
    b,
    base,
    worktree,
    foreignWorktree,
    alias,
    child,
    authority,
    set,
    calls,
    workflow,
    review,
    tracker,
    broadcaster,
  };
});
const managerFor = (overrides: Partial<Git.GitVcsDriver["Service"]>) =>
  GitManager.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(Git.GitVcsDriver)(overrides),
        Layer.mock(Hosting.SourceControlProviderRegistry)({ resolveLink: () => undefined }),
        Layer.mock(TextGeneration.TextGeneration)({}),
        Layer.mock(ProviderRegistry.ProviderRegistry)({}),
        Layer.mock(Setup.ProjectSetupScriptRunner)({}),
        Settings.ServerSettingsService.layerTest(),
      ),
    ),
  );
const nonRepo: Git.GitStatusDetails = {
  isRepo: false,
  hasOriginRemote: false,
  isDefaultBranch: false,
  branch: null,
  upstreamRef: null,
  hasWorkingTreeChanges: false,
  workingTree: { files: [], insertions: 0, deletions: 0 },
  hasUpstream: false,
  aheadCount: 0,
  behindCount: 0,
  aheadOfDefaultCount: 0,
};

it.effect(
  "all direct read entries deny hidden owners before routing, Git, credentials, cache or poller work",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const manager = yield* managerFor({});
      for (const cwd of [f.b, f.foreignWorktree]) {
        const input = { cwd, projectId: projectA };
        const operations: ReadonlyArray<
          Effect.Effect<
            unknown,
            EnvironmentAuthorizationError | import("@t3tools/contracts").GitManagerServiceError
          >
        > = [
          f.workflow.isRepository(f.authority, cwd),
          f.workflow.hasCommit(f.authority, { ...input, refName: "HEAD" }),
          f.workflow.status(f.authority, input),
          f.workflow.localStatus(f.authority, input),
          f.workflow.remoteStatus(f.authority, input),
          f.workflow.resolvePullRequest(f.authority, { ...input, reference: "#5" }),
          f.workflow.listRefs(f.authority, input),
          f.workflow.listLocalBranchNames(f.authority, cwd),
          f.workflow.remoteExists(f.authority, { ...input, remoteName: "origin" }),
          f.workflow.remoteBranchExists(f.authority, {
            ...input,
            remoteName: "origin",
            refName: "main",
          }),
          f.workflow.resolveRemoteTrackingCommit(f.authority, {
            ...input,
            refName: "main",
            fallbackRemoteName: "origin",
          }),
          manager.status(f.authority, input),
          manager.localStatus(f.authority, input),
          manager.remoteStatus(f.authority, input),
          manager.branchPullRequest(f.authority, { ...input, branch: "main" }),
          manager.resolvePullRequest(f.authority, { ...input, reference: "#5" }),
          f.broadcaster.getStatus(f.authority, input),
          f.broadcaster.refreshLocalStatus(f.authority, cwd),
          f.broadcaster.refreshStatus(f.authority, cwd),
          f.broadcaster.refreshPullRequestStatus(f.authority, cwd),
          Stream.runCollect(f.broadcaster.streamStatus(f.authority, input).pipe(Stream.take(1))),
        ];
        for (const operation of operations) yield* denied(operation);
        f.set(grantA, [AuthFilesystemReadScope]);
        yield* denied(f.review.getDiffPreview(f.authority, input));
        yield* denied(
          f.review.getDiffFileContents(f.authority, {
            ...input,
            sourceKind: "working-tree",
            changeType: "change",
            baseRef: null,
            headRef: null,
            oldPath: "secret",
            newPath: "secret",
          }),
        );
        f.set();
      }
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'broken json' WHERE thread_id = ${activeB}`;
      yield* denied(f.tracker.get(f.authority, activeB));
      yield* denied(Stream.runCollect(f.tracker.stream(f.authority, activeB).pipe(Stream.take(1))));
      expect(f.calls).toEqual([]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "registered roots, worktrees, canonical aliases and children remain usable with the existing action scopes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const access = yield* Access.make;
      for (const cwd of [f.a, f.worktree, f.alias, f.child]) {
        yield* access.admit(f.authority, { cwd, projectId: projectA });
        expect((yield* f.workflow.localStatus(f.authority, { cwd })).isRepo).toBe(false);
        f.set(grantA, [AuthFilesystemReadScope]);
        expect((yield* f.review.getDiffPreview(f.authority, { cwd })).sources).toEqual([]);
        yield* denied(f.workflow.status(f.authority, { cwd }));
        f.set();
        yield* denied(f.review.getDiffPreview(f.authority, { cwd }));
      }
      yield* access.admit(f.authority, { cwd: f.worktree, threadId: activeA });
      yield* denied(access.admit(f.authority, { cwd: f.a, threadId: activeB }));
      yield* denied(access.admit(f.authority, { cwd: f.b, projectId: projectA }));
      yield* denied(access.admit(f.authority, { cwd: f.base }));
      // Existing owner cwd compatibility and explicit bootstrap trust remain available.
      f.set({ version: 1, projects: { mode: "all" }, providers: { mode: "all" } });
      yield* access.admit(f.authority, { cwd: f.base });
      yield* denied(access.admit(f.authority, { cwd: f.b, projectId: projectA }));
      yield* denied(f.workflow.status(undefined as unknown as ThreadReadAuthority, { cwd: f.a }));
      yield* access.admit(trustedReadAuthority, { cwd: "bootstrap path" });
    }).pipe(Effect.provide(testLayer)),
);

it.effect.each(["grant", "scope", "owner", "root"] as const)(
  "canonical await rechecks current %s before any protected routing",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let once = true;
      const fs = {
        ...f.fs,
        realPath: (path: string) =>
          path === f.a && once
            ? Effect.sync(() => {
                once = false;
              }).pipe(
                Effect.andThen(Deferred.succeed(entered, undefined)),
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(f.fs.realPath(path)),
              )
            : f.fs.realPath(path),
      };
      const access = yield* Access.make.pipe(Effect.provideService(FileSystem.FileSystem, fs));
      const reader = yield* access
        .read(
          f.authority,
          { cwd: f.worktree, threadId: activeA },
          Effect.sync(() => f.calls.push("read")),
        )
        .pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone);
      if (cut === "scope") f.set(grantA, []);
      if (cut === "owner")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      if (cut === "root")
        yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.base} WHERE project_id = ${projectA}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(reader);
      expect(Result.isFailure(result)).toBe(true);
      expect(f.calls).toEqual([]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("malformed raw roots and hidden thread bodies cannot supply workspace authority", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const access = yield* Access.make;
    yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = 'invalid json' WHERE thread_id = ${activeB}`;
    yield* f.workflow.localStatus(f.authority, { cwd: f.a });
    for (const root of ["relative/path", "/", `${f.a}/../a`, `${f.a}\0bad`]) {
      yield* f.sql`UPDATE projection_projects SET workspace_root = ${root} WHERE project_id = ${projectA}`;
      yield* denied(access.admit(f.authority, { cwd: f.worktree, threadId: activeA }));
    }
    yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.a} WHERE project_id = ${projectA}`;
    yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', 123) WHERE thread_id = ${activeA}`;
    yield* denied(access.admit(f.authority, { cwd: f.worktree, threadId: activeA }));
  }).pipe(Effect.provide(testLayer)),
);

it.effect("a warm direct Git status cache still requires current resource admission", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    let probes = 0;
    const manager = yield* managerFor({
      statusDetailsLocal: () =>
        Effect.sync(() => {
          probes++;
          return nonRepo;
        }),
    });
    yield* manager.localStatus(f.authority, { cwd: f.a });
    yield* manager.localStatus(f.authority, { cwd: f.alias });
    expect(probes).toBe(1);
    f.set(grantNone);
    yield* denied(manager.localStatus(f.authority, { cwd: f.a }));
    expect(probes).toBe(1);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "a Git read milestone cannot refine hosting credentials after current grant revocation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let configReads = 0;
      const manager = yield* managerFor({
        statusDetailsLocal: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as({ ...nonRepo, isRepo: true, branch: "main" }),
          ),
        readConfigValue: () =>
          Effect.sync(() => {
            configReads++;
            return "https://github.com/hidden/repo";
          }),
      });
      const reader = yield* denied(manager.localStatus(f.authority, { cwd: f.a })).pipe(
        Effect.forkScoped,
      );
      yield* Deferred.await(entered);
      f.set(grantNone);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(reader);
      expect(configReads).toBe(0);
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "setup and VCS streams re-admit each emission and deny hidden subscriptions before poller creation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.tracker.begin({
        threadId: activeA,
        branch: null,
        baseRef: null,
        stages: ["agent"],
        fiber: null,
      });
      const setupFirst = yield* Deferred.make<void>();
      const vcsFirst = yield* Deferred.make<void>();
      let setups = 0,
        statuses = 0;
      const setupReader = yield* denied(
        f.tracker.stream(f.authority, activeA).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              setups++;
            }).pipe(Effect.andThen(Deferred.succeed(setupFirst, undefined))),
          ),
          Stream.runDrain,
        ),
      ).pipe(Effect.forkScoped);
      const vcsReader = yield* denied(
        f.broadcaster.streamStatus(f.authority, { cwd: f.a, includeRemote: false }).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              statuses++;
            }).pipe(Effect.andThen(Deferred.succeed(vcsFirst, undefined))),
          ),
          Stream.runDrain,
        ),
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(setupFirst);
      yield* Deferred.await(vcsFirst);
      f.set(grantNone);
      yield* f.tracker.stageStatus(activeA, "agent", "running");
      yield* f.broadcaster.refreshStatus(trustedReadAuthority, f.a);
      yield* Fiber.join(setupReader);
      yield* Fiber.join(vcsReader);
      expect(setups).toBe(1);
      expect(statuses).toBe(1);
      expect(f.calls.filter((call) => call === "policy")).toEqual([]);
    }).pipe(Effect.provide(testLayer)),
);

it.effect("ancestor repository fallback is denied before actual Git routing", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.fs.makeDirectory(`${f.base}/.git`);
    yield* denied(f.workflow.localStatus(f.authority, { cwd: f.a }));
    yield* denied(f.workflow.listRefs(f.authority, { cwd: f.a }));
    f.set(grantA, [AuthFilesystemReadScope]);
    yield* denied(f.review.getDiffPreview(f.authority, { cwd: f.a }));
    expect(f.calls).toEqual([]);
    // A's own repository marker prevents discovery from falling into the ancestor.
    yield* f.fs.makeDirectory(`${f.a}/.git`);
    f.set();
    yield* f.workflow.localStatus(f.authority, { cwd: f.child });
    expect(f.calls).toEqual([`detect:${f.child}`]);
  }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "routing awaits cannot release a Git/review read after revocation; permitted controls retain filesystem-only review",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let gitReads = 0;
      const driver = Layer.mock(Git.GitVcsDriver)({
        listLocalBranchNames: () =>
          Effect.sync(() => {
            gitReads++;
            return ["main"];
          }),
        getReviewDiffFileContents: () =>
          Effect.sync(() => {
            gitReads++;
            return { oldContents: "old", newContents: "new" };
          }),
      });
      const handle = { kind: "git", repository: { rootPath: f.a } } as Registry.VcsDriverHandle;
      const routing = Layer.mock(Registry.VcsDriverRegistry)({
        resolve: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as(handle),
          ),
        detect: () => Effect.succeed(handle),
      });
      const workflow = yield* GitWorkflow.make.pipe(
        Effect.provide(Layer.mergeAll(routing, driver, Layer.mock(GitManager.GitManager)({}))),
      );
      const reader = yield* denied(workflow.listLocalBranchNames(f.authority, f.a)).pipe(
        Effect.forkScoped,
      );
      yield* Deferred.await(entered);
      f.set(grantNone);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(reader);
      expect(gitReads).toBe(0);
      f.set();
      expect(yield* workflow.listLocalBranchNames(f.authority, f.child)).toEqual(["main"]);
      const review = yield* Review.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            routing,
            driver,
            ServerConfig.layerTest(f.base, f.base),
            Settings.ServerSettingsService.layerTest(),
          ),
        ),
      );
      f.set(grantA, [AuthFilesystemReadScope]);
      expect(
        yield* review.getDiffFileContents(f.authority, {
          cwd: f.worktree,
          sourceKind: "working-tree",
          changeType: "change",
          baseRef: null,
          headRef: null,
          oldPath: "a",
          newPath: "a",
        }),
      ).toEqual({ oldContents: "old", newContents: "new" });
    }).pipe(Effect.provide(testLayer)),
);

it.effect(
  "direct WS controllers use the same actual service admission, including filesystem-only review",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const handlers = workspaceReadHandlers(
        f.authority,
        {
          gitWorkflow: f.workflow,
          vcsStatusBroadcaster: f.broadcaster,
          review: f.review,
          worktreeSetupTracker: f.tracker,
        },
        Effect.succeed(Duration.seconds(60)),
      );
      yield* denied(
        Stream.runCollect(
          handlers[WS_METHODS.subscribeVcsStatus]({ cwd: f.b }).pipe(Stream.take(1)),
        ),
      );
      yield* denied(
        Stream.runCollect(
          handlers[WS_METHODS.subscribeWorktreeSetup]({ threadId: activeB }).pipe(Stream.take(1)),
        ),
      );
      yield* denied(handlers[WS_METHODS.vcsRefreshStatus]({ cwd: f.b }));
      yield* denied(handlers[WS_METHODS.gitResolvePullRequest]({ cwd: f.b, reference: "5" }));
      yield* denied(handlers[WS_METHODS.vcsListRefs]({ cwd: f.b }));
      f.set(grantA, [AuthFilesystemReadScope]);
      yield* denied(handlers[WS_METHODS.reviewGetDiffPreview]({ cwd: f.b }));
      yield* denied(
        handlers[WS_METHODS.reviewGetDiffFileContents]({
          cwd: f.b,
          sourceKind: "working-tree",
          changeType: "change",
          baseRef: null,
          headRef: null,
          oldPath: "x",
          newPath: "x",
        }),
      );
      expect(f.calls).toEqual([]);
      expect(
        (yield* handlers[WS_METHODS.reviewGetDiffPreview]({ cwd: f.worktree })).sources,
      ).toEqual([]);
    }).pipe(Effect.provide(testLayer)),
);
