import type * as SourceControl from "../sourceControl/SourceControlProvider.ts";
import * as Tracker from "../project/WorktreeSetupTracker.ts";
import * as Projects from "../project/ProjectService.ts";
import * as Terminals from "../terminal/Manager.ts";
import * as Settings from "../serverSettings.ts";
import * as Hosting from "../sourceControl/SourceControlProviderRegistry.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as Broadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as Background from "../background/BackgroundPolicy.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthSourceControlWriteScope,
  AuthOrchestrationOperateScope,
  DEFAULT_SERVER_SETTINGS,
  AuthOrchestrationReadScope,
  EnvironmentAuthorizationError,
  VcsUnsupportedOperationError,
  SourceControlProviderError as SourceControlProviderFailure,
  ThreadId,
  type AuthEnvironmentScope,
  type AuthResourceGrant,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { HttpClient, HttpClientResponse } from "effect/http";
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
import * as Config from "../config.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import * as Registry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as Provisioning from "../vcs/VcsProvisioningService.ts";
import * as Workflow from "../git/GitWorkflowService.ts";
import * as Manager from "../git/GitManager.ts";
import * as GitHubApi from "../sourceControl/GitHubApi.ts";
import * as Credentials from "../sourceControl/GitHubCredentials.ts";
import * as Quota from "../sourceControl/githubQuota.ts";
import * as Limits from "../sourceControl/SourceControlRateLimit.ts";
import * as Identity from "../project/RepositoryIdentityResolver.ts";
import * as Publish from "../sourceControl/SourceControlRepositoryService.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Access from "./workspaceReadAccess.ts";
import { trustedReadAuthority, type ThreadReadAuthority } from "./ResourceAuthorization.ts";

const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

const layer = Layer.mergeAll(collectionStoreLayer, NodeServices.layer);
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
  const records = yield* makeCollectionFixture;
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const base = yield* fs.makeTempDirectoryScoped({ prefix: "t3-workspace-write-" });
  const a = `${base}/a`,
    b = `${a}/hidden-b`,
    worktree = `${base}/a-worktree`,
    foreign = `${a}/b-worktree`;
  for (const root of [a, b, worktree, foreign]) yield* fs.makeDirectory(root, { recursive: true });
  yield* sql`UPDATE projection_projects SET workspace_root = ${a} WHERE project_id = ${projectA}`;
  yield* sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`;
  yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${worktree}) WHERE thread_id = ${activeA}`;
  yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${foreign}) WHERE thread_id = ${activeB}`;
  let current: { scopes: ReadonlyArray<AuthEnvironmentScope>; resourceGrant: AuthResourceGrant } = {
    scopes: [AuthSourceControlWriteScope],
    resourceGrant: grantA,
  };
  const authority: ThreadReadAuthority = { current: Effect.sync(() => current) };
  const set = (
    resourceGrant = grantA,
    scopes: ReadonlyArray<AuthEnvironmentScope> = [AuthSourceControlWriteScope],
  ) => {
    current = { resourceGrant, scopes };
  };
  const access = yield* Access.make;
  return { sql, fs, base, a, b, worktree, foreign, authority, set, access, records };
});
const handle = (
  exitCode: Effect.Effect<ChildProcessSpawner.ExitCode> = Effect.succeed(
    ChildProcessSpawner.ExitCode(0),
  ),
  stdout = "",
) =>
  ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode,
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.encodeText(Stream.make(stdout)),
    stderr: Stream.empty,
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
const core = (
  f: Effect.Success<typeof fixture>,
  spawn: ChildProcessSpawner.ChildProcessSpawner["Service"]["spawn"],
) =>
  makeGitVcsDriverCore().pipe(
    Effect.provide(Config.layerTest(f.base, f.base)),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, ChildProcessSpawner.make(spawn)),
  );
const workflow = (
  f: Effect.Success<typeof fixture>,
  git: Git.GitVcsDriver["Service"],
  resolve?: Registry.VcsDriverRegistry["Service"]["resolve"],
) =>
  Workflow.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(Git.GitVcsDriver, git),
        Layer.mock(Manager.GitManager)({}),
        Layer.mock(Registry.VcsDriverRegistry)({
          ...(resolve === undefined ? {} : { detect: resolve }),
          resolve:
            resolve ??
            (({ cwd }) =>
              Effect.succeed({
                kind: "git",
                repository: { rootPath: cwd },
              } as Registry.VcsDriverHandle)),
        }),
      ),
    ),
  );

it.effect(
  "write-only callers mutate A roots/worktrees; forged/hidden owners fail before routing",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const commands: string[][] = [];
      const git = yield* core(f, (command) =>
        Effect.sync(() => {
          commands.push([...(ChildProcess.isStandardCommand(command) ? command.args : [])]);
          return handle();
        }),
      );
      let routed = 0;
      const service = yield* workflow(f, git, ({ cwd }) =>
        Effect.sync(() => {
          routed++;
          return { kind: "git", repository: { rootPath: cwd } } as Registry.VcsDriverHandle;
        }),
      );
      yield* service.createRef(f.authority, { cwd: f.a, refName: "allowed-root" });
      yield* service.createRef(f.authority, { cwd: f.worktree, refName: "allowed-worktree" });
      const initial = routed;
      for (const input of [
        { cwd: f.b, projectId: projectA },
        { cwd: f.foreign, projectId: projectA },
        { cwd: f.a, threadId: activeB },
        { cwd: f.a, threadId: ThreadId.make("guessed-hidden") },
        { cwd: f.a, projectId: projectB },
        { cwd: `${f.base}/unregistered`, projectId: projectA },
      ])
        yield* denied(service.createRef(f.authority, { ...input, refName: "denied" }));
      expect(routed).toBe(initial);
      expect(commands.filter((args) => args[0] === "branch")).toEqual([
        ["branch", "allowed-root"],
        ["branch", "allowed-worktree"],
      ]);
      f.set(grantA, [AuthOrchestrationReadScope]);
      yield* denied(service.createRef(f.authority, { cwd: f.a, refName: "read-only" }));
      yield* service.createRef(trustedReadAuthority, { cwd: f.b, refName: "trusted" });
      f.set({ version: 1, projects: { mode: "all" }, providers: { mode: "all" } });
      yield* service.createRef(f.authority, { cwd: f.b, refName: "owner" });
      expect(commands.filter((args) => args[0] === "branch").slice(-2)).toEqual([
        ["branch", "trusted"],
        ["branch", "owner"],
      ]);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["grant", "scope", "root", "membership"] as const)(
  "final Git spawn rejects %s cut after independent driver preflight",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const commands: string[][] = [];
      const git = yield* core(f, (command) =>
        Effect.sync(() => {
          commands.push([...(ChildProcess.isStandardCommand(command) ? command.args : [])]);
          return handle();
        }),
      );
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.worktree, threadId: activeA },
        AuthSourceControlWriteScope,
      );
      const service = yield* workflow(f, git, ({ cwd }) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as({ kind: "git", repository: { rootPath: cwd } } as Registry.VcsDriverHandle),
        ),
      );
      const fiber = yield* service
        .createRef(f.authority, { cwd: f.worktree, refName: "cut" })
        .pipe(
          Effect.provideService(Access.workspaceEffectAdmission, admission),
          Effect.result,
          Effect.forkChild,
        );
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone);
      if (cut === "scope") f.set(grantA, [AuthOrchestrationReadScope]);
      if (cut === "root")
        yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.b} WHERE project_id = ${projectA}`;
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(fiber);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(commands).toEqual([]);
    }).pipe(Effect.provide(layer)),
);

it.effect("Git admission runs after the existing process semaphore wait", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const occupied = yield* Deferred.make<void>(),
      free = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
    let spawned = 0;
    const git = yield* core(f, () =>
      Effect.gen(function* () {
        spawned++;
        if (spawned === 8) yield* Deferred.succeed(occupied, undefined);
        return handle(Deferred.await(free));
      }),
    );
    const blockers = [];
    for (let i = 0; i < 8; i++)
      blockers.push(
        yield* git
          .execute({
            operation: "test.hold-permit",
            cwd: f.a,
            args: ["status"],
          })
          .pipe(Effect.forkChild),
      );
    yield* Deferred.await(occupied);
    const admission = yield* f.access.capture(
      f.authority,
      { cwd: f.a },
      AuthSourceControlWriteScope,
    );
    const entered = yield* Deferred.make<void>();
    const effect = Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(git.createRef({ cwd: f.a, refName: "queued" })),
    );
    const pending = yield* effect.pipe(
      Effect.provideService(Access.workspaceEffectAdmission, admission),
      Effect.result,
      Effect.forkChild,
    );
    yield* Deferred.await(entered);
    f.set(grantNone);
    yield* Deferred.succeed(free, ChildProcessSpawner.ExitCode(0));
    yield* Effect.forEach(blockers, Fiber.join);
    const result = yield* Fiber.join(pending);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result))
      expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
    expect(spawned).toBe(8);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["createRepository", "createPullRequest"] as const)(
  "%s checks current owner/scope after credential preflight and before HTTP",
  (operation) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let requests = 0;
      const api = yield* GitHubApi.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(
              Credentials.GitHubCredentials,
              Credentials.GitHubCredentials.of({
                get: (host) =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as({
                      host,
                      token: Redacted.make("test-only"),
                      source: "env" as const,
                      fingerprint: "test-only",
                    }),
                  ),
                invalidate: () => Effect.void,
              }),
            ),
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) => {
                requests++;
                return Effect.succeed(
                  HttpClientResponse.fromWeb(request, new Response("{}", { status: 201 })),
                );
              }),
            ),
            Quota.layer,
            Limits.layer,
          ),
        ),
      );
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.worktree, threadId: activeA },
        AuthSourceControlWriteScope,
      );
      const request = api.rest({
        host: "github.com",
        operation,
        method: "POST",
        path: operation === "createRepository" ? "user/repos" : "repos/a/repo/pulls",
        body: {},
      });
      let routed = 0;
      const publish = yield* Publish.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Config.layerTest(f.base, f.base),
            Layer.mock(Git.GitVcsDriver)({
              ensureRemote: () => Effect.succeed("origin"),
              execute: () =>
                Effect.succeed({
                  exitCode: ChildProcessSpawner.ExitCode(0),
                  stdout: "HEAD",
                  stderr: "",
                  stdoutTruncated: false,
                  stderrTruncated: false,
                }),
              pushCurrentBranch: () =>
                Effect.succeed({
                  status: "pushed",
                  branch: "main",
                  upstreamBranch: "origin/main",
                  setUpstream: true,
                }),
            }),
            Layer.mock(Hosting.SourceControlProviderRegistry)({
              resolveLink: () => Effect.die("unused"),
              get: () =>
                Effect.sync(() => {
                  routed++;
                  return {
                    kind: "github" as const,
                    createRepository: () =>
                      request.pipe(
                        Effect.mapError((error) =>
                          isEnvironmentAuthorizationError(error)
                            ? error
                            : new SourceControlProviderFailure({
                                provider: "github",
                                operation,
                                cwd: f.worktree,
                                detail: "Test HTTP failure",
                                cause: error,
                              }),
                        ),
                        Effect.as({
                          nameWithOwner: "a/repo",
                          url: "https://github.com/a/repo",
                          sshUrl: "git@github.com:a/repo.git",
                        }),
                      ),
                    listChangeRequests: () => Effect.die("unused"),
                    getDefaultBranch: () => Effect.die("unused"),
                    createChangeRequest: () => Effect.die("unused"),
                    getChangeRequest: () => Effect.die("unused"),
                    getRepositoryCloneUrls: () => Effect.die("unused"),
                    checkoutChangeRequest: () => Effect.die("unused"),
                  };
                }),
            }),
          ),
        ),
      );
      const run = Effect.gen(function* () {
        if (operation === "createRepository")
          yield* publish.publishRepository(f.authority, {
            cwd: f.worktree,
            provider: "github",
            repository: "a/repo",
            visibility: "private",
          });
        else yield* request;
      });
      const pending = yield* run.pipe(
        Effect.provideService(Access.workspaceEffectAdmission, admission),
        Effect.result,
        Effect.forkChild,
      );
      yield* Deferred.await(entered);
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(requests).toBe(0);
      if (operation === "createRepository") {
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectA} WHERE thread_id = ${activeA}`;
        const input = {
          cwd: f.worktree,
          provider: "github" as const,
          repository: "a/repo",
          visibility: "private" as const,
        };
        expect((yield* publish.publishRepository(f.authority, input)).status).toBe("pushed");
        expect(requests).toBe(1);
        expect(routed).toBe(2);
        yield* denied(publish.publishRepository(f.authority, { ...input, cwd: f.b }));
        expect(routed).toBe(2);
        expect(requests).toBe(1);
      }
    }).pipe(Effect.provide(layer)),
);

it.effect("separate init ProcessRunner path checks after its existing semaphore wait", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const occupied = yield* Deferred.make<void>(),
      free = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
    const entered = yield* Deferred.make<void>();
    let spawns = 0;
    const process = yield* VcsProcess.make.pipe(
      Effect.provide(
        ProcessRunner.layer.pipe(
          Layer.provide(
            Layer.succeed(
              ChildProcessSpawner.ChildProcessSpawner,
              ChildProcessSpawner.make(() =>
                Effect.gen(function* () {
                  spawns++;
                  if (spawns === 8) yield* Deferred.succeed(occupied, undefined);
                  return handle(Deferred.await(free));
                }),
              ),
            ),
          ),
        ),
      ),
    );
    const blockers = [];
    for (let i = 0; i < 8; i++)
      blockers.push(
        yield* process
          .run({ operation: "test.hold-init-permit", command: "git", cwd: f.a, args: ["status"] })
          .pipe(Effect.forkChild),
      );
    yield* Deferred.await(occupied);
    const driver = yield* Git.makeVcsDriverShape().pipe(
      Effect.provideService(VcsProcess.VcsProcess, process),
    );
    const service = yield* Provisioning.make.pipe(
      Effect.provide(
        Layer.mock(Registry.VcsDriverRegistry)({
          get: () => Deferred.succeed(entered, undefined).pipe(Effect.as(driver)),
        }),
      ),
    );
    const pending = yield* service
      .initRepository(f.authority, { cwd: f.a })
      .pipe(Effect.result, Effect.forkChild);
    yield* Deferred.await(entered);
    f.set(grantA, [AuthOrchestrationReadScope]);
    yield* Deferred.succeed(free, ChildProcessSpawner.ExitCode(0));
    yield* Effect.forEach(blockers, Fiber.join);
    const result = yield* Fiber.join(pending);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result))
      expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
    expect(spawns).toBe(8);
    f.set();
    yield* service.initRepository(f.authority, { cwd: f.a, kind: "git" });
    expect(spawns).toBe(9);
  }).pipe(Effect.provide(layer)),
);

it.effect("identity refresh cannot route a returned hidden repository root", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const commands: string[][] = [];
    const resolver = yield* Identity.make().pipe(
      Effect.provide(
        ProcessRunner.layer.pipe(
          Layer.provide(
            Layer.succeed(
              ChildProcessSpawner.ChildProcessSpawner,
              ChildProcessSpawner.make((command) =>
                Effect.sync(() => {
                  const args = [...(ChildProcess.isStandardCommand(command) ? command.args : [])];
                  commands.push(args);
                  return handle(undefined, `${f.b}\n`);
                }),
              ),
            ),
          ),
        ),
      ),
    );
    yield* denied(
      f.access.run(
        f.authority,
        { cwd: f.a },
        resolver.resolve(f.a, { refresh: true }),
        AuthSourceControlWriteScope,
      ),
    );
    expect(commands).toEqual([["-C", f.a, "rev-parse", "--show-toplevel"]]);
  }).pipe(Effect.provide(layer)),
);

const status: Git.GitStatusDetails = {
  isRepo: true,
  hasOriginRemote: true,
  isDefaultBranch: false,
  branch: "feature",
  upstreamRef: "origin/feature",
  hasWorkingTreeChanges: false,
  workingTree: { files: [], insertions: 0, deletions: 0 },
  hasUpstream: true,
  aheadCount: 1,
  behindCount: 0,
  aheadOfDefaultCount: 1,
};
it.effect("direct GitManager write-only push retains current authority across push preflight", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const entered = yield* Deferred.make<void>(),
      release = yield* Deferred.make<void>();
    let hold = false,
      pushed = 0;
    const git = yield* core(f, () =>
      Effect.sync(() => {
        pushed++;
        return handle();
      }),
    );
    const manager = yield* Manager.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(Git.GitVcsDriver)({
            statusDetails: () => Effect.succeed(status),
            readConfigValue: () => Effect.succeed(""),
            pushCurrentBranch: (cwd) =>
              (hold
                ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
                : Effect.void
              ).pipe(
                Effect.andThen(
                  git.execute({
                    operation: "test.push-final-effect",
                    cwd,
                    args: ["push", "origin", "HEAD"],
                  }),
                ),
                Effect.as({
                  status: "pushed" as const,
                  branch: "feature",
                  upstreamBranch: "origin/feature",
                  setUpstream: false,
                }),
              ),
          }),
          Layer.mock(Hosting.SourceControlProviderRegistry)({
            resolveLink: () => Effect.die("unused"),
            resolve: () =>
              Effect.succeed({
                kind: "github" as const,
                listChangeRequests: () => Effect.succeed([]),
                getDefaultBranch: () => Effect.succeed(null),
                createChangeRequest: () => Effect.void,
                getChangeRequest: () => Effect.die("unused"),
                getRepositoryCloneUrls: () => Effect.die("unused"),
                createRepository: () => Effect.die("unused"),
                checkoutChangeRequest: () => Effect.die("unused"),
              }),
          }),
          Layer.mock(TextGeneration.TextGeneration)({}),
          Layer.mock(Providers.ProviderRegistry)({}),
          Layer.mock(Setup.ProjectSetupScriptRunner)({}),
          Settings.layerTest(),
        ),
      ),
    );
    const input = {
      actionId: "write-only-push",
      action: "push" as const,
      cwd: f.worktree,
      threadId: activeA,
    };
    const result = yield* manager.runStackedAction(f.authority, input);
    expect(result.push.status).toBe("pushed");
    expect(pushed).toBe(1);
    hold = true;
    const pending = yield* manager
      .runStackedAction(f.authority, { ...input, actionId: "cut-push" })
      .pipe(Effect.result, Effect.forkChild);
    yield* Deferred.await(entered);
    f.set(grantNone);
    yield* Deferred.succeed(release, undefined);
    const cut = yield* Fiber.join(pending);
    expect(Result.isFailure(cut)).toBe(true);
    if (Result.isFailure(cut)) expect(isEnvironmentAuthorizationError(cut.failure)).toBe(true);
    expect(pushed).toBe(1);
  }).pipe(Effect.provide(layer)),
);
it.effect(
  "read-only refresh skips optional auto-pull; write-only callback and owner keep action scope",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let pulls = 0;
      const local = {
        isRepo: true,
        hasPrimaryRemote: true,
        isDefaultRef: true,
        refName: "main",
        hasWorkingTreeChanges: false,
        workingTree: { files: [], insertions: 0, deletions: 0 },
      };
      const remote = {
        defaultBranch: "main",
        isDefaultRef: true,
        refName: "main",
        upstreamRef: "origin/main",
        hasUpstream: true,
        aheadCount: 0,
        behindCount: 1,
        aheadOfDefaultCount: 0,
        pr: null,
      };
      const broadcaster = yield* Broadcaster.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(Workflow.GitWorkflowService)({
              invalidateStatus: () => Effect.void,
              invalidateLocalStatus: () => Effect.void,
              localStatus: () => Effect.succeed(local),
              remoteStatus: () => Effect.succeed(remote),
              pullCurrentBranch: (authority, cwd) =>
                f.access.run(
                  authority,
                  { cwd },
                  Effect.sync(() => {
                    pulls++;
                    return {
                      status: "pulled" as const,
                      refName: "main",
                      upstreamRef: "origin/main",
                    };
                  }),
                  AuthSourceControlWriteScope,
                ),
            }),
            Layer.mock(Background.BackgroundPolicy)({ streamChanges: Stream.empty }),
          ),
        ),
        Effect.provideService(Broadcaster.VcsAutoPullPolicy, {
          isEnabled: () => Effect.succeed(true),
        }),
      );
      f.set(grantA, [AuthOrchestrationReadScope]);
      expect((yield* broadcaster.refreshStatus(f.authority, f.a)).refName).toBe("main");
      expect(pulls).toBe(0);
      f.set();
      yield* broadcaster.refreshStatus(f.authority, f.a, AuthSourceControlWriteScope);
      expect(pulls).toBe(1);
      f.set({ version: 1, projects: { mode: "all" }, providers: { mode: "all" } }, [
        AuthOrchestrationReadScope,
        AuthSourceControlWriteScope,
      ]);
      yield* broadcaster.refreshStatus(f.authority, f.a);
      expect(pulls).toBe(2);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["createRef", "renameBranch"] as const)(
  "registry preflight denial stays typed for %s",
  (method) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let spawned = 0;
      const git = yield* core(f, () =>
        Effect.sync(() => {
          spawned++;
          return handle();
        }),
      );
      const reached = yield* Deferred.make<void>();
      const resume = yield* Deferred.make<void>();
      const service = yield* workflow(f, git, ({ cwd }) =>
        Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(resume)),
          Effect.andThen(Access.beforeWorkspaceEffect(cwd)),
          Effect.as({ kind: "git", repository: { rootPath: cwd } } as Registry.VcsDriverHandle),
        ),
      );
      const pending = yield* (
        method === "createRef"
          ? service.createRef(f.authority, { cwd: f.a, refName: "denied" }).pipe(Effect.asVoid)
          : service
              .renameBranch(f.authority, { cwd: f.a, oldBranch: "old", newBranch: "new" })
              .pipe(Effect.asVoid)
      ).pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(reached);
      f.set(grantNone);
      yield* Deferred.succeed(resume, undefined);
      const result = yield* Fiber.join(pending);
      expect(spawned).toBe(0);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(result.failure._tag).toBe("EnvironmentAuthorizationError");
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["createRef", "renameBranch", "hasCommit", "localStatus"] as const)(
  "registry %s preserves domain denial and wraps ordinary errors without effects",
  (method) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      if (method === "hasCommit" || method === "localStatus")
        f.set(grantA, [AuthOrchestrationReadScope]);
      let spawned = 0;
      const git = yield* core(f, () =>
        Effect.sync(() => {
          spawned++;
          return handle();
        }),
      );
      const ordinary = new VcsUnsupportedOperationError({
        kind: "unknown",
        operation: "detect",
        detail: "no repository",
      });
      const denial = new EnvironmentAuthorizationError({
        requiredScope: AuthSourceControlWriteScope,
        message: "denied",
      });
      for (const cause of [denial, ordinary]) {
        const service = yield* workflow(f, git, () => Effect.fail(cause));
        const operation =
          method === "createRef"
            ? service.createRef(f.authority, { cwd: f.a, refName: "ordinary" }).pipe(Effect.asVoid)
            : method === "renameBranch"
              ? service
                  .renameBranch(f.authority, { cwd: f.a, oldBranch: "old", newBranch: "new" })
                  .pipe(Effect.asVoid)
              : method === "hasCommit"
                ? service.hasCommit(f.authority, { cwd: f.a, refName: "main" }).pipe(Effect.asVoid)
                : service.localStatus(f.authority, { cwd: f.a }).pipe(Effect.asVoid);
        const error = yield* Effect.flip(operation);
        expect(spawned).toBe(0);
        if (cause === denial) expect(error).toBe(denial);
        else {
          expect(error._tag).toBe(
            method === "createRef" || method === "hasCommit"
              ? "GitCommandError"
              : "GitManagerError",
          );
          if (error._tag !== "EnvironmentAuthorizationError") expect(error.cause).toBe(cause);
        }
      }
    }).pipe(Effect.provide(layer)),
);

it.effect("workspace recovery retains denial and recovers ordinary failures only", () =>
  Effect.gen(function* () {
    let recovered = 0;
    const recover = Access.recoverWorkspaceError(() => {
      recovered++;
      return "fallback";
    });
    const denial = new EnvironmentAuthorizationError({
      requiredScope: AuthSourceControlWriteScope,
      message: "denied",
    });
    expect(yield* Effect.fail(denial).pipe(recover, Effect.flip)).toBe(denial);
    expect(recovered).toBe(0);
    expect(
      yield* Effect.fail(
        new VcsUnsupportedOperationError({
          kind: "unknown",
          operation: "detect",
          detail: "offline",
        }),
      ).pipe(recover),
    ).toBe("fallback");
    expect(recovered).toBe(1);
  }),
);

const lifecycleManager = (
  git: Partial<Git.GitVcsDriver["Service"]>,
  hosting: Partial<SourceControl.SourceControlProvider["Service"]>,
  setup?: Setup.ProjectSetupScriptRunner["Service"]["runForThread"],
  settings?: Effect.Effect<typeof DEFAULT_SERVER_SETTINGS>,
) =>
  Manager.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(Git.GitVcsDriver)(git),
        Layer.mock(Hosting.SourceControlProviderRegistry)({
          resolveLink: () => Effect.die("unused"),
          resolve: () =>
            Effect.succeed(
              hosting as Effect.Success<
                ReturnType<Hosting.SourceControlProviderRegistry["Service"]["resolve"]>
              >,
            ),
        }),
        Layer.mock(TextGeneration.TextGeneration)({}),
        Layer.mock(Providers.ProviderRegistry)({}),
        Layer.mock(Setup.ProjectSetupScriptRunner)(
          setup === undefined ? {} : { runForThread: setup },
        ),
        settings === undefined
          ? Settings.layerTest()
          : Layer.mock(Settings.ServerSettingsService)({ getSettings: settings }),
      ),
    ),
  );
it.effect("creation settings await rechecks source before project enrichment", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const entered = yield* Deferred.make<void>(),
      release = yield* Deferred.make<void>();
    let reads = 0;
    const manager = yield* lifecycleManager(
      { createWorktree: () => Effect.die("forbidden add") },
      {},
      undefined,
      Deferred.succeed(entered, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.as({
          ...DEFAULT_SERVER_SETTINGS,
          projectSettingsOverrides: { [projectA]: { worktreeSubmodules: "none" as const } },
        }),
      ),
    ).pipe(
      Effect.provideService(ProjectStore.ProjectStoreV2, {
        ...projects,
        findActiveByWorkspaceRoot: (root) => {
          reads++;
          return projects.findActiveByWorkspaceRoot(root);
        },
      }),
    );
    const pending = yield* manager
      .createWorktree(f.authority, {
        cwd: f.a,
        refName: "main",
        newRefName: "new",
        path: null,
      })
      .pipe(Effect.result, Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.worktree} WHERE project_id = ${projectA}`;
    yield* Deferred.succeed(release, undefined);
    const result = yield* Fiber.join(pending);
    expect(Result.isFailure(result)).toBe(true);
    if (Result.isFailure(result))
      expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
    expect(reads).toBe(0);
  }).pipe(Effect.provide(layer)),
);
const lifecyclePr = {
  provider: "github" as const,
  number: 1,
  title: "Lifecycle",
  url: "https://github.com/a/repo/pull/1",
  baseRefName: "main",
  headRefName: "feature",
  isDraft: false,
  state: "open" as const,
  updatedAt: Option.none(),
};

it.effect(
  "lifecycle entries deny foreign source, explicit destinations and invented registrations before routing",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let routed = 0;
      const service = yield* workflow(
        f,
        yield* core(f, () => Effect.die("forbidden Git")),
        ({ cwd }) =>
          Effect.sync(() => {
            routed++;
            return { kind: "git", repository: { rootPath: cwd } } as Registry.VcsDriverHandle;
          }),
      );
      yield* denied(service.createWorktree(f.authority, { cwd: f.b, refName: "main", path: null }));
      for (const path of [f.b, `${f.b}/new`, f.foreign, `${f.base}/unregistered`]) {
        yield* denied(service.createWorktree(f.authority, { cwd: f.a, refName: "main", path }));
        yield* denied(service.removeWorktree(f.authority, { cwd: f.a, path, force: true }));
      }
      yield* denied(
        service.preparePullRequestThread(f.authority, {
          cwd: f.a,
          threadId: activeB,
          mode: "local",
          reference: "1",
        }),
      );
      expect(routed).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["grant", "scope", "root", "membership", "destination"] as const)(
  "worktree add rechecks %s after actual checkout.workers preflight",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
      const commands: string[][] = [];
      const newPath = `${f.base}/created/a/new`;
      // The process has started; completion of this config read is the driver's preflight milestone.
      const gatedGit = yield* makeGitVcsDriverCore().pipe(
        Effect.provide(Config.layerTest(f.base, f.base)),
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make((command) =>
            Effect.gen(function* () {
              const args = [...(ChildProcess.isStandardCommand(command) ? command.args : [])];
              commands.push(args);
              if (args.includes("checkout.workers")) {
                yield* Deferred.succeed(entered, undefined);
                return handle(Deferred.await(release));
              }
              return handle();
            }),
          ),
        ),
      );
      const pending = yield* f.access
        .runWorktree(
          f.authority,
          { cwd: f.a, threadId: activeA },
          gatedGit.createWorktree(
            { cwd: f.a, refName: "main", newRefName: "new", path: null },
            { worktreesDirectory: `${f.base}/created` },
          ),
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone);
      if (cut === "scope") f.set(grantA, [AuthOrchestrationReadScope]);
      if (cut === "root")
        yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.b} WHERE project_id = ${projectA}`;
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      if (cut === "destination")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${newPath}) WHERE thread_id = ${activeB}`;
      yield* Deferred.succeed(release, ChildProcessSpawner.ExitCode(0));
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(commands.some((args) => args.includes("add"))).toBe(false);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["grant", "scope", "root", "membership", "raw-owner", "canonical-owner"] as const)(
  "claimed new path denies %s before submodule/config/result effects",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const commands: string[][] = [];
      let claimed: string | undefined;
      const git = yield* core(f, (command) =>
        Effect.sync(() => {
          const args = [...(ChildProcess.isStandardCommand(command) ? command.args : [])];
          commands.push(args);
          return handle();
        }),
      );
      const pending = yield* f.access
        .runWorktree(
          f.authority,
          { cwd: f.a, threadId: activeA },
          git.createWorktree(
            { cwd: f.a, refName: "main", newRefName: "new", baseRefName: "main", path: null },
            {
              worktreesDirectory: `${f.base}/created`,
              progress: {
                onWorktreeClaimed: (path) =>
                  Effect.gen(function* () {
                    claimed = path;
                    yield* f.fs.makeDirectory(path, { recursive: true }).pipe(Effect.orDie);
                    yield* f.fs
                      .writeFileString(`${path}/.gitmodules`, "[submodule]\n")
                      .pipe(Effect.orDie);
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                  }),
              },
            },
          ),
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone);
      if (cut === "scope") f.set(grantA, [AuthOrchestrationReadScope]);
      if (cut === "root")
        yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.b} WHERE project_id = ${projectA}`;
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      if (cut === "raw-owner")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${claimed!}) WHERE thread_id = ${activeB}`;
      if (cut === "canonical-owner") {
        const alias = `${f.base}/foreign-alias`;
        yield* f.fs.symlink(claimed!, alias);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${alias}) WHERE thread_id = ${activeB}`;
      }
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(commands.filter((args) => args.includes("add"))).toHaveLength(1);
      expect(
        commands.some(
          (args) => args.includes("submodule") || args.some((a) => a.includes("gh-merge-base")),
        ),
      ).toBe(false);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "new native worktree has exact operation claim; subsequent arbitrary external path stays unregistered",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const git = yield* makeGitVcsDriverCore().pipe(
        Effect.provide(Config.layerTest(f.base, f.base)),
      );
      yield* git.execute({ operation: "test.init", cwd: f.a, args: ["init", "-b", "main"] });
      yield* git.execute({
        operation: "test.commit",
        cwd: f.a,
        args: [
          "-c",
          "user.name=Fixture",
          "-c",
          "user.email=fixture@example.com",
          "commit",
          "--allow-empty",
          "-m",
          "initial",
        ],
      });
      const manager = yield* lifecycleManager(git, {});
      const created = yield* manager.createWorktree(
        f.authority,
        { cwd: f.a, refName: "main", newRefName: "new", path: null },
        { worktreesDirectory: `${f.base}/created` },
      );
      expect(yield* f.fs.exists(`${created.worktree.path}/.git`)).toBe(true);
      yield* denied(
        f.access.admit(f.authority, { cwd: created.worktree.path }, AuthSourceControlWriteScope),
      );
      // Registered child destinations use A's actual owner; caller cannot invent an outside claim.
      const child = yield* manager.createWorktree(f.authority, {
        cwd: f.a,
        refName: "main",
        newRefName: "child",
        path: `${f.a}/child-worktree`,
      });
      expect(yield* f.fs.exists(`${child.worktree.path}/.git`)).toBe(true);
      const alias = `${f.base}/allowed-alias`;
      yield* f.fs.symlink(f.a, alias);
      const aliased = yield* manager.createWorktree(f.authority, {
        cwd: f.a,
        refName: "main",
        newRefName: "alias-child",
        path: `${alias}/aliased-child`,
      });
      expect(yield* f.fs.exists(`${aliased.worktree.path}/.git`)).toBe(true);
      f.set({ version: 1, projects: { mode: "all" }, providers: { mode: "all" } });
      let queries = 0;
      const sql = new Proxy(f.sql, {
        apply: (target, context, args) => {
          queries++;
          return Reflect.apply(target, context, args);
        },
      });
      const ownerManager = yield* lifecycleManager(git, {}).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      const ownerPath = `${f.base}/owner-worktree`;
      yield* ownerManager.createWorktree(f.authority, {
        cwd: f.a,
        refName: "main",
        newRefName: "owner",
        path: ownerPath,
      });
      expect(queries).toBe(0);
      f.set(grantA, []);
      yield* denied(
        ownerManager.createWorktree(f.authority, {
          cwd: f.a,
          refName: "main",
          newRefName: "no-scope",
          path: null,
        }),
      );
      expect(queries).toBe(0);
      const trustedPath = `${f.base}/trusted-worktree`;
      yield* ownerManager.createWorktree(trustedReadAuthority, {
        cwd: f.a,
        refName: "main",
        newRefName: "trusted",
        path: trustedPath,
      });
      expect(queries).toBe(0);
      for (const path of [aliased.worktree.path, ownerPath, trustedPath])
        yield* git.removeWorktree({ cwd: f.a, path });
      yield* git.removeWorktree({ cwd: f.a, path: created.worktree.path });
      yield* git.removeWorktree({ cwd: f.a, path: child.worktree.path });
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["before", "after"] as const)(
  "prepare denies hidden same-repo reuse %s materialization before worktree probes/setup",
  (when) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const calls: string[] = [];
      let lists = 0;
      const manager = yield* lifecycleManager(
        {
          listRefs: () =>
            Effect.sync(() => ({
              refs:
                ++lists > (when === "before" ? 0 : 1)
                  ? [
                      {
                        name: "feature",
                        isRemote: false,
                        current: false,
                        isDefault: false,
                        worktreePath: f.foreign,
                      },
                    ]
                  : [],
              isRepo: true,
              hasPrimaryRemote: true,
              nextCursor: null,
              totalCount: 1,
            })),
          fetchPullRequestBranch: () =>
            Effect.sync(() => {
              calls.push("fetch-source");
            }),
          statusDetails: (cwd) =>
            Effect.sync(() => {
              calls.push(`status:${cwd}`);
              return status;
            }),
        },
        { kind: "github", getChangeRequest: () => Effect.succeed(lifecyclePr) },
        () =>
          Effect.sync(() => {
            calls.push("setup");
            return { status: "no-script" as const };
          }),
      );
      yield* denied(
        manager.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
        }),
      );
      expect(calls).toEqual(when === "before" ? [] : ["fetch-source"]);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "prepare predicate preserves write-only local/worktree, conditional operate, and checks returned paths",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const calls: string[] = [];
      const manager = yield* lifecycleManager(
        {
          listRefs: () =>
            Effect.succeed({
              refs: [
                {
                  name: "feature",
                  isRemote: false,
                  current: false,
                  isDefault: false,
                  worktreePath: f.worktree,
                },
              ],
              isRepo: true,
              hasPrimaryRemote: true,
              nextCursor: null,
              totalCount: 1,
            }),
          statusDetails: (cwd) =>
            Effect.sync(() => {
              calls.push(`status:${cwd}`);
              return { ...status, branch: "feature" };
            }),
          resolveCommit: () => Effect.succeed({ commitSha: "head" }),
          resolvePrimaryRemoteName: () => Effect.succeed("origin"),
          fetchRemoteTrackingBranch: () => Effect.void,
          setBranchUpstream: () => Effect.void,
          fetchPullRequestHeadCommit: () => Effect.succeed({ commitSha: "head" }),
          refreshCheckedOutBranch: () =>
            Effect.succeed({ headCommit: "head", moved: false, onTarget: true }),
        },
        {
          kind: "github",
          getChangeRequest: () => Effect.succeed(lifecyclePr),
          checkoutChangeRequest: () =>
            Effect.sync(() => {
              calls.push("checkout");
            }),
        },
      );
      expect(
        (yield* manager.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "local",
          threadId: activeA,
        })).worktreePath,
      ).toBeNull();
      expect(
        (yield* manager.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
        })).worktreePath,
      ).toBe(f.worktree);
      const count = calls.length;
      yield* denied(
        manager.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
          threadId: activeA,
        }),
      );
      expect(calls).toHaveLength(count);
      f.set(grantA, [AuthSourceControlWriteScope, AuthOrchestrationOperateScope]);
      expect(
        (yield* manager.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
          threadId: activeA,
        })).worktreePath,
      ).toBe(f.worktree);
      const returned = yield* lifecycleManager(
        {
          listRefs: () =>
            Effect.succeed({
              refs: [],
              isRepo: true,
              hasPrimaryRemote: true,
              nextCursor: null,
              totalCount: 0,
            }),
          fetchPullRequestBranch: () => Effect.void,
          createWorktree: () =>
            Effect.succeed({ worktree: { path: f.foreign, refName: "feature" } }),
          statusDetails: () => Effect.die("forbidden result probe"),
        },
        { kind: "github", getChangeRequest: () => Effect.succeed(lifecyclePr) },
      );
      yield* denied(
        returned.preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
        }),
      );
    }).pipe(Effect.provide(layer)),
);

const setupRunner = (
  f: Effect.Success<typeof fixture>,
  settings: Effect.Effect<typeof DEFAULT_SERVER_SETTINGS>,
  terminal: Partial<Terminals.TerminalManager["Service"]>,
) =>
  Setup.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(Projects.ProjectService, f.records.projectService),
        Layer.mock(Settings.ServerSettingsService)({ getSettings: settings }),
        Layer.mock(Terminals.TerminalManager)(terminal),
      ),
    ),
  );
const setupScript = [
  {
    id: "setup",
    name: "Setup",
    command: "printf ready",
    icon: "configure",
    runOnWorktreeCreate: true,
  },
];
const terminalSnapshot = (input: Parameters<Terminals.TerminalManager["Service"]["open"]>[0]) => ({
  threadId: input.threadId,
  terminalId: input.terminalId,
  cwd: input.cwd,
  worktreePath: input.worktreePath ?? null,
  status: "running" as const,
  pid: 1,
  history: "",
  exitCode: null,
  exitSignal: null,
  label: "Shell",
  updatedAt: "2026-10-11T00:00:00Z",
});

it.effect.each(["grant", "scope", "root", "membership"] as const)(
  "setup rechecks %s after settings await before opening terminal",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthOrchestrationOperateScope]);
      yield* f.sql`UPDATE projection_projects SET scripts_json = ${JSON.stringify(setupScript)} WHERE project_id = ${projectA}`;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let opened = 0,
        written = 0;
      const runner = yield* setupRunner(
        f,
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(DEFAULT_SERVER_SETTINGS),
        ),
        {
          open: (input) =>
            Effect.sync(() => {
              opened++;
              return terminalSnapshot(input);
            }),
          write: () =>
            Effect.sync(() => {
              written++;
            }),
        },
      );
      const pending = yield* runner
        .runForThread(f.authority, { threadId: activeA, projectCwd: f.a, worktreePath: f.worktree })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone, [AuthOrchestrationOperateScope]);
      if (cut === "scope") f.set(grantA, [AuthSourceControlWriteScope]);
      if (cut === "root")
        yield* f.sql`UPDATE projection_projects SET workspace_root = ${f.b} WHERE project_id = ${projectA}`;
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect([opened, written]).toEqual([0, 0]);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "setup keeps operate-only access and rechecks after open before write; forged project hint has no lookup",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthOrchestrationOperateScope]);
      yield* f.sql`UPDATE projection_projects SET scripts_json = ${JSON.stringify(setupScript)} WHERE project_id = ${projectA}`;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let hold = false,
        opened = 0,
        written = 0;
      const runner = yield* setupRunner(f, Effect.succeed(DEFAULT_SERVER_SETTINGS), {
        open: (input) =>
          Effect.gen(function* () {
            opened++;
            if (hold) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return terminalSnapshot(input);
          }),
        write: () =>
          Effect.sync(() => {
            written++;
          }),
      });
      const input = { threadId: activeA, projectCwd: f.a, worktreePath: f.worktree };
      expect((yield* runner.runForThread(f.authority, input)).status).toBe("started");
      expect([opened, written]).toEqual([1, 1]);
      f.records.enrichedRoots.length = 0;
      yield* denied(runner.runForThread(f.authority, { ...input, projectId: projectB }));
      expect(f.records.enrichedRoots).toEqual([]);
      hold = true;
      const pending = yield* runner
        .runForThread(f.authority, input)
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      f.set(grantNone, [AuthOrchestrationOperateScope]);
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect([opened, written]).toEqual([2, 1]);
    }).pipe(Effect.provide(layer)),
);

it.effect("setup may use only current operation's exact claimed new path", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.set(grantA, [AuthSourceControlWriteScope, AuthOrchestrationOperateScope]);
    yield* f.sql`UPDATE projection_projects SET scripts_json = ${JSON.stringify(setupScript)} WHERE project_id = ${projectA}`;
    let opened = 0,
      written = 0;
    const runner = yield* setupRunner(f, Effect.succeed(DEFAULT_SERVER_SETTINGS), {
      open: (input) =>
        Effect.sync(() => {
          opened++;
          return terminalSnapshot(input);
        }),
      write: () =>
        Effect.sync(() => {
          written++;
        }),
    });
    const path = `${f.base}/new-owned`;
    const run = f.access.runWorktree(
      f.authority,
      { cwd: f.a, threadId: activeA },
      Effect.gen(function* () {
        const lifecycle = (yield* Access.worktreeEffectAdmission)!;
        yield* lifecycle.prepare(path, true);
        yield* f.fs.makeDirectory(path);
        yield* lifecycle.claim(path);
        return yield* runner.runForThread(f.authority, {
          threadId: activeA,
          projectCwd: f.a,
          worktreePath: path,
        });
      }),
      { additionalScope: AuthOrchestrationOperateScope },
    );
    expect((yield* run).status).toBe("started");
    yield* denied(
      runner.runForThread(f.authority, { threadId: activeA, projectCwd: f.a, worktreePath: path }),
    );
    yield* denied(
      runner.runForThread(f.authority, {
        threadId: activeB,
        projectCwd: f.a,
        worktreePath: f.foreign,
      }),
    );
    expect([opened, written]).toEqual([1, 1]);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "cancel checks actual durable and snapshot owners even with nothing running; allowed operate interrupts",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthOrchestrationOperateScope]);
      const tracker = yield* Tracker.make;
      yield* denied(tracker.cancel(f.authority, activeB));
      expect(yield* tracker.cancel(f.authority, activeA)).toBe(false);
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${f.foreign}) WHERE thread_id = ${activeA}`;
      yield* denied(tracker.cancel(f.authority, activeA));
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${f.worktree}) WHERE thread_id = ${activeA}`;
      let interrupted = 0;
      const running = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            interrupted++;
          }),
        ),
        Effect.forkChild,
      );
      yield* tracker.begin({
        threadId: activeA,
        branch: null,
        baseRef: null,
        stages: ["checkout"],
        fiber: running,
      });
      yield* tracker.update(activeA, (s) => ({ ...s, worktreePath: f.foreign }));
      yield* denied(tracker.cancel(f.authority, activeA));
      expect(interrupted).toBe(0);
      yield* tracker.update(activeA, (s) => ({ ...s, worktreePath: f.worktree }));
      expect(yield* tracker.cancel(f.authority, activeA)).toBe(true);
      expect(interrupted).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["grant", "scope", "membership"] as const)(
  "cancel rechecks %s after canonical await before interrupt",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthOrchestrationOperateScope]);
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let hold = false,
        interrupted = 0;
      const fs = FileSystem.FileSystem.of({
        ...f.fs,
        realPath: (path) =>
          (hold && path === f.worktree
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void
          ).pipe(Effect.andThen(f.fs.realPath(path))),
      });
      const tracker = yield* Tracker.make.pipe(Effect.provideService(FileSystem.FileSystem, fs));
      const running = yield* Effect.never.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            interrupted++;
          }),
        ),
        Effect.forkChild,
      );
      yield* tracker.begin({
        threadId: activeA,
        branch: null,
        baseRef: null,
        stages: ["checkout"],
        fiber: running,
      });
      hold = true;
      const pending = yield* tracker
        .cancel(f.authority, activeA)
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      if (cut === "grant") f.set(grantNone, [AuthOrchestrationOperateScope]);
      if (cut === "scope") f.set(grantA, [AuthSourceControlWriteScope]);
      if (cut === "membership")
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET project_id = ${projectB} WHERE thread_id = ${activeA}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(interrupted).toBe(0);
      yield* Fiber.interrupt(running);
    }).pipe(Effect.provide(layer)),
);

it.effect("prune admits every actual worktree including detached/stale paths before mutation", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const commands: string[][] = [];
    let hidden = false;
    const git = yield* core(f, (command) =>
      Effect.sync(() => {
        const args = [...(ChildProcess.isStandardCommand(command) ? command.args : [])];
        commands.push(args);
        return handle(
          undefined,
          args.includes("--porcelain")
            ? `worktree ${f.a}\0HEAD abc\0branch refs/heads/main\0\0worktree ${hidden ? f.foreign : f.worktree}\0detached\0prunable missing directory\0\0`
            : "",
        );
      }),
    );
    const service = yield* workflow(f, git);
    yield* service.pruneWorktrees(f.authority, { cwd: f.a });
    expect(commands.filter((args) => args.includes("prune"))).toHaveLength(1);
    hidden = true;
    yield* denied(service.pruneWorktrees(f.authority, { cwd: f.a }));
    expect(commands.filter((args) => args.includes("prune"))).toHaveLength(1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "remove rechecks target registration after actual routing await; missing registered path stays usable",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const commands: string[][] = [];
      let hold = false;
      const service = yield* workflow(
        f,
        yield* core(f, (command) =>
          Effect.sync(() => {
            commands.push([...(ChildProcess.isStandardCommand(command) ? command.args : [])]);
            return handle();
          }),
        ),
        ({ cwd }) =>
          (hold
            ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void
          ).pipe(
            Effect.as({ kind: "git", repository: { rootPath: cwd } } as Registry.VcsDriverHandle),
          ),
      );
      yield* f.fs.remove(f.worktree, { recursive: true });
      yield* service.removeWorktree(f.authority, { cwd: f.a, path: f.worktree });
      expect(commands.filter((args) => args.includes("remove"))).toHaveLength(1);
      hold = true;
      const pending = yield* service
        .removeWorktree(f.authority, { cwd: f.a, path: f.worktree, force: true })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${f.worktree}) WHERE thread_id = ${activeB}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(commands.filter((args) => args.includes("remove"))).toHaveLength(1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["allow", "grant", "canonical-owner"] as const)(
  "creation SQL admission %s after pre-commit await; publication follows durable binding",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const tracker = yield* Tracker.make;
      yield* tracker.begin({
        threadId: activeA,
        branch: null,
        baseRef: "main",
        stages: ["checkout"],
        fiber: null,
      });
      let path = "",
        inTransaction = false,
        transactionFs = 0;
      const fs = FileSystem.FileSystem.of({
        ...f.fs,
        realPath: (path) =>
          Effect.sync(() => {
            if (inTransaction) transactionFs++;
          }).pipe(Effect.andThen(f.fs.realPath(path))),
      });
      const access = yield* Access.make.pipe(Effect.provideService(FileSystem.FileSystem, fs));
      const git = yield* core(f, () => Effect.succeed(handle()));
      const pending = yield* access
        .runWorktree(
          f.authority,
          { cwd: f.a },
          git.createWorktree(
            { cwd: f.a, refName: "main", newRefName: "binding", path: null },
            {
              worktreesDirectory: `${f.base}/created`,
              progress: {
                onWorktreeClaimed: (created) =>
                  Effect.sync(() => {
                    path = created;
                  }).pipe(
                    Effect.andThen(
                      f.fs.makeDirectory(created, { recursive: true }).pipe(Effect.orDie),
                    ),
                  ),
              },
            },
          ),
          {
            afterSuccess: (admission) =>
              Effect.gen(function* () {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
                yield* f.sql
                  .withTransaction(
                    Effect.gen(function* () {
                      inTransaction = true;
                      yield* admission.checkSql;
                      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${path}) WHERE thread_id = ${activeA}`;
                    }).pipe(
                      Effect.ensuring(
                        Effect.sync(() => {
                          inTransaction = false;
                        }),
                      ),
                    ),
                  )
                  .pipe(
                    Effect.mapError((error) =>
                      isEnvironmentAuthorizationError(error)
                        ? error
                        : new EnvironmentAuthorizationError({
                            requiredScope: AuthSourceControlWriteScope,
                            message: "Test transaction failed.",
                          }),
                    ),
                  );
                yield* tracker.update(activeA, (snapshot) => ({ ...snapshot, worktreePath: path }));
              }),
          },
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      const reader: ThreadReadAuthority = {
        current: Effect.succeed({ resourceGrant: grantA, scopes: [AuthOrchestrationReadScope] }),
      };
      expect((yield* tracker.get(reader, activeA))?.worktreePath).toBeNull();
      if (cut === "grant") f.set(grantNone);
      if (cut === "canonical-owner") {
        const alias = `${f.base}/late-alias`;
        yield* f.fs.symlink(path, alias);
        yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${alias}) WHERE thread_id = ${activeB}`;
      }
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      const bound = (yield* f.sql<{
        readonly path: string;
      }>`SELECT json_extract(payload_json, '$.worktreePath') AS path FROM orchestration_v2_projection_threads WHERE thread_id = ${activeA}`)[0]!
        .path;
      if (cut === "allow") {
        expect(Result.isSuccess(result)).toBe(true);
        expect(bound).toBe(path);
        expect((yield* tracker.get(reader, activeA))?.worktreePath).toBe(path);
      } else {
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result))
          expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
        expect(bound).toBe(f.worktree);
        expect((yield* tracker.get(reader, activeA))?.worktreePath).toBeNull();
      }
      expect(transactionFs).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["materialize", "configure", "reuse", "setup"] as const)(
  "prepare retains typed denial from %s without alternate effects",
  (stage) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthSourceControlWriteScope, AuthOrchestrationOperateScope]);
      const failure = new EnvironmentAuthorizationError({
        requiredScope: AuthSourceControlWriteScope,
        message: "Denied test effect.",
      });
      let fallback = 0,
        setup = 0;
      const manager = yield* lifecycleManager(
        {
          listRefs: () =>
            Effect.succeed({
              refs:
                stage === "materialize"
                  ? []
                  : [
                      {
                        name: "feature",
                        isRemote: false,
                        current: false,
                        isDefault: false,
                        worktreePath: f.worktree,
                      },
                    ],
              isRepo: true,
              hasPrimaryRemote: true,
              nextCursor: null,
              totalCount: 1,
            }),
          statusDetails: () => Effect.succeed({ ...status, branch: "feature" }),
          resolveCommit: () => Effect.succeed({ commitSha: "head" }),
          readConfigValue: () => Effect.succeed(""),
          resolvePrimaryRemoteName: () => Effect.succeed("origin"),
          ensureRemote: () => Effect.succeed("origin"),
          fetchRemoteTrackingBranch: () =>
            stage === "configure" ? Effect.fail(failure) : Effect.void,
          setBranchUpstream: () => Effect.void,
          fetchPullRequestBranch: () =>
            Effect.sync(() => {
              fallback++;
            }),
          fetchRemoteBranch: () => Effect.fail(failure),
          fetchPullRequestHeadCommit: () =>
            stage === "reuse" ? Effect.fail(failure) : Effect.succeed({ commitSha: "head" }),
          refreshCheckedOutBranch: () =>
            Effect.succeed({ headCommit: "head", moved: true, onTarget: true }),
        },
        {
          kind: "github",
          getChangeRequest: () =>
            Effect.succeed({
              ...lifecyclePr,
              ...(stage === "materialize" ? { headRepositoryNameWithOwner: "a/repo" } : {}),
            }),
          getRepositoryCloneUrls: () =>
            Effect.succeed({
              nameWithOwner: "a/repo",
              url: "https://github.com/a/repo",
              sshUrl: "git@github.com:a/repo",
            }),
        },
        () =>
          Effect.sync(() => {
            setup++;
          }).pipe(Effect.andThen(Effect.fail(failure))),
      );
      const result = yield* manager
        .preparePullRequestThread(f.authority, {
          cwd: f.a,
          reference: "1",
          mode: "worktree",
          threadId: activeA,
        })
        .pipe(Effect.result);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(result.failure).toBe(failure);
      expect(fallback).toBe(0);
      expect(setup).toBe(stage === "setup" ? 1 : 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "setup rechecks current authority after completion-token crypto await before terminal effect",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.set(grantA, [AuthOrchestrationOperateScope]);
      yield* f.sql`UPDATE projection_projects SET scripts_json = ${JSON.stringify(setupScript)} WHERE project_id = ${projectA}`;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let opened = 0;
      const crypto = yield* Crypto.Crypto;
      const runner = yield* setupRunner(f, Effect.succeed(DEFAULT_SERVER_SETTINGS), {
        open: (input) =>
          Effect.sync(() => {
            opened++;
            return terminalSnapshot(input);
          }),
      }).pipe(
        Effect.provideService(Crypto.Crypto, {
          ...crypto,
          randomUUIDv4: Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
            Effect.as("8abb5c67-8327-4a97-b27f-2e332bd62cad"),
          ),
        }),
      );
      const pending = yield* runner
        .runForThread(f.authority, {
          threadId: activeA,
          projectCwd: f.a,
          worktreePath: f.worktree,
          observeCompletion: {},
        })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      f.set(grantNone, [AuthOrchestrationOperateScope]);
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      expect(opened).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["unrelated", "raw-overlap", "canonical-overlap"] as const)(
  "creation re-evaluates %s registration change across final canonical await",
  (change) =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const path = `${f.base}/new-operation`,
        unrelated = `${f.base}/unrelated-b`,
        alias = `${f.base}/alias-new-operation`;
      yield* f.fs.makeDirectory(path);
      yield* f.fs.makeDirectory(unrelated);
      yield* f.fs.symlink(path, alias);
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      let armed = false,
        reads = 0,
        effects = 0;
      const fs = FileSystem.FileSystem.of({
        ...f.fs,
        realPath: (requested) =>
          Effect.gen(function* () {
            if (armed && requested === path && ++reads === 2) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return yield* f.fs.realPath(requested);
          }),
      });
      const access = yield* Access.make.pipe(Effect.provideService(FileSystem.FileSystem, fs));
      const pending = yield* access
        .runWorktree(
          f.authority,
          { cwd: f.a, threadId: activeA },
          Effect.gen(function* () {
            const lifecycle = (yield* Access.worktreeEffectAdmission)!;
            yield* lifecycle.prepare(path, true);
            yield* lifecycle.claim(path);
            armed = true;
            yield* Access.beforeWorkspaceEffect(f.a);
            effects++;
          }),
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      const root = change === "unrelated" ? unrelated : change === "raw-overlap" ? path : alias;
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${root}) WHERE thread_id = ${activeB}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      if (change === "unrelated") expect(Result.isSuccess(result)).toBe(true);
      else {
        expect(Result.isFailure(result)).toBe(true);
        if (Result.isFailure(result))
          expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
      }
      expect(effects).toBe(change === "unrelated" ? 1 : 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "reused result rechecks actual owner after success callback await before publication",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>(),
        release = yield* Deferred.make<void>();
      const manager = yield* lifecycleManager(
        {
          listRefs: () =>
            Effect.succeed({
              refs: [
                {
                  name: "feature",
                  isRemote: false,
                  current: false,
                  isDefault: false,
                  worktreePath: f.worktree,
                },
              ],
              isRepo: true,
              hasPrimaryRemote: true,
              nextCursor: null,
              totalCount: 1,
            }),
          statusDetails: () => Effect.succeed({ ...status, branch: "feature" }),
          resolveCommit: () => Effect.succeed({ commitSha: "head" }),
          resolvePrimaryRemoteName: () => Effect.succeed("origin"),
          fetchRemoteTrackingBranch: () => Effect.void,
          setBranchUpstream: () => Effect.void,
          fetchPullRequestHeadCommit: () => Effect.succeed({ commitSha: "head" }),
          refreshCheckedOutBranch: () =>
            Effect.succeed({ headCommit: "head", moved: false, onTarget: true }),
        },
        { kind: "github", getChangeRequest: () => Effect.succeed(lifecyclePr) },
      );
      const service = yield* Workflow.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.succeed(Manager.GitManager, manager),
            Layer.mock(Git.GitVcsDriver)({}),
            Layer.mock(Registry.VcsDriverRegistry)({
              resolve: ({ cwd }) =>
                Effect.succeed({
                  kind: "git",
                  repository: { rootPath: cwd },
                } as Registry.VcsDriverHandle),
            }),
          ),
        ),
      );
      const pending = yield* service
        .preparePullRequestThread(f.authority, { cwd: f.a, reference: "1", mode: "worktree" }, () =>
          Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        )
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(entered);
      yield* f.sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json, '$.worktreePath', ${f.worktree}) WHERE thread_id = ${activeB}`;
      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result))
        expect(isEnvironmentAuthorizationError(result.failure)).toBe(true);
    }).pipe(Effect.provide(layer)),
);
