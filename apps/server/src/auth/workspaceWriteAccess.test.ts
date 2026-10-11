import * as Settings from "../serverSettings.ts";
import * as Hosting from "../sourceControl/SourceControlProviderRegistry.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as Setup from "../project/ProjectSetupScriptRunner.ts";
import * as Broadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as Background from "../background/BackgroundPolicy.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthSourceControlWriteScope,
  AuthOrchestrationReadScope,
  EnvironmentAuthorizationError,
  SourceControlProviderError as SourceControlProviderFailure,
  ThreadId,
  type AuthEnvironmentScope,
  type AuthResourceGrant,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
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
  yield* makeCollectionFixture;
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
  return { sql, fs, base, a, b, worktree, foreign, authority, set, access };
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
