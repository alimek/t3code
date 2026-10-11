import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  type AuthEnvironmentScope,
  type AuthResourceGrant,
  EnvironmentAuthorizationError,
  type SourceControlProviderError,
  ServerSettingsError,
} from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as Access from "../auth/workspaceReadAccess.ts";
import {
  collectionStoreLayer,
  grantA,
  grantNone,
  makeCollectionFixture,
  projectA,
  projectB,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";
import * as Settings from "../serverSettings.ts";
import * as GitHubApi from "../sourceControl/GitHubApi.ts";
import * as Credentials from "../sourceControl/GitHubCredentials.ts";
import * as Forgejo from "../sourceControl/ForgejoCli.ts";
import * as Bitbucket from "../sourceControl/BitbucketApi.ts";
import * as BitbucketPR from "./BitbucketPullRequestApi.ts";
import * as Quota from "../sourceControl/githubQuota.ts";
import * as Hosting from "../sourceControl/GitHubSourceControlProvider.ts";
import * as Limits from "../sourceControl/SourceControlRateLimit.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as PullRequests from "./GitHubPullRequestApi.ts";
import * as ConditionalChecks from "./gitHubConditionalChecks.ts";
import * as GitLab from "../sourceControl/GitLabCli.ts";
import * as Azure from "../sourceControl/AzureDevOpsCli.ts";
import * as Registry from "../vcs/VcsDriverRegistry.ts";
import * as ForgejoProvider from "./ForgejoPullRequestProvider.ts";

const layer = Layer.mergeAll(NodeServices.layer, collectionStoreLayer);
const fixture = (scope: AuthEnvironmentScope = AuthOrchestrationReadScope) =>
  Effect.gen(function* () {
    yield* makeCollectionFixture;
    const sql = yield* SqlClient.SqlClient;
    const fs = yield* FileSystem.FileSystem;
    const base = yield* fs.makeTempDirectoryScoped({ prefix: "t3-pr-read-effects-" });
    const a = `${base}/a`,
      b = `${base}/b`;
    for (const root of [a, b]) yield* fs.makeDirectory(`${root}/.git`, { recursive: true });
    yield* sql`UPDATE projection_projects SET workspace_root = ${a} WHERE project_id = ${projectA}`;
    yield* sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`;
    let current: { resourceGrant: AuthResourceGrant; scopes: ReadonlyArray<AuthEnvironmentScope> } =
      {
        resourceGrant: { ...grantA, projects: { mode: "explicit", ids: [projectB] } },
        scopes: [scope],
      };
    const authority: ThreadReadAuthority = { current: Effect.sync(() => current) };
    const aAuthority: ThreadReadAuthority = {
      current: Effect.succeed({
        resourceGrant: grantA,
        scopes: [scope],
      }),
    };
    const access = yield* Access.make;
    const cut = (kind: "grant" | "root" | "membership" | "action") => {
      if (kind === "root")
        return sql`UPDATE projection_projects SET workspace_root = ${`${base}/moved`} WHERE project_id = ${projectB}`.pipe(
          Effect.asVoid,
        );
      if (kind === "membership")
        return sql`UPDATE projection_projects SET deleted_at = '2026-10-11T00:00:00Z' WHERE project_id = ${projectB}`.pipe(
          Effect.asVoid,
        );
      return Effect.sync(() => {
        current =
          kind === "grant" ? { ...current, resourceGrant: grantNone } : { ...current, scopes: [] };
      });
    };
    return {
      a,
      b,
      access,
      authority,
      aAuthority,
      cut,
      restore:
        sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`.pipe(
          Effect.asVoid,
        ),
    };
  });
const admitted = <E, R>(
  effect: Effect.Effect<unknown, E, R>,
  admission: Access.WorkspaceActionAdmission,
) => effect.pipe(Effect.provideService(Access.workspaceEffectAdmission, admission));
const expectDenied = <A, E>(result: Result.Result<A, E>) => {
  expect(Result.isFailure(result)).toBe(true);
  if (Result.isFailure(result))
    expect(result.failure).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
};
const output = (stdout: string, exitCode = 0): VcsProcess.VcsProcessOutput => ({
  exitCode: ChildProcessSpawner.ExitCode(exitCode),
  stdout,
  stderr: "",
  stdoutTruncated: false,
  stderrTruncated: false,
});
const summary = (number: number) => ({
  number,
  title: `PR ${number}`,
  url: `https://github.com/acme/web/pull/${number}`,
  author: null,
  baseRefName: "main",
  headRefName: `feat/${number}`,
  state: "OPEN",
  isDraft: false,
  mergeable: "MERGEABLE",
  reviewDecision: null,
  latestReviews: { nodes: [] },
  additions: 1,
  deletions: 0,
  changedFiles: 1,
  updatedAt: "2026-10-11T00:00:00Z",
  mergedAt: null,
  closedAt: null,
  commits: { nodes: [] },
});
const fingerprint = {
  state: "OPEN",
  mergeable: "MERGEABLE",
  headRefOid: "abc123",
  comments: { totalCount: 0, nodes: [] },
  reviews: { totalCount: 0, nodes: [] },
  reviewThreads: { totalCount: 0 },
  commits: { nodes: [] },
};
const github = (waitForCredential: Effect.Effect<void>) =>
  Effect.gen(function* () {
    const documents: Array<Record<string, unknown>> = [];
    const calls: string[] = [];
    const api = yield* GitHubApi.make.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(Credentials.GitHubCredentials)({
            get: (host) =>
              waitForCredential.pipe(
                Effect.as({
                  host,
                  token: Redacted.make("fixture-token"),
                  source: "gh" as const,
                  fingerprint: "same-account",
                }),
              ),
            invalidate: () =>
              Effect.sync(() => {
                calls.push("invalidate");
              }),
          }),
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.sync(() => {
                calls.push(request.url);
                if (request.url.endsWith("/user"))
                  return HttpClientResponse.fromWeb(
                    request,
                    new Response(JSON.stringify({ id: 123, login: "same-account" })),
                  );
                if (request.body._tag !== "Uint8Array")
                  throw new Error("Expected a GraphQL document");
                const { query, variables } = JSON.parse(
                  new TextDecoder().decode(request.body.body),
                ) as { query: string; variables: Record<string, unknown> };
                documents.push(variables);
                const data: Record<string, unknown> = {};
                for (const [key, value] of Object.entries(variables)) {
                  if (key.endsWith("_number"))
                    data[key.slice(0, -7)] = {
                      pullRequest: query.includes("PullRequestWatchFingerprints")
                        ? fingerprint
                        : summary(Number(value)),
                    };
                }
                if (query.includes("PullRequestsByHead"))
                  data.repository = { h0: { nodes: [] }, h1: { nodes: [] } };
                return HttpClientResponse.fromWeb(request, new Response(JSON.stringify({ data })));
              }),
            ),
          ),
          Quota.layer,
          Limits.layer,
        ),
      ),
    );
    const process = Layer.mock(VcsProcess.VcsProcess)({
      run: (input) =>
        Effect.succeed(
          output(
            input.args[0] === "remote" ? "origin git@github.com:acme/web.git (fetch)" : "",
            input.args[0] === "config" ? 1 : 0,
          ),
        ),
    });
    const cli = yield* PullRequests.make.pipe(
      Effect.provideService(GitHubApi.GitHubApi, api),
      Effect.provide(process),
    );
    const hosting = yield* Hosting.make.pipe(
      Effect.provideService(GitHubApi.GitHubApi, api),
      Effect.provide(Layer.mergeAll(process, Layer.mock(Git.GitVcsDriver)({}))),
      Effect.provideService(HostProcessEnvironment, {}),
    );
    return { api, cli, hosting, documents, calls };
  });

it.effect.each(
  (["summary", "fingerprint", "head"] as const).flatMap((method) =>
    (["grant", "root", "membership", "action"] as const).map((cut) => ({ method, cut })),
  ),
)(
  "$method: same-credential B $cut cut during credential preflight never reaches the document",
  ({ method, cut }) =>
    Effect.gen(function* () {
      const f = yield* fixture(
        method === "head" ? AuthSourceControlWriteScope : AuthOrchestrationReadScope,
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const g = yield* github(
        Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
      );
      const scope = method === "head" ? AuthSourceControlWriteScope : AuthOrchestrationReadScope;
      const a = yield* f.access.capture(f.aAuthority, { cwd: f.a, projectId: projectA }, scope);
      const b = yield* f.access.capture(f.authority, { cwd: f.b, projectId: projectB }, scope);
      const read = (
        cwd: string,
        number: number,
      ): Effect.Effect<
        unknown,
        | PullRequests.GitHubPullRequestApiError
        | SourceControlProviderError
        | EnvironmentAuthorizationError
      > =>
        method === "head"
          ? g.hosting.listChangeRequests({
              cwd,
              headSelector: `feature-${number}`,
              state: "open",
            })
          : method === "summary"
            ? g.cli.getPullRequestSummary({
                cwd,
                repository: "acme/web",
                host: "github.com",
                number,
              })
            : g.cli.getPullRequestWatchFingerprint({
                cwd,
                repository: "acme/web",
                host: "github.com",
                number,
              });
      const first = yield* admitted(read(f.a, 1), a).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      const second = yield* admitted(read(f.b, 2), b).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* TestClock.adjust(method === "head" ? "50 millis" : "10 millis");
      yield* Deferred.await(started);
      yield* f.cut(cut);
      yield* Deferred.succeed(release, undefined);
      expect(Result.isSuccess(yield* Fiber.join(first))).toBe(true);
      expectDenied(yield* Fiber.join(second));
      expect(g.documents).toHaveLength(1);
      expect(Object.values(g.documents[0]!)).not.toContain(2);
      expect(Object.values(g.documents[0]!)).not.toContain("feature-2");
      expect(g.calls).toHaveLength(1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["summary", "fingerprint", "head"] as const)(
  "%s: external B cannot borrow trusted A through the batching delay",
  (method) =>
    Effect.gen(function* () {
      const f = yield* fixture(
        method === "head" ? AuthSourceControlWriteScope : AuthOrchestrationReadScope,
      );
      const g = yield* github(Effect.void);
      const scope = method === "head" ? AuthSourceControlWriteScope : AuthOrchestrationReadScope;
      const a = yield* f.access.capture(trustedReadAuthority, { cwd: f.a }, scope);
      const b = yield* f.access.capture(f.authority, { cwd: f.b, projectId: projectB }, scope);
      const read = (
        cwd: string,
        number: number,
      ): Effect.Effect<
        unknown,
        | PullRequests.GitHubPullRequestApiError
        | SourceControlProviderError
        | EnvironmentAuthorizationError
      > =>
        method === "head"
          ? g.hosting.listChangeRequests({ cwd, headSelector: `feature-${number}`, state: "open" })
          : method === "summary"
            ? g.cli.getPullRequestSummary({
                cwd,
                repository: "acme/web",
                host: "github.com",
                number,
              })
            : g.cli.getPullRequestWatchFingerprint({
                cwd,
                repository: "acme/web",
                host: "github.com",
                number,
              });
      const first = yield* admitted(read(f.a, 1), a).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      const second = yield* admitted(read(f.b, 2), b).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* f.cut("grant");
      yield* TestClock.adjust(method === "head" ? "50 millis" : "10 millis");
      expect(Result.isSuccess(yield* Fiber.join(first))).toBe(true);
      expectDenied(yield* Fiber.join(second));
      expect(g.documents).toHaveLength(1);
      expect(Object.values(g.documents[0]!)).not.toContain(2);
      expect(Object.values(g.documents[0]!)).not.toContain("feature-2");
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["host settings", "saved token settings", "warm token"] as const)(
  "GitHubCredentials: %s wait cannot reach a credential after a current root cut",
  (stage) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const settings = yield* Settings.ServerSettingsService.pipe(
        Effect.provide(Settings.ServerSettingsService.layerTest()),
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let hold = stage !== "warm token",
        reads = 0,
        commands = 0,
        tokenReads = 0;
      const snapshot = yield* settings.getSettings;
      const tokens = Object.defineProperty({}, "github.com", {
        get: () => {
          tokenReads++;
          return undefined;
        },
      });
      const credentials = yield* Credentials.make.pipe(
        Effect.provideService(HostProcessEnvironment, {}),
        Effect.provideService(Settings.ServerSettingsService, {
          ...settings,
          getSettings: Effect.suspend(() => {
            reads++;
            const wait = hold && (stage === "saved token settings" ? reads === 2 : true);
            return (
              wait
                ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
                : Effect.void
            ).pipe(Effect.as({ ...snapshot, github: { ...snapshot.github, tokens } }));
          }),
        }),
        Effect.provide(
          Layer.mock(VcsProcess.VcsProcess)({
            run: () =>
              Access.beforeCurrentWorkspaceEffect.pipe(
                Effect.andThen(
                  Effect.sync(() => {
                    commands++;
                    return output("fixture-token");
                  }),
                ),
              ),
          }),
        ),
      );
      if (stage === "warm token") {
        yield* admitted(credentials.get("github.com"), admission);
        hold = true;
        tokenReads = 0;
      }
      const pending = yield* admitted(credentials.get("github.com"), admission).pipe(
        Effect.result,
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      yield* f.cut("root");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      expect(commands).toBe(stage === "warm token" ? 1 : 0);
      expect(tokenReads).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.effect("GitHubApi: a pinned credential cannot skip current action admission", () =>
  Effect.gen(function* () {
    const f = yield* fixture(AuthSourceControlWriteScope);
    const admission = yield* f.access.capture(
      f.authority,
      { cwd: f.b, projectId: projectB },
      AuthSourceControlWriteScope,
    );
    const g = yield* github(Effect.void);
    yield* f.cut("action");
    const result = yield* admitted(
      g.api.credential("github.com").pipe(
        Effect.provideService(GitHubApi.PinnedGitHubCredential, {
          host: "github.com",
          token: Redacted.make("fixture-token"),
          credentialFingerprint: "same-account",
        }),
      ),
      admission,
    ).pipe(Effect.result);
    expectDenied(result);
    expect(g.calls).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["settings", "gh"] as const)(
  "GitHubCredentials: %s fingerprint await cannot publish a denied token",
  (source) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let commands = 0,
        hold = true;
      const crypto = yield* Crypto.Crypto;
      const credentials = yield* Credentials.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Settings.ServerSettingsService.layerTest({
              github: {
                tokens: source === "settings" ? { "github.com": "fixture-token" } : {},
                hosts: {},
              },
            }),
            Layer.mock(VcsProcess.VcsProcess)({
              run: () =>
                Effect.sync(() => {
                  commands++;
                  return output("fixture-token");
                }),
            }),
          ),
        ),
        Effect.provideService(HostProcessEnvironment, {}),
        Effect.provideService(Crypto.Crypto, {
          ...crypto,
          digest: (algorithm, value) =>
            Effect.suspend(() =>
              (hold
                ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release)))
                : Effect.void
              ).pipe(Effect.andThen(crypto.digest(algorithm, value))),
            ),
        }),
      );
      const pending = yield* admitted(credentials.get("github.com"), admission).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(started);
      yield* f.cut("root");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      yield* f.restore;
      hold = false;
      yield* admitted(credentials.get("github.com"), admission);
      expect(commands).toBe(source === "gh" ? 2 : 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["grant", "root", "membership", "action"] as const)(
  "GitHub identity lock: queued B %s cut cannot read A's cached identity",
  (cut) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const queued = yield* Deferred.make<void>();
      let http = 0,
        bChecks = 0;
      const cli = yield* PullRequests.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(GitHubApi.GitHubApi)({
              credential: () =>
                Effect.succeed({
                  token: Redacted.make("fixture-token"),
                  fingerprint: "same-account",
                }),
              rest: () =>
                Effect.sync(() => {
                  http++;
                }).pipe(
                  Effect.andThen(Deferred.succeed(started, undefined)),
                  Effect.andThen(Deferred.await(release)),
                  Effect.as({
                    status: 200,
                    headers: {},
                    body: '{"id":123,"login":"same-account"}',
                    truncated: false,
                    invalidUtf8: false,
                  }),
                ),
            }),
            Layer.mock(VcsProcess.VcsProcess)({}),
          ),
        ),
      );
      const a = yield* f.access.capture(
        f.aAuthority,
        { cwd: f.a, projectId: projectA },
        AuthOrchestrationReadScope,
      );
      const rawB = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const b = {
        ...rawB,
        beforeEffect: (cwd: string, scope?: AuthEnvironmentScope) =>
          rawB.beforeEffect(cwd, scope).pipe(
            Effect.tap(() => {
              // The second completed admission is after the credential, just before taking the lock.
              bChecks++;
              return bChecks === 2 ? Deferred.succeed(queued, undefined) : Effect.void;
            }),
          ),
      };
      const first = yield* admitted(
        cli.getRoutingIdentity({ cwd: f.a, host: "github.com" }),
        a,
      ).pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(started);
      const second = yield* admitted(
        cli.getRoutingIdentity({ cwd: f.b, host: "github.com" }),
        b,
      ).pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(queued);
      yield* f.cut(cut);
      yield* Deferred.succeed(release, undefined);
      expect(Result.isSuccess(yield* Fiber.join(first))).toBe(true);
      expectDenied(yield* Fiber.join(second));
      expect(http).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "Bitbucket revision cache: warm=%s cannot cross a current root cut",
  (warm) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let requests = 0,
        hold = !warm;
      const api = yield* BitbucketPR.make.pipe(
        Effect.provide(
          Layer.mock(Bitbucket.BitbucketApi)({
            request: () =>
              Effect.suspend(() => {
                requests++;
                return (
                  hold
                    ? Deferred.succeed(started, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void
                ).pipe(
                  Effect.as({
                    body: "diff --git a/a.ts b/a.ts\nindex abc123..def456 100644\n--- a/a.ts\n+++ b/a.ts\n@@ -1 +1 @@\n-old\n+new\n",
                    truncated: false,
                  }),
                );
              }),
          }),
        ),
      );
      const read = api.getFileRevisions({ repository: "acme/web", number: 7, paths: ["a.ts"] });
      if (warm) yield* admitted(read, admission);
      const pending = yield* (
        warm
          ? Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(admitted(read, admission)),
            )
          : admitted(read, admission)
      ).pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(started);
      yield* f.cut("root");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      expect(requests).toBe(1);
      yield* f.restore;
      hold = false;
      yield* admitted(read, admission);
      expect(requests).toBe(warm ? 1 : 2);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([true, false])(
  "Forgejo: credential file exists=%s wait cut denies before reading tokens or trying fj/tea",
  (exists) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const fs = yield* FileSystem.FileSystem;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const effects: string[] = [];
      const cli = yield* Forgejo.make.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          exists: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(exists),
            ),
          readFileString: () =>
            Effect.sync(() => {
              effects.push("credential file");
              return "{}";
            }),
        }),
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(VcsProcess.VcsProcess)({
              run: () =>
                Effect.sync(() => {
                  effects.push("process");
                  return output("");
                }),
            }),
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) =>
                Effect.sync(() => {
                  effects.push("HTTP");
                  return HttpClientResponse.fromWeb(request, new Response("{}"));
                }),
              ),
            ),
          ),
        ),
      );
      const pending = yield* admitted(cli.listLogins!({ cwd: f.b, command: "fj" }), admission).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(started);
      yield* f.cut("membership");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      expect(effects).toEqual([]);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "Forgejo: queued authLock waiter cannot read credential files or cached identity after a grant cut",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const a = yield* f.access.capture(
        f.aAuthority,
        { cwd: f.a, projectId: projectA },
        AuthOrchestrationReadScope,
      );
      const rawB = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const fs = yield* FileSystem.FileSystem;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const bFile = yield* Deferred.make<void>();
      let bChecks = 0;
      const b = {
        ...rawB,
        beforeEffect: (cwd: string, scope?: AuthEnvironmentScope) =>
          rawB.beforeEffect(cwd, scope).pipe(
            Effect.tap(() => {
              // The file has returned and its final admission passed; the next wait is authLock.
              bChecks++;
              return bChecks === 4 ? Deferred.succeed(bFile, undefined) : Effect.void;
            }),
          ),
      };
      const effects: string[] = [];
      const cli = yield* Forgejo.make.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          exists: () => Effect.succeed(true),
          readFileString: () =>
            Access.workspaceEffectAdmission.pipe(
              Effect.flatMap((admission) => {
                effects.push(`file:${admission!.projectId}`);
                return Effect.succeed(
                  '{"hosts":{"forgejo.test":{"type":"Application","token":"fixture-token"}}}',
                );
              }),
            ),
        }),
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(VcsProcess.VcsProcess)({
              run: () =>
                Effect.sync(() => {
                  effects.push("fj whoami");
                }).pipe(
                  Effect.andThen(Deferred.succeed(started, undefined)),
                  Effect.andThen(Deferred.await(release)),
                  Effect.as(output("")),
                ),
            }),
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) =>
                Effect.sync(() => {
                  effects.push("HTTP");
                  return HttpClientResponse.fromWeb(
                    request,
                    new Response('{"login":"same-account"}'),
                  );
                }),
              ),
            ),
          ),
        ),
      );
      const first = yield* admitted(
        cli.getAccount!({ cwd: f.a, baseUrl: "https://forgejo.test" }),
        a,
      ).pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(started);
      const second = yield* admitted(
        cli.getAccount!({ cwd: f.b, baseUrl: "https://forgejo.test" }),
        b,
      ).pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(bFile);
      yield* f.cut("grant");
      yield* Deferred.succeed(release, undefined);
      expect(Result.isSuccess(yield* Fiber.join(first))).toBe(true);
      expectDenied(yield* Fiber.join(second));
      expect(effects.filter((effect) => effect === `file:${projectB}`)).toHaveLength(1);
      expect(effects.filter((effect) => effect === "fj whoami")).toHaveLength(1);
      expect(effects.filter((effect) => effect === "HTTP")).toHaveLength(1);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "GitHub conditional validators: a cut during REST cannot cache its answer or take the ordinary unsupported fallback",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const requests: GitHubApi.GitHubRestInput[] = [];
      let hold = true,
        reads = 0;
      const sha = "a".repeat(40);
      const revalidate = yield* ConditionalChecks.makeChecksRevalidator.pipe(
        Effect.provide(
          Layer.mock(GitHubApi.GitHubApi)({
            rest: (input) =>
              Effect.suspend(() => {
                requests.push(input);
                return (
                  hold
                    ? Deferred.succeed(started, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void
                ).pipe(
                  Effect.as({
                    status: 200,
                    headers: { etag: "fixture-etag" },
                    body: input.path.endsWith("/pulls/7")
                      ? JSON.stringify({
                          head: { sha, repo: { id: 1 } },
                          base: { repo: { id: 1 } },
                        })
                      : "{}",
                    truncated: false,
                    invalidUtf8: false,
                  }),
                );
              }),
          }),
        ),
      );
      const read = Effect.sync(() => {
        reads++;
        return { state: "open" as const, checks: [], headSha: sha, workflowApprovalsRequired: 0 };
      });
      const checked = admitted(
        revalidate({ cwd: f.b, host: "github.com", repository: "acme/web", number: 7 }, read).pipe(
          Effect.provideService(GitHubApi.PinnedGitHubCredential, {
            host: "github.com",
            token: Redacted.make("fixture-token"),
            credentialFingerprint: "same-account",
          }),
        ),
        admission,
      );
      const pending = yield* checked.pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(started);
      yield* f.cut("root");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      expect(requests).toHaveLength(1);
      expect(reads).toBe(0);
      yield* f.restore;
      hold = false;
      yield* checked;
      expect(requests[1]!.ifNoneMatch).toBeUndefined();
      expect(reads).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "GitHub summary: a typed transport denial escapes unchanged without an individual fallback",
  () =>
    Effect.gen(function* () {
      const denial = new EnvironmentAuthorizationError({
        requiredScope: AuthOrchestrationReadScope,
        message: "fixture denial",
      });
      let requests = 0;
      const cli = yield* PullRequests.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(GitHubApi.GitHubApi)({
              graphql: () =>
                Effect.sync(() => {
                  requests++;
                }).pipe(Effect.andThen(Effect.fail(denial))),
            }),
            Layer.mock(VcsProcess.VcsProcess)({}),
          ),
        ),
      );
      const pending = yield* cli
        .getPullRequestSummary({
          cwd: "/fixture",
          repository: "acme/web",
          host: "github.com",
          number: 7,
        })
        .pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* TestClock.adjust("10 millis");
      const result = yield* Fiber.join(pending);
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(result.failure).toBe(denial);
      expect(requests).toBe(1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect.each(["success", "failure"] as const)(
  "Bitbucket: Settings %s wait cut denies before resolving saved tokens or starting HTTP",
  (outcome) =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const settings = yield* Settings.ServerSettingsService.pipe(
        Effect.provide(Settings.ServerSettingsService.layerTest()),
      );
      const snapshot = yield* settings.getSettings;
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let tokens = 0,
        http = 0;
      const api = yield* Bitbucket.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(Registry.VcsDriverRegistry)({}),
            Layer.mock(Git.GitVcsDriver)({}),
            Layer.succeed(Settings.ServerSettingsService, {
              ...settings,
              getSettings: Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.flatMap(() =>
                  outcome === "failure"
                    ? Effect.fail(
                        new ServerSettingsError({
                          settingsPath: "<fixture>",
                          operation: "read-file",
                          cause: "fixture failure",
                        }),
                      )
                    : Effect.sync(() =>
                        Object.defineProperty({ ...snapshot }, "bitbucket", {
                          get: () => {
                            tokens++;
                            return snapshot.bitbucket;
                          },
                        }),
                      ),
                ),
              ),
            }),
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) =>
                Effect.sync(() => {
                  http++;
                  return HttpClientResponse.fromWeb(request, new Response("{}"));
                }),
              ),
            ),
          ),
        ),
      );
      const pending = yield* admitted(api.request({ method: "GET", url: "/user" }), admission).pipe(
        Effect.result,
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(started);
      yield* f.cut("action");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      expect(tokens).toBe(0);
      expect(http).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["gitlab", "azure-devops", "forgejo"] as const)(
  "%s: lower process denial retains its exact typed error without another command",
  (provider) =>
    Effect.gen(function* () {
      const denial = new EnvironmentAuthorizationError({
        requiredScope: AuthOrchestrationReadScope,
        message: "fixture denial",
      });
      let commands = 0;
      const process = Layer.mock(VcsProcess.VcsProcess)({
        run: () =>
          Effect.sync(() => {
            commands++;
          }).pipe(Effect.andThen(Effect.fail(denial))),
      });
      if (provider === "gitlab") {
        const cli = yield* GitLab.make.pipe(Effect.provide(process));
        expect(
          yield* cli.execute({ cwd: "/fixture", args: ["api", "user"] }).pipe(Effect.flip),
        ).toBe(denial);
      } else if (provider === "azure-devops") {
        const cli = yield* Azure.make.pipe(Effect.provide(process));
        expect(
          yield* cli.execute({ cwd: "/fixture", args: ["repos", "pr", "show"] }).pipe(Effect.flip),
        ).toBe(denial);
      } else {
        const cli = yield* Forgejo.make.pipe(
          Effect.provide(
            Layer.mergeAll(
              process,
              Layer.succeed(
                HttpClient.HttpClient,
                HttpClient.make(() => Effect.die("Unexpected HTTP")),
              ),
            ),
          ),
        );
        expect(yield* cli.resolveRepository({ cwd: "/fixture" }).pipe(Effect.flip)).toBe(denial);
        const adapter = yield* ForgejoProvider.make.pipe(
          Effect.provide(Layer.mock(Forgejo.ForgejoCli)({ api: () => Effect.fail(denial) })),
        );
        expect(
          yield* adapter.getChangeRequestSummary!({
            cwd: "/fixture",
            host: "forgejo.test",
            repository: "acme/web",
            number: 7,
          }).pipe(Effect.flip),
        ).toBe(denial);
      }
      expect(commands).toBe(1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "GitHub identity: current root cut after user HTTP prevents a successful identity cache write",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const admission = yield* f.access.capture(
        f.authority,
        { cwd: f.b, projectId: projectB },
        AuthOrchestrationReadScope,
      );
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let requests = 0,
        hold = true;
      const cli = yield* PullRequests.make.pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(GitHubApi.GitHubApi)({
              credential: () =>
                Effect.succeed({
                  token: Redacted.make("fixture-token"),
                  fingerprint: "same-account",
                }),
              rest: () =>
                Effect.suspend(() => {
                  requests++;
                  return (
                    hold
                      ? Deferred.succeed(started, undefined).pipe(
                          Effect.andThen(Deferred.await(release)),
                        )
                      : Effect.void
                  ).pipe(
                    Effect.as({
                      status: 200,
                      headers: {},
                      body: '{"id":123,"login":"same-account"}',
                      truncated: false,
                      invalidUtf8: false,
                    }),
                  );
                }),
            }),
            Layer.mock(VcsProcess.VcsProcess)({}),
          ),
        ),
      );
      const read = admitted(cli.getRoutingIdentity({ cwd: f.b, host: "github.com" }), admission);
      const pending = yield* read.pipe(Effect.result, Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(started);
      yield* f.cut("root");
      yield* Deferred.succeed(release, undefined);
      expectDenied(yield* Fiber.join(pending));
      yield* f.restore;
      hold = false;
      yield* read;
      expect(requests).toBe(2);
    }).pipe(Effect.provide(layer)),
);
