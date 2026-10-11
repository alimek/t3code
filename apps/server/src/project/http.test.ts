import * as SessionStore from "../auth/SessionStore.ts";
import { assert, it } from "@effect/vitest";
import {
  ProjectId,
  AuthSessionId,
  AuthAllResourceGrant,
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { Etag, HttpRouter } from "effect/http";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import * as NodeServices from "@effect/platform-node/NodeServices";

import {
  ProjectConflictError,
  ProjectNotEmptyError,
  ProjectNotFoundError,
  ProjectOperationError,
} from "./ProjectService.ts";
import { ServerRuntimeStartupError } from "../serverRuntimeStartup.ts";
import { failProjectMutation } from "./http.ts";
import * as ProjectHttp from "./http.ts";
import * as OrchestrationHttp from "../orchestration-v2/http.ts";
import * as Startup from "../serverRuntimeStartup.ts";
import {
  collectionStoreLayer,
  makeCollectionFixture,
  grantA,
  grantNone,
  projectA,
  projectB,
  activeA,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";

class CollectionApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.projects,
  EnvironmentHttpApi.groups.orchestration,
) {}

it.effect(
  "HTTP project and shell collections filter before enrichment with unchanged schemas",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeCollectionFixture;
      const context = yield* Effect.context<Layer.Success<typeof collectionStoreLayer>>();
      const head = yield* fixture.events.latestApplicationSequence;
      for (const grant of [grantA, grantNone, AuthAllResourceGrant, undefined]) {
        const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
          effect.pipe(
            Effect.provideService(EnvironmentAuthenticatedPrincipal, {
              sessionId: AuthSessionId.make("collection-http-fixture"),
              subject: "fixture",
              method: "browser-session-cookie",
              scopes: new Set([AuthOrchestrationReadScope]),
              ...(grant === undefined ? {} : { resourceGrant: grant }),
            }),
          ),
        );
        const routes = HttpApiBuilder.layer(CollectionApi).pipe(
          Layer.provide(ProjectHttp.layer),
          Layer.provide(OrchestrationHttp.layer),
          Layer.provide(
            Layer.mock(SessionStore.SessionStore)({
              cookieName: "fixture",
              legacyCookieName: undefined,
            }),
          ),
          Layer.provide(auth),
          Layer.provide(fixture.services()),
          Layer.provide(Layer.mock(Startup.ServerRuntimeStartup)({})),
          Layer.provide(Layer.succeedContext(context)),
          Layer.provide(NodeHttpPlatform.layer),
          Layer.provide(Etag.layerWeak),
          Layer.provide(NodeServices.layer),
        );
        yield* Effect.acquireUseRelease(
          Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
          ({ handler }) =>
            Effect.promise(async () => {
              const expected =
                grant === grantNone ? [] : grant === grantA ? [projectA] : [projectA, projectB];
              for (const path of ["/api/projects", "/api/orchestration/shell"]) {
                fixture.enrichedRoots.length = 0;
                const response = await handler(
                  new Request(`http://fixture.test${path}`, {
                    headers: {
                      [ORCHESTRATION_PROTOCOL_HEADER]: String(ORCHESTRATION_PROTOCOL_VERSION),
                    },
                  }),
                );
                assert.equal(response.status, 200);
                const body = (await response.json()) as {
                  projects: ReadonlyArray<{ id: ProjectId }>;
                  threads?: ReadonlyArray<{ id: string }>;
                  archivedThreads?: ReadonlyArray<unknown>;
                  snapshotSequence?: number;
                };
                assert.deepEqual(
                  body.projects.map((project) => project.id),
                  expected,
                );
                assert.deepEqual(
                  fixture.enrichedRoots,
                  expected.map((id) => `/work/${id}`),
                );
                if (path.endsWith("/shell")) {
                  assert.equal(body.snapshotSequence, head);
                  assert.deepEqual(body.archivedThreads, []);
                  if (grant === grantA)
                    assert.deepEqual(
                      body.threads?.map((thread) => thread.id),
                      [activeA],
                    );
                  if (grant === grantNone) assert.deepEqual(body.threads, []);
                }
                if (grant === grantA || grant === grantNone)
                  assert.notInclude(JSON.stringify(body), "b-secret");
              }
            }),
          ({ dispose }) => Effect.promise(dispose),
        );
      }
    }).pipe(Effect.provide(collectionStoreLayer), Effect.scoped),
);

const projectId = ProjectId.make("project:http-mutation");

it.effect.each([
  new ProjectNotFoundError({ projectId }),
  new ProjectNotEmptyError({ projectId }),
  new ProjectConflictError({
    projectId,
    workspaceRoot: "/workspace/project",
    conflictingProjectId: ProjectId.make("project:http-mutation-conflict"),
  }),
])("maps expected project mutation failures to invalid requests", (cause) =>
  Effect.gen(function* () {
    const error = yield* failProjectMutation(cause).pipe(Effect.flip);

    assert.equal(error._tag, "EnvironmentRequestInvalidError");
    assert.equal(error.code, "invalid_request");
    if (error._tag === "EnvironmentScopeRequiredError")
      return assert.fail("Unexpected authorization error");
    assert.equal(error.reason, "invalid_command");
  }),
);

it.effect.each([
  new ProjectOperationError({
    operation: "dispatch-project-command",
    projectId,
    cause: "database unavailable",
  }),
  new ServerRuntimeStartupError({
    mode: "web",
    host: null,
    port: 0,
    cause: "startup unavailable",
  }),
])("keeps operational and startup failures internal", (cause) =>
  Effect.gen(function* () {
    const error = yield* failProjectMutation(cause).pipe(Effect.flip);

    assert.equal(error._tag, "EnvironmentInternalError");
    assert.equal(error.code, "internal_error");
    if (error._tag === "EnvironmentScopeRequiredError")
      return assert.fail("Unexpected authorization error");
    assert.equal(error.reason, "project_mutation_failed");
  }),
);
