import { expect, it } from "@effect/vitest";
import {
  AuthAllResourceGrant,
  AuthOrchestrationReadScope,
  AuthResourceGrant,
  ProviderInstanceId,
  ProviderDriverKind,
  ServerProvider,
  ServerConfig as ServerConfigSchema,
  type ServerConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Config from "../config.ts";
import * as Registry from "./ProviderRegistry.ts";
import * as Instances from "./ProviderInstanceRegistry.ts";
import * as Manifest from "./ModelManifest.ts";
import type { ProviderInstance } from "./ProviderDriver.ts";
import { type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as Catalog from "./ProviderCatalog.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { providerCatalog, restrictedServerConfig } from "./ProviderCatalog.ts";

const isServerProvider = Schema.is(ServerProvider);
const isServerConfig = Schema.is(ServerConfigSchema);

const instanceId = ProviderInstanceId.make("allowed");
const snapshot: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  displayName: "Allowed",
  enabled: true,
  installed: true,
  status: "ready",
  version: "secret-version",
  checkedAt: "2026-10-11T00:00:00Z",
  auth: { status: "authenticated", email: "secret-email", profileId: "secret-profile" },
  message: "secret-message",
  runtimePaths: { homePath: "/secret-home", shadowHomePath: null },
  models: ["one", "two"].map((slug) => ({ slug, name: slug, isCustom: false, capabilities: null })),
  slashCommands: [{ name: "secret-command", description: "secret-command" }],
  skills: [],
};
const grant: AuthResourceGrant = {
  ...AuthAllResourceGrant,
  providers: {
    mode: "explicit",
    instances: [{ instanceId, models: { mode: "explicit", ids: ["one"] } }],
  },
};
const authority: ThreadReadAuthority = {
  current: Effect.succeed({ resourceGrant: grant, scopes: [AuthOrchestrationReadScope] }),
};

it.effect(
  "v1 catalog exposes configured allowed models only and no owner account/config/helper data",
  () =>
    Effect.gen(function* () {
      const providers = yield* providerCatalog(authority, [
        snapshot,
        { ...snapshot, instanceId: ProviderInstanceId.make("hidden") },
      ]);
      expect(providers.map((provider) => provider.instanceId)).toEqual([instanceId]);
      expect(providers[0]?.models.map((model) => model.slug)).toEqual(["one"]);
      expect(JSON.stringify(providers)).not.toContain("secret");
      expect(isServerProvider(providers[0])).toBe(true);
      const config = restrictedServerConfig(
        {
          environmentId: "test" as ServerConfig["environment"]["environmentId"],
          label: "test",
          platform: { os: "linux", arch: "x64" } as ServerConfig["environment"]["platform"],
          serverVersion: "1",
          capabilities: { repositoryIdentity: false },
        },
        {
          policy: "loopback-browser",
          bootstrapMethods: ["one-time-token"],
          sessionMethods: ["browser-session-cookie"],
          sessionCookieName: "test",
        },
        providers,
      );
      expect(config.settings.providerInstances).toEqual({});
      expect(config.settings.textGenerationModelSelection.instanceId).toBe("unavailable");
      expect(isServerConfig(config)).toBe(true);
      expect(config.cwd).toBe("/");
      expect(config.availableEditors).toEqual([]);
      const owner = {
        current: Effect.succeed({
          resourceGrant: AuthAllResourceGrant,
          scopes: [AuthOrchestrationReadScope],
        }),
      };
      expect(yield* providerCatalog(owner, [snapshot])).toEqual([snapshot]);
      expect(
        yield* providerCatalog(
          {
            current: Effect.succeed({
              resourceGrant: { ...grant, providers: { mode: "explicit", instances: [] } },
              scopes: [AuthOrchestrationReadScope],
            }),
          },
          [snapshot],
        ),
      ).toEqual([]);
    }),
);

it.effect(
  "v1 denies provider refresh before credential effects, including a cut after source lookup",
  () =>
    Effect.gen(function* () {
      const selected = yield* Ref.make<AuthResourceGrant>(AuthAllResourceGrant);
      const reached = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let hold = false,
        refreshes = 0,
        checks = 0;
      const current: ThreadReadAuthority = {
        current: Effect.gen(function* () {
          if (hold && ++checks === 2) {
            yield* Deferred.succeed(reached, undefined);
            yield* Deferred.await(release);
          }
          return { resourceGrant: yield* Ref.get(selected), scopes: [AuthOrchestrationReadScope] };
        }),
      };
      const instance = {
        instanceId,
        driverKind: snapshot.driver,
        enabled: true,
        displayName: undefined,
        continuationIdentity: { driverKind: snapshot.driver, continuationKey: "test" },
        snapshot: {
          getSnapshot: Effect.succeed(snapshot),
          streamChanges: Stream.empty,
          refresh: Effect.sync(() => {
            refreshes++;
            return snapshot;
          }),
          applyUsageLimits: () => Effect.void,
          resolveMaintenance: () => Effect.die("no maintenance expected"),
        },
        orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
        textGeneration: {} as ProviderInstance["textGeneration"],
      } satisfies ProviderInstance;
      const instances = Layer.succeed(Instances.ProviderInstanceRegistry, {
        getInstance: () => Effect.succeed(instance),
        listInstances: Effect.succeed([instance]),
        listUnavailable: Effect.succeed([]),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
      });
      const registry = yield* Registry.ProviderRegistry.pipe(
        Effect.provide(
          Registry.layer.pipe(
            Layer.provide(instances),
            Layer.provide(Manifest.layerTest),
            Layer.provide(
              Config.layerTest(process.cwd(), { prefix: "t3-v1-catalog-" }).pipe(
                Layer.provideMerge(NodeServices.layer),
              ),
            ),
          ),
        ),
      );
      yield* Ref.set(selected, grant);
      for (const read of [
        registry.refresh(current),
        registry.refreshInstance(current, instanceId),
        registry.refreshWorkspaceSnapshot(current, { instanceId, cwd: "/hidden" }),
      ])
        expect(yield* read.pipe(Effect.flip)).toMatchObject({
          _tag: "EnvironmentAuthorizationError",
        });
      expect(refreshes).toBe(0);
      yield* Ref.set(selected, AuthAllResourceGrant);
      yield* registry.refreshInstance(current, instanceId);
      expect(refreshes).toBe(1);
      hold = true;
      const pending = yield* registry.refreshInstance(current, instanceId).pipe(Effect.forkChild);
      yield* Deferred.await(reached);
      yield* Ref.set(selected, grant);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(pending).pipe(Effect.flip)).toMatchObject({
        _tag: "EnvironmentAuthorizationError",
      });
      expect(refreshes).toBe(1);
    }).pipe(Effect.scoped),
);

it.effect("restricted catalog service owns cached config, refresh and current stream cuts", () =>
  Effect.gen(function* () {
    const selected = yield* Ref.make<AuthResourceGrant>(AuthAllResourceGrant);
    const scopes = yield* Ref.make<ReadonlyArray<typeof AuthOrchestrationReadScope>>([
      AuthOrchestrationReadScope,
    ]);
    const reached = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    let descriptorReads = 0;
    const current: ThreadReadAuthority = {
      current: Effect.all({
        resourceGrant: Ref.get(selected),
        scopes: Ref.get(scopes),
      }),
    };
    const environment: ServerConfig["environment"] = {
      environmentId: "test" as ServerConfig["environment"]["environmentId"],
      label: "test",
      platform: { os: "linux", arch: "x64" },
      serverVersion: "1",
      capabilities: { repositoryIdentity: false },
    };
    const auth: ServerConfig["auth"] = {
      policy: "loopback-browser",
      bootstrapMethods: ["one-time-token"],
      sessionMethods: ["browser-session-cookie"],
      sessionCookieName: "test",
    };
    const catalog = yield* Catalog.ProviderCatalog.pipe(
      Effect.provide(
        Catalog.layer.pipe(
          Layer.provide(
            Layer.succeed(ServerEnvironment.ServerEnvironment, {
              getEnvironmentId: Effect.succeed(environment.environmentId),
              getDescriptor: Effect.sync(() => {
                descriptorReads++;
                return environment;
              }),
            }),
          ),
          Layer.provide(
            Layer.succeed(EnvironmentAuth.EnvironmentAuth, {
              getDescriptor: () => Effect.succeed(auth),
            } as EnvironmentAuth.EnvironmentAuth["Service"]),
          ),
          Layer.provide(
            Layer.succeed(Registry.ProviderRegistry, {
              getProviders: Effect.succeed([snapshot]),
              refresh: () => Effect.die("Catalog must not refresh credentials"),
              refreshInstance: () => Effect.die("Catalog must not refresh credentials"),
              refreshWorkspaceSnapshot: () => Effect.die("Catalog must not probe workspaces"),
              getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("No maintenance"),
              setProviderMaintenanceActionState: () => Effect.die("No maintenance"),
              streamChanges: Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Deferred.succeed(reached, undefined);
                  yield* Deferred.await(release);
                  return [snapshot];
                }),
              ),
            }),
          ),
        ),
      ),
    );
    // Owner transport continues through its inherited implementation without catalog work.
    expect(yield* catalog.restrictedConfig(current)).toBeUndefined();
    expect(yield* catalog.restrictedRefresh(current)).toBeUndefined();
    expect(yield* catalog.restrictedSubscription(current)).toBeUndefined();
    expect(descriptorReads).toBe(0);
    yield* Ref.set(selected, grant);
    const config = yield* catalog.restrictedConfig(current);
    expect(isServerConfig(config)).toBe(true);
    expect(config?.providers[0]?.models.map((model) => model.slug)).toEqual(["one"]);
    expect(JSON.stringify(config)).not.toContain("secret");
    expect(yield* catalog.restrictedRefresh(current)).toEqual({ providers: config?.providers });
    const initial = yield* catalog.restrictedSubscription(current);
    if (initial === undefined) return yield* Effect.die("Expected restricted subscription");
    const events = yield* initial.pipe(Stream.take(1), Stream.runCollect);
    expect(events).toEqual([{ version: 1, type: "snapshot", config }]);
    const updates = yield* catalog.restrictedSubscription(current);
    if (updates === undefined) return yield* Effect.die("Expected restricted updates");
    const pending = yield* updates.pipe(Stream.runCollect, Effect.forkChild);
    yield* Deferred.await(reached);
    yield* Ref.set(scopes, []);
    yield* Deferred.succeed(release, undefined);
    expect(yield* Fiber.join(pending).pipe(Effect.flip)).toMatchObject({
      _tag: "EnvironmentAuthorizationError",
    });
  }).pipe(Effect.scoped),
);
