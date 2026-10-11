import {
  AuthOrchestrationReadScope,
  AuthResourceGrant,
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  resourceGrantAllowsModel,
  type ServerProvider,
  type ServerConfig,
  type ServerConfigStreamEvent,
  EnvironmentAuthorizationError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import { extraToolDenied, resourceRestricted } from "../auth/ToolAuthorization.ts";

export class ProviderCatalog extends Context.Service<
  ProviderCatalog,
  {
    readonly restrictedConfig: (
      authority: ThreadReadAuthority,
    ) => Effect.Effect<ServerConfig | undefined, EnvironmentAuthorizationError>;
    readonly restrictedRefresh: (
      authority: ThreadReadAuthority,
    ) => Effect.Effect<
      { readonly providers: ReadonlyArray<ServerProvider> } | undefined,
      EnvironmentAuthorizationError
    >;
    readonly restrictedSubscription: (
      authority: ThreadReadAuthority,
    ) => Effect.Effect<
      Stream.Stream<ServerConfigStreamEvent, EnvironmentAuthorizationError> | undefined,
      EnvironmentAuthorizationError
    >;
  }
>()("t3/provider/ProviderCatalog") {}

const isResourceGrant = Schema.is(AuthResourceGrant);

/** Cached configured descriptors only: catalog reads never refresh credentials or probe the host. */
export const providerCatalog = Effect.fn("provider.catalog")(function* (
  authority: ThreadReadAuthority,
  providers: ReadonlyArray<ServerProvider>,
) {
  if (authority === trustedReadAuthority) return providers;
  if (!authority?.current) return yield* extraToolDenied();
  const current = yield* authority.current;
  if (!current.scopes.includes(AuthOrchestrationReadScope)) return yield* extraToolDenied();
  if (!resourceRestricted(current.resourceGrant)) return providers;
  if (!isResourceGrant(current.resourceGrant)) return yield* extraToolDenied();
  return providers.flatMap((provider): ReadonlyArray<ServerProvider> => {
    const models = provider.models.filter((model) =>
      resourceGrantAllowsModel(current.resourceGrant, provider.instanceId, model.slug),
    );
    // Explicit empty/missing instances convey no catalog or execution authority.
    if (models.length === 0) return [];
    return [
      {
        instanceId: provider.instanceId,
        driver: provider.driver,
        ...(provider.displayName === undefined ? {} : { displayName: provider.displayName }),
        enabled: provider.enabled,
        installed: provider.installed,
        status: provider.status,
        version: null,
        auth: { status: "unknown" },
        checkedAt: provider.checkedAt,
        models: models.map(({ slug, name, shortName, isCustom, isDefault, capabilities }) => ({
          slug,
          name,
          ...(shortName === undefined ? {} : { shortName }),
          isCustom,
          ...(isDefault === undefined ? {} : { isDefault }),
          capabilities,
        })),
        slashCommands: [],
        skills: [],
      },
    ];
  });
});

/** Wire compatibility without disclosing owner paths, provider configuration or helper selection. */
export const restrictedServerConfig = (
  environment: ServerConfig["environment"],
  auth: ServerConfig["auth"],
  providers: ServerConfig["providers"],
): ServerConfig => ({
  environment,
  auth,
  providers,
  cwd: "/",
  keybindingsConfigPath: "/",
  keybindings: [],
  issues: [],
  availableEditors: [],
  remoteOpenTargets: [],
  directEndpoints: [],
  observability: {
    logsDirectoryPath: "/",
    localTracingEnabled: false,
    otlpTracesEnabled: false,
    otlpMetricsEnabled: false,
    otlpLogsEnabled: false,
  },
  settings: {
    ...DEFAULT_SERVER_SETTINGS,
    providerInstances: {},
    textGenerationModelSelection: {
      instanceId: ProviderInstanceId.make("unavailable"),
      model: "unavailable",
    },
  },
  shellResumeCompletionMarker: true,
  threadResumeCompletionMarker: true,
  threadSnapshotPagination: true,
  threadFind: true,
  threadFindProgressive: true,
});

const make = Effect.gen(function* () {
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const isRestricted = (authority: ThreadReadAuthority) =>
    Effect.gen(function* () {
      if (authority === trustedReadAuthority) return false;
      if (!authority?.current) return yield* extraToolDenied();
      return resourceRestricted((yield* authority.current).resourceGrant);
    });
  const restrictedConfig = Effect.fn("ProviderCatalog.restrictedConfig")(function* (
    authority: ThreadReadAuthority,
  ) {
    if (!(yield* isRestricted(authority))) return undefined;
    const descriptor = yield* environment.getDescriptor;
    const authDescriptor = yield* auth.getDescriptor();
    return restrictedServerConfig(
      descriptor,
      authDescriptor,
      yield* providerCatalog(authority, yield* registry.getProviders),
    );
  });
  const restrictedRefresh = Effect.fn("ProviderCatalog.restrictedRefresh")(function* (
    authority: ThreadReadAuthority,
  ) {
    if (!(yield* isRestricted(authority))) return undefined;
    return { providers: yield* providerCatalog(authority, yield* registry.getProviders) };
  });
  const restrictedSubscription = Effect.fn("ProviderCatalog.restrictedSubscription")(function* (
    authority: ThreadReadAuthority,
  ) {
    const config = yield* restrictedConfig(authority);
    if (config === undefined) return undefined;
    return Stream.concat(
      Stream.fromEffect(
        providerCatalog(authority, config.providers).pipe(
          Effect.map((providers): ServerConfigStreamEvent => ({
            version: 1,
            type: "snapshot",
            config: restrictedServerConfig(config.environment, config.auth, providers),
          })),
        ),
      ),
      registry.streamChanges.pipe(
        Stream.mapEffect((providers) => providerCatalog(authority, providers)),
        Stream.map((providers): ServerConfigStreamEvent => ({
          version: 1,
          type: "providerStatuses",
          payload: { providers },
        })),
      ),
    );
  });
  return ProviderCatalog.of({ restrictedConfig, restrictedRefresh, restrictedSubscription });
});

export const layer = Layer.effect(ProviderCatalog, make);
