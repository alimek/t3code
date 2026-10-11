import {
  AuthOrchestrationReadScope,
  AuthResourceGrant,
  DEFAULT_SERVER_SETTINGS,
  ProviderInstanceId,
  resourceGrantAllowsModel,
  type ServerProvider,
  type ServerConfig,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import { extraToolDenied, resourceRestricted } from "../auth/ToolAuthorization.ts";

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
