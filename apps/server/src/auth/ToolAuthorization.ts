import {
  AuthOrchestrationReadScope,
  AuthResourceGrant,
  EnvironmentAuthorizationError,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { trustedReadAuthority, type ThreadReadAuthority } from "./ResourceAuthorization.ts";

const isResourceGrant = Schema.is(AuthResourceGrant);

/** Explicit selections stay restricted even when they list every current resource. */
export const resourceRestricted = (grant: unknown): boolean =>
  grant !== undefined &&
  grant !== null &&
  (!isResourceGrant(grant) || grant.projects.mode !== "all" || grant.providers.mode !== "all");

export const extraToolDenied = (requiredScope: AuthEnvironmentScope = AuthOrchestrationReadScope) =>
  new EnvironmentAuthorizationError({
    requiredScope,
    message: "Extra T3 tools are unavailable to resource-restricted connections.",
  });

/** Authenticated HTTP principals already carry the request's freshly verified grant. */
export const authorizeResourceTool = (
  grant: unknown,
  scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
): Effect.Effect<void, EnvironmentAuthorizationError> =>
  resourceRestricted(grant) ? Effect.fail(extraToolDenied(scope)) : Effect.void;

/** Public tool entries must supply current authority; only named internal work opts into trust. */
export const authorizeExtraTool = Effect.fn("authorization.extraTool")(function* (
  authority: ThreadReadAuthority,
  scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
) {
  if (authority === trustedReadAuthority) return;
  if (!authority?.current) return yield* extraToolDenied(scope);
  const current = yield* authority.current;
  if (!current.scopes.includes(scope)) return yield* extraToolDenied(scope);
  yield* authorizeResourceTool(current.resourceGrant, scope);
});

/** Keep the existing workspace admission's final-effect reloads, including grant cuts after waits. */
export const extraToolAuthority = (authority: ThreadReadAuthority): ThreadReadAuthority =>
  authority === trustedReadAuthority
    ? authority
    : {
        current: Effect.suspend(() => authority?.current ?? Effect.fail(extraToolDenied())).pipe(
          Effect.tap((current) => authorizeResourceTool(current.resourceGrant)),
        ),
      };
