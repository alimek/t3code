import * as SessionStore from "../auth/SessionStore.ts";
import { mutationDenied, type MutationAuthority } from "../auth/CommandAuthorization.ts";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import {
  annotateEnvironmentRequest,
  failEnvironmentInternal,
  failEnvironmentInvalidRequest,
  requireEnvironmentScope,
  failEnvironmentScopeRequired,
} from "../auth/http.ts";
import { projectCollectionOptions } from "../auth/projectCollectionAccess.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as ServerRuntimeStartup from "../serverRuntimeStartup.ts";
import * as ProjectService from "./ProjectService.ts";
import { projectMutationOperation } from "./ProjectMutation.ts";

export const failProjectMutation = Effect.fn("environment.projects.failMutation")(function* (
  cause: ProjectService.ProjectServiceError | ServerRuntimeStartup.ServerRuntimeStartupError,
) {
  if (cause._tag === "EnvironmentAuthorizationError")
    return yield* failEnvironmentScopeRequired(cause.requiredScope);
  if (
    cause._tag === "ProjectNotFoundError" ||
    cause._tag === "ProjectConflictError" ||
    cause._tag === "ProjectNotEmptyError"
  ) {
    return yield* failEnvironmentInvalidRequest("invalid_command");
  }
  return yield* failEnvironmentInternal("project_mutation_failed", cause);
});

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "projects",
  Effect.fnUntraced(function* (handlers) {
    const sessions = yield* SessionStore.SessionStore;
    const projects = yield* ProjectService.ProjectService;
    const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;

    return handlers
      .handle(
        "snapshot",
        Effect.fn("environment.projects.snapshot")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const principal = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* projects
            .getSnapshot(projectCollectionOptions(principal.resourceGrant))
            .pipe(
              traceLocalHandlerWork,
              Effect.catch((cause) => failEnvironmentInternal("project_snapshot_failed", cause)),
            );
        }),
      )
      .handle(
        "mutate",
        Effect.fn("environment.projects.mutate")(function* (args) {
          yield* annotateEnvironmentRequest(args.endpoint.name);
          const principal = yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          const authority: MutationAuthority = {
            current: sessions
              .getAuthority(principal.sessionId, { ...principal, scopes: [...principal.scopes] })
              .pipe(Effect.mapError(() => mutationDenied())),
          };
          const operation = projectMutationOperation(projects, authority, args.payload);
          return yield* startup
            .enqueueCommand(operation)
            .pipe(traceLocalHandlerWork, Effect.catch(failProjectMutation));
        }),
      );
  }),
);
