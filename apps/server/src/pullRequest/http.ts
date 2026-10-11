import { AuthOrchestrationReadScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { annotateEnvironmentRequest, requireEnvironmentScope } from "../auth/http.ts";
import { traceLocalHandlerWork } from "../cloud/traceRelayRequest.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import { threadReadDenied, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as PullRequestService from "./PullRequestService.ts";

/** The patch is often the largest PR payload and benefits from HTTP compression and flow control. */
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "pullRequests",
  Effect.fnUntraced(function* (handlers) {
    const sessions = yield* SessionStore.SessionStore;
    const pullRequests = yield* PullRequestService.PullRequestService;
    return handlers.handle(
      "diff",
      Effect.fn("environment.pullRequests.diff")(function* (args) {
        yield* annotateEnvironmentRequest(args.endpoint.name);
        const principal = yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        const authority: ThreadReadAuthority = {
          current: sessions
            .getAuthority(principal.sessionId, { ...principal, scopes: [...principal.scopes] })
            .pipe(Effect.mapError(() => threadReadDenied())),
        };
        return yield* pullRequests.diff(authority, args.payload).pipe(traceLocalHandlerWork);
      }),
    );
  }),
);
