import { extraToolAuthority } from "../auth/ToolAuthorization.ts";
import { EnvironmentAuthorizationError, AuthSourceControlWriteScope } from "@t3tools/contracts";
import type { ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as WorkspaceReadAccess from "../auth/workspaceReadAccess.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  type VcsDriverKind,
  type VcsError,
  type VcsInitInput,
  VcsUnsupportedOperationError,
} from "@t3tools/contracts";
import * as VcsDriverRegistry from "./VcsDriverRegistry.ts";

export class VcsProvisioningService extends Context.Service<
  VcsProvisioningService,
  {
    readonly initRepository: (
      authority: ThreadReadAuthority,
      input: VcsInitInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<void>,
    ) => Effect.Effect<void, VcsError | EnvironmentAuthorizationError>;
  }
>()("t3/vcs/VcsProvisioningService") {}

function resolveRequestedKind(
  kind: VcsDriverKind | undefined,
): Effect.Effect<VcsDriverKind, VcsUnsupportedOperationError> {
  if (kind === undefined) {
    return Effect.succeed("git");
  }
  if (kind === "unknown") {
    return Effect.fail(
      new VcsUnsupportedOperationError({
        operation: "VcsProvisioningService.resolveRequestedKind",
        kind,
        detail: "A concrete VCS driver kind is required for repository provisioning.",
      }),
    );
  }
  return Effect.succeed(kind);
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const access = yield* WorkspaceReadAccess.make;
  const registry = yield* VcsDriverRegistry.VcsDriverRegistry;

  const initRepository: VcsProvisioningService["Service"]["initRepository"] = Effect.fn(
    "VcsProvisioningService.initRepository",
  )(function* (authority, input) {
    const kind = yield* resolveRequestedKind(input.kind);
    const driver = yield* registry.get(kind);
    yield* WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd);
    return yield* driver.initRepository(input);
  });

  return VcsProvisioningService.of({
    initRepository: (authority, input, afterSuccess) =>
      access.run(
        extraToolAuthority(authority),
        input,
        initRepository(authority, input),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
  });
});

export const layer = Layer.effect(VcsProvisioningService, make);
