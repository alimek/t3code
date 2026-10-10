import { AuthResourceGrant } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export class ResourceGrantsNotActivatedError extends Schema.TaggedError<ResourceGrantsNotActivatedError>()(
  "ResourceGrantsNotActivatedError",
  {},
) {
  override get message(): string {
    return "Resource grant issuance, editing and conversion are not activated.";
  }
}

const isResourceGrant = Schema.is(AuthResourceGrant);

/** Remove this gate only once every access path enforces resource grants (activation ticket). */
export function requireUnrestrictedResourceGrant(grant: unknown) {
  return grant === undefined ||
    grant === null ||
    (isResourceGrant(grant) && grant.projects.mode === "all" && grant.providers.mode === "all")
    ? Effect.void
    : Effect.fail(new ResourceGrantsNotActivatedError({}));
}
