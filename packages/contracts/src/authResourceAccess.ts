import * as Schema from "effect/Schema";

import { ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

const AllResources = Schema.Struct({ mode: Schema.Literal("all") });
const ExplicitProjects = Schema.Struct({
  mode: Schema.Literal("explicit"),
  ids: Schema.Array(ProjectId),
});
const ModelResources = Schema.Union([
  AllResources,
  Schema.Struct({ mode: Schema.Literal("explicit"), ids: Schema.Array(TrimmedNonEmptyString) }),
]);

/** IDs are stable project/instance IDs and server-resolved canonical model IDs, never driver names. */
export const AuthResourceGrant = Schema.Struct({
  version: Schema.Literal(1),
  projects: Schema.Union([AllResources, ExplicitProjects]),
  providers: Schema.Union([
    AllResources,
    Schema.Struct({
      mode: Schema.Literal("explicit"),
      instances: Schema.Array(
        Schema.Struct({ instanceId: ProviderInstanceId, models: ModelResources }),
      ).check(
        Schema.makeFilter(
          (entries) => new Set(entries.map((entry) => entry.instanceId)).size === entries.length,
          { message: "Provider instance grants must be unique." },
        ),
      ),
    }),
  ]),
});
export type AuthResourceGrant = typeof AuthResourceGrant.Type;

export const AuthAllResourceGrant: AuthResourceGrant = {
  version: 1,
  projects: { mode: "all" },
  providers: { mode: "all" },
};

type LegacyResourceGrant = AuthResourceGrant | null | undefined;
type IdSelection<T> =
  | { readonly mode: "all" }
  | { readonly mode: "explicit"; readonly ids: ReadonlyArray<T> };

function includesId<T>(selection: IdSelection<T>, id: T): boolean {
  return selection.mode === "all" || selection.ids.includes(id);
}

function intersectIds<T>(left: IdSelection<T>, right: IdSelection<T>): IdSelection<T> {
  if (left.mode === "all") return right;
  if (right.mode === "all") return left;
  const allowed = new Set(right.ids);
  return { mode: "explicit", ids: [...new Set(left.ids)].filter((id) => allowed.has(id)) };
}

function idsSubset<T>(left: IdSelection<T>, right: IdSelection<T>): boolean {
  return (
    right.mode === "all" ||
    (left.mode === "explicit" && left.ids.every((id) => right.ids.includes(id)))
  );
}

export function resourceGrantAllowsProject(
  grant: LegacyResourceGrant,
  projectId: ProjectId,
): boolean {
  return includesId((grant ?? AuthAllResourceGrant).projects, projectId);
}

export function resourceGrantAllowsModel(
  grant: LegacyResourceGrant,
  instanceId: ProviderInstanceId,
  canonicalModelId: string,
): boolean {
  const providers = (grant ?? AuthAllResourceGrant).providers;
  if (providers.mode === "all") return true;
  const instance = providers.instances.find((entry) => entry.instanceId === instanceId);
  return instance !== undefined && includesId(instance.models, canonicalModelId);
}

export function intersectResourceGrants(
  left: LegacyResourceGrant,
  right: LegacyResourceGrant,
): AuthResourceGrant {
  const a = left ?? AuthAllResourceGrant;
  const b = right ?? AuthAllResourceGrant;
  let providers = a.providers;
  if (a.providers.mode === "all") {
    providers = b.providers;
  } else if (b.providers.mode === "explicit") {
    const allowed = new Map(b.providers.instances.map((entry) => [entry.instanceId, entry.models]));
    providers = {
      mode: "explicit",
      instances: a.providers.instances.flatMap((entry) => {
        const models = allowed.get(entry.instanceId);
        return models === undefined
          ? []
          : [{ instanceId: entry.instanceId, models: intersectIds(entry.models, models) }];
      }),
    };
  }
  return { version: 1, projects: intersectIds(a.projects, b.projects), providers };
}

export function resourceGrantIsSubset(
  left: LegacyResourceGrant,
  right: LegacyResourceGrant,
): boolean {
  const a = left ?? AuthAllResourceGrant;
  const b = right ?? AuthAllResourceGrant;
  if (!idsSubset(a.projects, b.projects)) return false;
  if (b.providers.mode === "all") return true;
  if (a.providers.mode === "all") return false;
  const allowed = new Map(b.providers.instances.map((entry) => [entry.instanceId, entry.models]));
  return a.providers.instances.every((entry) => {
    // An explicitly empty model set conveys no execution authority, even for an absent instance.
    if (entry.models.mode === "explicit" && entry.models.ids.length === 0) return true;
    const models = allowed.get(entry.instanceId);
    return models !== undefined && idsSubset(entry.models, models);
  });
}
