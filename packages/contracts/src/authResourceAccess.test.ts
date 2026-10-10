import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AuthBrowserSessionRequest,
  AuthCreatePairingCredentialInput,
  AuthTokenExchangeRequest,
  AuthTokenExchangeGrantType,
  AuthEnvironmentBootstrapTokenType,
  AuthAccessTokenType,
} from "./auth.ts";
import { ProjectId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import {
  AuthAllResourceGrant,
  AuthResourceGrant,
  intersectResourceGrants,
  resourceGrantAllowsModel,
  resourceGrantAllowsProject,
  resourceGrantIsSubset,
} from "./authResourceAccess.ts";

const decodePairingInput = Schema.decodeUnknownSync(AuthCreatePairingCredentialInput);
const decodeBrowserExchange = Schema.decodeUnknownSync(AuthBrowserSessionRequest);
const decodeTokenExchange = Schema.decodeUnknownSync(AuthTokenExchangeRequest);

const decode = Schema.decodeUnknownSync(AuthResourceGrant);
const selected = decode({
  version: 1,
  projects: { mode: "explicit", ids: ["project-a", "deleted-project"] },
  providers: {
    mode: "explicit",
    instances: [
      { instanceId: "codex_personal", models: { mode: "explicit", ids: ["canonical-model"] } },
      { instanceId: "codex_work", models: { mode: "all" } },
      { instanceId: "deleted_instance", models: { mode: "explicit", ids: [] } },
    ],
  },
});
const empty = decode({
  version: 1,
  projects: { mode: "explicit", ids: [] },
  providers: { mode: "explicit", instances: [] },
});

describe("resource grants", () => {
  it("rejects grant input on exchanges instead of silently minting unrestricted credentials", () => {
    expect(decodeBrowserExchange({ credential: "legacy" })).toEqual({ credential: "legacy" });
    for (const resourceGrant of [AuthAllResourceGrant, selected, empty, null, {}]) {
      expect(() => decodeBrowserExchange({ credential: "legacy", resourceGrant })).toThrow();
      expect(() =>
        decodeTokenExchange({
          grant_type: AuthTokenExchangeGrantType,
          subject_token: "legacy",
          subject_token_type: AuthEnvironmentBootstrapTokenType,
          requested_token_type: AuthAccessTokenType,
          resourceGrant: JSON.stringify(resourceGrant),
        }),
      ).toThrow();
    }
  });
  it.each([AuthAllResourceGrant, selected, empty])(
    "round-trips all, explicit and empty grants",
    (grant) => {
      const json = Schema.fromJsonString(AuthResourceGrant);
      expect(Schema.decodeUnknownSync(json)(Schema.encodeSync(json)(grant))).toEqual(grant);
      expect(decodePairingInput({ resourceGrant: grant })).toEqual({ resourceGrant: grant });
    },
  );

  it("distinguishes configured instances and never widens explicit selections", () => {
    expect(resourceGrantAllowsProject(selected, ProjectId.make("project-a"))).toBe(true);
    expect(resourceGrantAllowsProject(selected, ProjectId.make("future-project"))).toBe(false);
    expect(
      resourceGrantAllowsModel(
        selected,
        ProviderInstanceId.make("codex_personal"),
        "canonical-model",
      ),
    ).toBe(true);
    expect(
      resourceGrantAllowsModel(selected, ProviderInstanceId.make("codex_personal"), "future-model"),
    ).toBe(false);
    expect(
      resourceGrantAllowsModel(selected, ProviderInstanceId.make("codex_work"), "future-model"),
    ).toBe(true);
    expect(
      resourceGrantAllowsModel(
        selected,
        ProviderInstanceId.make("future_instance"),
        "canonical-model",
      ),
    ).toBe(false);
    expect(
      resourceGrantAllowsModel(
        selected,
        ProviderInstanceId.make("deleted_instance"),
        "canonical-model",
      ),
    ).toBe(false);
    expect(resourceGrantAllowsProject(empty, ProjectId.make("project-a"))).toBe(false);
    expect(
      resourceGrantAllowsModel(empty, ProviderInstanceId.make("codex_work"), "canonical-model"),
    ).toBe(false);
  });

  it.each([undefined, null, AuthAllResourceGrant])(
    "keeps legacy and all grants open to future IDs",
    (grant) => {
      expect(resourceGrantAllowsProject(grant, ProjectId.make("future-project"))).toBe(true);
      expect(
        resourceGrantAllowsModel(grant, ProviderInstanceId.make("future_instance"), "future-model"),
      ).toBe(true);
    },
  );

  it("intersects nested grants without widening either operand", () => {
    const other = decode({
      version: 1,
      projects: { mode: "explicit", ids: ["project-a", "project-b"] },
      providers: {
        mode: "explicit",
        instances: [
          { instanceId: "codex_personal", models: { mode: "explicit", ids: ["other-model"] } },
          { instanceId: "codex_work", models: { mode: "explicit", ids: ["work-model"] } },
        ],
      },
    });
    const intersection = intersectResourceGrants(selected, other);
    expect(intersection.projects).toEqual({ mode: "explicit", ids: ["project-a"] });
    expect(
      resourceGrantAllowsModel(
        intersection,
        ProviderInstanceId.make("codex_personal"),
        "canonical-model",
      ),
    ).toBe(false);
    expect(
      resourceGrantAllowsModel(intersection, ProviderInstanceId.make("codex_work"), "work-model"),
    ).toBe(true);
    expect(
      resourceGrantAllowsModel(intersection, ProviderInstanceId.make("codex_work"), "future-model"),
    ).toBe(false);
    expect(resourceGrantIsSubset(intersection, selected)).toBe(true);
    expect(resourceGrantIsSubset(intersection, other)).toBe(true);
    expect(resourceGrantIsSubset(selected, other)).toBe(false);
    expect(resourceGrantIsSubset(empty, selected)).toBe(true);
    expect(resourceGrantIsSubset(null, selected)).toBe(false);
    expect(resourceGrantIsSubset(selected, undefined)).toBe(true);
    expect(intersectResourceGrants(null, selected)).toEqual(selected);
    expect(intersectResourceGrants(selected, undefined)).toEqual(selected);
  });

  it.each([
    null,
    {},
    { ...selected, version: 2 },
    { ...selected, projects: { mode: "explicit" } },
    {
      ...selected,
      providers: {
        mode: "explicit",
        instances: [{ instanceId: "codex", models: { mode: "explicit", ids: [""] } }],
      },
    },
    {
      ...selected,
      providers: {
        mode: "explicit",
        instances: [
          { instanceId: "codex", models: { mode: "all" } },
          { instanceId: "codex", models: { mode: "explicit", ids: [] } },
        ],
      },
    },
  ])("rejects malformed explicit grants", (grant) => {
    expect(() => decode(grant)).toThrow();
    expect(() => decodePairingInput({ resourceGrant: grant })).toThrow();
  });
});
