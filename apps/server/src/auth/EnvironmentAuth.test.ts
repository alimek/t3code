// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect Crypto cannot sign ES256 DPoP fixtures.
import * as NodeCrypto from "node:crypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  authScopeResponse,
  AuthAdministrativeScopes,
  AuthAllResourceGrant,
  AuthCreatePairingCredentialInput,
  AuthStandardClientScopes,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import {
  computeDpopAccessTokenHash,
  computeDpopJwkThumbprint,
  DpopPublicJwk,
} from "@t3tools/shared/dpop";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerConfig from "../config.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as PersistenceErrors from "../persistence/Errors.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as PairingGrantStore from "./PairingGrantStore.ts";
import * as EnvironmentAuth from "./EnvironmentAuth.ts";

import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as SessionStore from "./SessionStore.ts";

/** Pinned so dev-mode cookie tests can assert the port-scoped name. */
const decodeDpopJwk = Schema.decodeUnknownSync(DpopPublicJwk);
const decodePairingInput = Schema.decodeUnknownEffect(AuthCreatePairingCredentialInput);

const TEST_SERVER_PORT = 13_773;
const isPairingCredentialIssueError = Schema.is(PairingGrantStore.PairingCredentialIssueError);
const isPersistenceSqlError = Schema.is(PersistenceErrors.PersistenceSqlError);

const layerServerConfig = (overrides?: Partial<ServerConfig.ServerConfig["Service"]>) =>
  Layer.effect(
    ServerConfig.ServerConfig,
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      return {
        ...config,
        ...overrides,
        // Keep the test server deterministic even when the default test layer
        // changes its development port.
        port: TEST_SERVER_PORT,
      } satisfies ServerConfig.ServerConfig["Service"];
    }),
  ).pipe(Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-auth-server-test-" })));

const layerEnvironmentAuth = (overrides?: Partial<ServerConfig.ServerConfig["Service"]>) =>
  EnvironmentAuth.layer.pipe(
    Layer.provideMerge(SqlitePersistence.layerMemory),
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(ServerEnvironment.layerIdentity),
    Layer.provide(layerServerConfig(overrides)),
  );

const makeCookieRequest = (
  cookieName: string,
  sessionToken: string,
): Parameters<EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]>[0] =>
  ({
    cookies: {
      [cookieName]: sessionToken,
    },
    headers: {},
  }) as unknown as Parameters<
    EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]
  >[0];

const makeBearerRequest = (
  token: string,
): Parameters<EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]>[0] =>
  ({
    cookies: {},
    headers: {
      authorization: `Bearer ${token}`,
    },
  }) as unknown as Parameters<
    EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]
  >[0];

const requestMetadata = {
  deviceType: "desktop" as const,
  os: "macOS",
  browser: "Chrome",
  ipAddress: "192.168.1.23",
};

it.layer(NodeServices.layer)("EnvironmentAuth.layer", (it) => {
  it.effect("cookie, bearer, DPoP, refresh and WS reconnect use one current device authority", () =>
    Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const ownerToken = yield* auth.issueSession();
      const owner = yield* auth.authenticateHttpRequest(makeBearerRequest(ownerToken.token));
      const pairing = yield* auth.issueClientPairingCredential(owner);
      const cookie = yield* auth.createBrowserSession(pairing.credential, requestMetadata);
      const cookieRequest = makeCookieRequest(sessions.cookieName, cookie.sessionToken);
      const root = yield* auth.authenticateHttpRequest(cookieRequest);
      const bearer = yield* auth.exchangeBootstrapCredentialForAccessToken(
        cookie.sessionToken,
        ["orchestration:read"],
        requestMetadata,
      );
      const derivedCookie = yield* auth.createBrowserSession(bearer.access_token, requestMetadata);
      const derivedCookieRequest = makeCookieRequest(
        sessions.cookieName,
        derivedCookie.sessionToken,
      );
      expect((yield* auth.authenticateHttpRequest(derivedCookieRequest)).deviceId).toBe(
        root.sessionId,
      );
      const key = NodeCrypto.generateKeyPairSync("ec", { namedCurve: "P-256" });
      const jwk = decodeDpopJwk(key.publicKey.export({ format: "jwk" }));
      const thumbprint = computeDpopJwkThumbprint(jwk);
      const dpop = yield* auth.exchangeBootstrapCredentialForAccessToken(
        bearer.access_token,
        ["orchestration:read", "orchestration:operate"],
        requestMetadata,
        { proofKeyThumbprint: thumbprint },
      );
      let proofId = 0;
      const dpopRequest = Effect.gen(function* () {
        const header = Buffer.from(JSON.stringify({ typ: "dpop+jwt", alg: "ES256", jwk })).toString(
          "base64url",
        );
        const payload = Buffer.from(
          JSON.stringify({
            htm: "GET",
            htu: "http://localhost/api/auth/session",
            jti: `authority-${++proofId}`,
            iat: Math.floor((yield* DateTime.now).epochMilliseconds / 1000),
            ath: computeDpopAccessTokenHash(dpop.access_token),
          }),
        ).toString("base64url");
        const signature = NodeCrypto.sign("sha256", Buffer.from(`${header}.${payload}`), {
          key: key.privateKey,
          dsaEncoding: "ieee-p1363",
        }).toString("base64url");
        return HttpServerRequest.fromWeb(
          new Request("http://localhost/api/auth/session", {
            headers: {
              authorization: `DPoP ${dpop.access_token}`,
              dpop: `${header}.${payload}.${signature}`,
            },
          }),
        );
      });
      const bound = yield* auth.authenticateHttpRequest(yield* dpopRequest);
      expect(bound.deviceId).toBe(root.sessionId);
      expect(bound.scopes).toEqual(["orchestration:read"]);
      const ticket = yield* auth.issueWebSocketTicket(bound);
      const wsRequest = HttpServerRequest.fromWeb(
        new Request(`http://localhost/ws?wsTicket=${ticket.ticket}`),
      );
      expect((yield* auth.authenticateWebSocketUpgrade(wsRequest)).deviceId).toBe(root.sessionId);
      const edit = {
        scopes: [] as const,
        resourceGrant: AuthAllResourceGrant,
        authorityRevision: 0,
      };
      const updated = yield* auth.updateClientGrants(owner, root.sessionId, edit);
      expect(updated.authorityRevision).toBe(1);
      expect((yield* auth.getSessionState(cookieRequest)).permissions).toEqual([]);
      expect((yield* auth.getSessionState(derivedCookieRequest)).permissions).toEqual([]);
      expect(
        (yield* auth.authenticateHttpRequest(makeBearerRequest(bearer.access_token))).scopes,
      ).toEqual([]);
      expect((yield* auth.authenticateHttpRequest(yield* dpopRequest)).scopes).toEqual([]);
      expect((yield* auth.authenticateWebSocketUpgrade(wsRequest)).scopes).toEqual([]);
      expect((yield* auth.getSessionState(cookieRequest)).deviceId).toBe(root.sessionId);
      yield* auth.updateClientGrants(owner, root.sessionId, {
        ...edit,
        scopes: [...AuthStandardClientScopes],
        authorityRevision: 1,
      });
      const refreshed = yield* auth.exchangeBootstrapCredentialForAccessToken(
        dpop.access_token,
        undefined,
        requestMetadata,
        { proofKeyThumbprint: thumbprint },
      );
      expect((yield* sessions.verify(refreshed.access_token)).scopes).toEqual([
        "orchestration:read",
      ]);
      expect(
        (yield* Effect.flip(auth.createBrowserSession(dpop.access_token, requestMetadata)))._tag,
      ).toBe("ServerAuthInvalidCredentialError");
      expect(
        (yield* Effect.flip(
          auth.exchangeBootstrapCredentialForAccessToken(
            dpop.access_token,
            undefined,
            requestMetadata,
            { proofKeyThumbprint: "other-key" },
          ),
        ))._tag,
      ).toBe("ServerAuthInvalidCredentialError");
      yield* auth.revokeSession(bound.sessionId);
      for (const request of [
        cookieRequest,
        derivedCookieRequest,
        makeBearerRequest(bearer.access_token),
        yield* dpopRequest,
      ]) {
        expect((yield* Effect.flip(auth.authenticateHttpRequest(request)))._tag).toBe(
          "ServerAuthInvalidCredentialError",
        );
      }
      expect((yield* Effect.flip(auth.authenticateWebSocketUpgrade(wsRequest)))._tag).toBe(
        "ServerAuthInvalidCredentialError",
      );
      expect(
        (yield* auth.authenticateHttpRequest(makeBearerRequest(ownerToken.token))).scopes,
      ).toEqual(AuthAdministrativeScopes);
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect(
    "owner checks, pending invitation validity, gate and cloud remint stay inside auth service",
    () =>
      Effect.gen(function* () {
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const sessions = yield* SessionStore.SessionStore;
        const ownerToken = yield* auth.issueSession();
        const owner = yield* auth.authenticateHttpRequest(makeBearerRequest(ownerToken.token));
        const nonOwnerToken = yield* auth.issueSession({ scopes: ["orchestration:read"] });
        const nonOwner = yield* auth.authenticateHttpRequest(
          makeBearerRequest(nonOwnerToken.token),
        );
        expect((yield* Effect.flip(auth.issueClientPairingCredential(nonOwner)))._tag).toBe(
          "ServerAuthAuthorityUpdateError",
        );
        const limited = yield* auth.issueSession({
          scopes: ["access:write", "orchestration:read"],
        });
        const limitedIssuer = yield* auth.authenticateHttpRequest(makeBearerRequest(limited.token));
        expect(
          (yield* Effect.flip(
            auth.issueClientPairingCredential(limitedIssuer, { scopes: ["orchestration:operate"] }),
          ))._tag,
        ).toBe("ServerAuthAuthorityUpdateError");
        const pending = yield* auth.issueClientPairingCredential(limitedIssuer, {
          scopes: ["orchestration:read"],
        });
        yield* auth.updateClientGrants(owner, limited.sessionId, {
          scopes: ["access:write"],
          resourceGrant: AuthAllResourceGrant,
          authorityRevision: 0,
        });
        expect(
          (yield* Effect.flip(auth.createBrowserSession(pending.credential, requestMetadata)))._tag,
        ).toBe("ServerAuthInvalidCredentialError");
        const edit = {
          scopes: ["orchestration:read"] as const,
          resourceGrant: AuthAllResourceGrant,
          authorityRevision: 0,
        };
        expect(
          (yield* Effect.flip(auth.updateClientGrants(nonOwner, nonOwner.sessionId, edit)))._tag,
        ).toBe("ServerAuthAuthorityUpdateError");
        const restricted = yield* decodePairingInput({
          resourceGrant: {
            version: 1,
            projects: { mode: "explicit", ids: [] },
            providers: { mode: "all" },
          },
        });
        const cloud = yield* auth.issueSession({ subject: "cloud-connect" });
        expect(
          (yield* Effect.flip(
            auth.updateClientGrants(owner, cloud.sessionId, {
              ...edit,
              resourceGrant: restricted.resourceGrant!,
            }),
          ))._tag,
        ).toBe("ServerAuthAuthorityUpdateError");
        expect(
          (yield* Effect.flip(
            auth.updateClientGrants(owner, nonOwner.sessionId, {
              ...edit,
              resourceGrant: restricted.resourceGrant!,
            }),
          ))._tag,
        ).toBe("ServerAuthSessionCredentialValidationError");
        const remint = yield* auth.issueSession({
          subject: "cloud-connect",
          scopes: AuthStandardClientScopes,
        });
        expect((yield* sessions.verify(remint.token)).deviceId).toBe(remint.sessionId);
        expect((yield* sessions.verify(cloud.token)).deviceId).toBe(cloud.sessionId);
        const revokedLink = yield* auth.issueClientPairingCredential(owner, {
          scopes: ["orchestration:read"],
        });
        yield* auth.revokeSession(owner.sessionId);
        expect(
          (yield* Effect.flip(auth.createBrowserSession(revokedLink.credential, requestMetadata)))
            ._tag,
        ).toBe("ServerAuthInvalidCredentialError");
      }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect("rejects restricted grant requests through every issuance API", () =>
    Effect.gen(function* () {
      const auth = yield* EnvironmentAuth.EnvironmentAuth;
      const sql = yield* SqlClient.SqlClient;
      const input = yield* decodePairingInput({
        resourceGrant: {
          version: 1,
          projects: { mode: "explicit", ids: [] },
          providers: { mode: "all" },
        },
      });
      expect((yield* Effect.flip(auth.issuePairingCredential(input)))._tag).toBe(
        "ServerAuthPairingLinkCreationError",
      );
      expect((yield* Effect.flip(auth.createPairingLink(input)))._tag).toBe(
        "ServerAuthPairingLinkCreationError",
      );
      expect((yield* Effect.flip(auth.issueSession(input)))._tag).toBe(
        "ServerAuthSessionTokenIssueError",
      );
      expect(yield* sql`SELECT id FROM auth_pairing_links`).toEqual([]);
      expect(yield* sql`SELECT session_id FROM auth_sessions`).toEqual([]);
      const all = yield* auth.issuePairingCredential({ resourceGrant: AuthAllResourceGrant });
      const exchanged = yield* auth.createBrowserSession(all.credential, requestMetadata);
      expect(exchanged.response.permissions).toEqual(AuthStandardClientScopes);
      expect(exchanged.response).not.toHaveProperty("resourceGrant");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect(
    "does not convert stored restricted pairing links into cookie, bearer or DPoP sessions",
    () =>
      Effect.gen(function* () {
        const auth = yield* EnvironmentAuth.EnvironmentAuth;
        const sql = yield* SqlClient.SqlClient;
        const restricted = JSON.stringify({
          version: 1,
          projects: { mode: "all" },
          providers: { mode: "explicit", instances: [] },
        });
        for (const method of ["cookie", "bearer", "dpop"]) {
          const link = yield* auth.createPairingLink();
          yield* sql`UPDATE auth_pairing_links SET resource_grant = ${restricted} WHERE id = ${link.id}`;
          const operation =
            method === "cookie"
              ? auth.createBrowserSession(link.credential, requestMetadata).pipe(Effect.asVoid)
              : auth
                  .exchangeBootstrapCredentialForAccessToken(
                    link.credential,
                    undefined,
                    requestMetadata,
                    method === "dpop" ? { proofKeyThumbprint: "proof-key" } : undefined,
                  )
                  .pipe(Effect.asVoid);
          expect((yield* Effect.flip(operation))._tag).toBe(
            "ServerAuthBootstrapCredentialValidationError",
          );
        }
        expect(yield* sql`SELECT session_id FROM auth_sessions`).toEqual([]);
      }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect("uses the reusable dev cookie without overriding a normal scoped cookie", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const devExchange = yield* serverAuth.createBrowserSession(token, requestMetadata);
      const pairing = yield* serverAuth.issuePairingCredential({ scopes: ["orchestration:read"] });
      const scopedExchange = yield* serverAuth.createBrowserSession(
        pairing.credential,
        requestMetadata,
      );
      const request = {
        cookies: {
          [sessions.cookieName]: scopedExchange.sessionToken,
          [devExchange.cookieName ?? "missing"]: devExchange.sessionToken,
        },
        headers: {},
      } as unknown as Parameters<
        EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]
      >[0];

      const authenticated = yield* serverAuth.authenticateHttpRequest(request);
      expect(devExchange.cookieName).toMatch(/^t3_dev_session_/);
      expect(devExchange.expireNormalCookie).toBe(true);
      expect(devExchange.response).toMatchObject(authScopeResponse(AuthAdministrativeScopes));
      expect(authenticated.scopes).toEqual(["orchestration:read"]);
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("does not fall back to the dev cookie after a normal cookie is rejected", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const devExchange = yield* serverAuth.createBrowserSession(token, requestMetadata);
      const pairing = yield* serverAuth.issuePairingCredential({ scopes: ["orchestration:read"] });
      const scopedExchange = yield* serverAuth.createBrowserSession(
        pairing.credential,
        requestMetadata,
      );
      const scoped = yield* sessions.verify(scopedExchange.sessionToken);
      yield* sessions.revoke(scoped.sessionId);
      const request = {
        cookies: {
          [sessions.cookieName]: scopedExchange.sessionToken,
          [devExchange.cookieName ?? "missing"]: devExchange.sessionToken,
        },
        headers: {},
      } as unknown as Parameters<
        EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]
      >[0];

      const error = yield* Effect.flip(serverAuth.authenticateHttpRequest(request));
      expect(error._tag).toBe("ServerAuthInvalidCredentialError");
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("does not use the dev cookie when Authorization is invalid or empty", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const devExchange = yield* serverAuth.createBrowserSession(token, requestMetadata);
      for (const authorization of ["Bearer invalid", ""] as const) {
        const request = {
          cookies: { [devExchange.cookieName ?? "missing"]: devExchange.sessionToken },
          headers: { authorization },
        } as unknown as Parameters<
          EnvironmentAuth.EnvironmentAuth["Service"]["authenticateHttpRequest"]
        >[0];
        const error = yield* Effect.flip(serverAuth.authenticateHttpRequest(request));
        expect(EnvironmentAuth.isServerAuthCredentialError(error)).toBe(true);
      }
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("exchanges the reusable dev token for a local scoped OAuth session", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const exchanged = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        token,
        ["orchestration:read"],
        requestMetadata,
      );

      expect(exchanged.access_token).not.toBe(token);
      expect(exchanged.scope).toBe("orchestration:read");
      const dpop = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        token,
        ["orchestration:read"],
        requestMetadata,
        { proofKeyThumbprint: "test-proof-key" },
      );
      expect(dpop.access_token).not.toBe(token);
      expect(dpop.access_token).not.toBe(exchanged.access_token);
      expect(dpop.token_type).toBe("DPoP");
      expect(dpop.scope).toBe("orchestration:read");

      const secondBearer = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        token,
        ["orchestration:read"],
        requestMetadata,
      );
      const firstSession = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(exchanged.access_token),
      );
      const secondSession = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(secondBearer.access_token),
      );
      expect(firstSession.subject).toBe("reusable-dev-token-child");
      expect(secondSession.subject).toBe("reusable-dev-token-child");
      expect((yield* sessions.verify(token)).subject).toBe("reusable-dev-token");
      const before = yield* serverAuth.listClientSessions(firstSession.sessionId);
      const denied = yield* serverAuth
        .exchangeBootstrapCredentialForAccessToken(token, ["review:write"], requestMetadata)
        .pipe(Effect.flip);
      expect(denied._tag).toBe("ServerAuthScopeNotGrantedError");
      const empty = yield* serverAuth
        .exchangeBootstrapCredentialForAccessToken(token, [], requestMetadata)
        .pipe(Effect.flip);
      expect(empty._tag).toBe("ServerAuthScopeNotGrantedError");
      expect(yield* serverAuth.listClientSessions(firstSession.sessionId)).toEqual(before);
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("uses a one-time startup credential after local dev token revocation", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const initial = yield* serverAuth.issueStartupPairingCredential();
      const seeded = yield* sessions.verify(token);

      yield* sessions.revoke(seeded.sessionId);
      const recovery = yield* serverAuth.issueStartupPairingCredential();

      expect(initial.credential).toBe(token);
      expect(recovery.credential).not.toBe(token);
      expect((yield* Effect.flip(sessions.verify(token)))._tag).toBe("SessionTokenRevokedError");
      expect(
        (yield* serverAuth.createBrowserSession(recovery.credential, requestMetadata)).response,
      ).toMatchObject({ authenticated: true, ...authScopeResponse(AuthAdministrativeScopes) });
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("keeps the pairing issue error as the immediate recovery failure", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const sql = yield* SqlClient.SqlClient;
      const token = "reusable-dev-auth-token-that-is-long-enough";
      const seeded = yield* sessions.verify(token);

      yield* sessions.revoke(seeded.sessionId);
      yield* sql`
        CREATE TRIGGER reject_startup_pairing_link
        BEFORE INSERT ON auth_pairing_links
        BEGIN
          SELECT RAISE(ABORT, 'startup pairing insert rejected');
        END
      `;

      const error = yield* Effect.flip(serverAuth.issueStartupPairingCredential());

      expect(error._tag).toBe("ServerAuthPairingLinkCreationError");
      expect(isPairingCredentialIssueError(error.cause)).toBe(true);
      if (isPairingCredentialIssueError(error.cause)) {
        expect(isPersistenceSqlError(error.cause.cause)).toBe(true);
      }
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          mode: "web",
          devUrl: new URL("http://127.0.0.1:5173"),
          devAuthToken: Redacted.make("reusable-dev-auth-token-that-is-long-enough"),
        }),
      ),
    ),
  );

  it.effect("classifies invalid bootstrap credential failures for the HTTP boundary", () =>
    Effect.sync(() => {
      const error = EnvironmentAuth.toBootstrapExchangeError(
        new PairingGrantStore.UnknownBootstrapCredentialError({}),
      );

      expect(error._tag).toBe("ServerAuthInvalidCredentialError");
    }),
  );

  it.effect("maps unexpected bootstrap failures to 500", () =>
    Effect.sync(() => {
      const cause = new PairingGrantStore.BootstrapCredentialConsumeError({
        cause: new Error("sqlite is unavailable"),
      });
      const error = EnvironmentAuth.toBootstrapExchangeError(cause);

      expect(error._tag).toBe("ServerAuthBootstrapCredentialValidationError");
      expect(error.message).toBe("Failed to validate bootstrap credential.");
      if (error._tag === "ServerAuthBootstrapCredentialValidationError") {
        expect(error.cause).toBe(cause);
      }
    }),
  );

  it.effect("issues standard pairing credentials by default", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;

      const pairingCredential = yield* serverAuth.issuePairingCredential();
      const exchanged = yield* serverAuth.createBrowserSession(
        pairingCredential.credential,
        requestMetadata,
      );
      const verified = yield* serverAuth.authenticateHttpRequest(
        makeCookieRequest(sessions.cookieName, exchanged.sessionToken),
      );

      expect(verified.sessionId.length).toBeGreaterThan(0);
      expect(verified.scopes).toEqual(AuthStandardClientScopes);
      expect(verified.subject).toBe("one-time-token");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect("prefers a bearer token over a stale legacy cookie", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const bearer = yield* serverAuth.issueSession();
      const verified = yield* serverAuth.authenticateHttpRequest({
        cookies: { [sessions.legacyCookieName ?? "t3_session"]: "stale" },
        headers: { authorization: `Bearer ${bearer.token}` },
      } as never);

      expect(verified.sessionId).toBe(bearer.sessionId);
    }).pipe(Effect.provide(layerEnvironmentAuth({ mode: "web", host: "192.168.1.50" }))),
  );

  it.effect("preserves pairing grants after rejecting scopes they do not grant", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const pairingCredential = yield* serverAuth.issuePairingCredential();

      const error = yield* serverAuth
        .exchangeBootstrapCredentialForAccessToken(
          pairingCredential.credential,
          ["access:write"],
          requestMetadata,
        )
        .pipe(Effect.flip);

      expect(error._tag).toBe("ServerAuthScopeNotGrantedError");
      expect((yield* serverAuth.listPairingLinks()).map((link) => link.id)).toContain(
        pairingCredential.id,
      );
      expect(yield* serverAuth.listSessions()).toEqual([]);

      const token = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        pairingCredential.credential,
        ["orchestration:read"],
        requestMetadata,
      );
      const session = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(token.access_token),
      );

      expect(token.scope).toBe("orchestration:read");
      expect(session.scopes).toEqual(["orchestration:read"]);
      expect((yield* serverAuth.listPairingLinks()).map((link) => link.id)).not.toContain(
        pairingCredential.id,
      );
      const reused = yield* serverAuth
        .exchangeBootstrapCredentialForAccessToken(
          pairingCredential.credential,
          ["orchestration:read"],
          requestMetadata,
        )
        .pipe(Effect.flip);
      expect(reused._tag).toBe("ServerAuthInvalidCredentialError");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect.each([
    { label: "omits scope", requestedScopes: undefined },
    {
      label: "requests unsupported permissions alongside a granted one",
      requestedScopes: ["orchestration:read", "access:write"] as const,
    },
  ])("inherits a constrained pairing grant when token exchange $label", ({ requestedScopes }) =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const pairingCredential = yield* serverAuth.issuePairingCredential({
        scopes: ["orchestration:read"],
      });

      const token = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        pairingCredential.credential,
        requestedScopes,
        requestMetadata,
      );

      expect(token.scope).toBe("orchestration:read");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect("narrows seeded desktop grants to the requested scopes", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const token = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        "desktop-bootstrap-token",
        ["orchestration:read"],
        requestMetadata,
      );
      const session = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(token.access_token),
      );

      expect(token.scope).toBe("orchestration:read");
      expect(session.scopes).toEqual(["orchestration:read"]);
    }).pipe(
      Effect.provide(layerEnvironmentAuth({ desktopBootstrapToken: "desktop-bootstrap-token" })),
    ),
  );

  it.effect("rotates desktop bearer sessions without accumulating authorized clients", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;
      const browser = yield* serverAuth.createBrowserSession(
        "desktop-bootstrap-token",
        requestMetadata,
      );
      const browserSession = yield* serverAuth.authenticateHttpRequest(
        makeCookieRequest(sessions.cookieName, browser.sessionToken),
      );
      const staleSessions = yield* Effect.forEach([1, 2, 3], () =>
        sessions.issue({ subject: "desktop-bootstrap", method: "bearer-access-token" }),
      );
      const pairing = yield* serverAuth.issuePairingCredential();
      const paired = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        pairing.credential,
        undefined,
        { ...requestMetadata, label: "T3 Code Desktop" },
      );
      const first = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        "desktop-bootstrap-token",
        undefined,
        requestMetadata,
      );
      const firstSession = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(first.access_token),
      );
      const second = yield* serverAuth.exchangeBootstrapCredentialForAccessToken(
        "desktop-bootstrap-token",
        undefined,
        requestMetadata,
      );

      const active = yield* serverAuth.listSessions();
      const firstError = yield* serverAuth
        .authenticateHttpRequest(makeBearerRequest(first.access_token))
        .pipe(Effect.flip);
      const secondSession = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(second.access_token),
      );

      expect(active).toHaveLength(3);
      expect(active.map((entry) => entry.sessionId)).toContain(browserSession.sessionId);
      expect(active.map((entry) => entry.sessionId)).toContain(secondSession.sessionId);
      expect(active.map((entry) => entry.sessionId)).not.toContain(firstSession.sessionId);
      expect(firstError._tag).toBe("ServerAuthInvalidCredentialError");
      for (const stale of staleSessions) {
        const error = yield* sessions.verify(stale.token).pipe(Effect.flip);
        expect(error._tag).toBe("SessionTokenRevokedError");
      }
      const pairedSession = yield* serverAuth.authenticateHttpRequest(
        makeBearerRequest(paired.access_token),
      );
      expect(pairedSession.subject).toBe("one-time-token");
      expect(active.map((entry) => entry.sessionId)).toContain(pairedSession.sessionId);
    }).pipe(
      Effect.provide(
        layerEnvironmentAuth({
          desktopBootstrapToken: "desktop-bootstrap-token",
        }),
      ),
    ),
  );

  it.effect("keeps user-issued administrative pairing links manageable", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const pairingCredential = yield* serverAuth.issuePairingCredential({
        scopes: AuthAdministrativeScopes,
      });
      const listedPairingLinks = yield* serverAuth.listPairingLinks();

      expect(
        listedPairingLinks.find((pairingLink) => pairingLink.id === pairingCredential.id)?.subject,
      ).toBe("one-time-token");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect("issues startup pairing URLs that bootstrap administrative sessions", () =>
    Effect.gen(function* () {
      const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
      const sessions = yield* SessionStore.SessionStore;

      const pairingUrl = yield* serverAuth.issueStartupPairingUrl("http://127.0.0.1:3773");
      const token = new URLSearchParams(new URL(pairingUrl).hash.slice(1)).get("token");
      const listedPairingLinks = yield* serverAuth.listPairingLinks();
      expect(token).toBeTruthy();
      expect(
        listedPairingLinks.some(
          (pairingLink) => pairingLink.subject === "administrative-bootstrap",
        ),
      ).toBe(false);

      const exchanged = yield* serverAuth.createBrowserSession(token ?? "", requestMetadata);
      const verified = yield* serverAuth.authenticateHttpRequest(
        makeCookieRequest(sessions.cookieName, exchanged.sessionToken),
      );

      expect(verified.scopes).toEqual(AuthAdministrativeScopes);
      expect(verified.subject).toBe("administrative-bootstrap");
    }).pipe(Effect.provide(layerEnvironmentAuth())),
  );

  it.effect(
    "lists pairing links and revokes other sessions while keeping the administrative session",
    () =>
      Effect.gen(function* () {
        const serverAuth = yield* EnvironmentAuth.EnvironmentAuth;
        const sessions = yield* SessionStore.SessionStore;

        const administrativeExchange = yield* serverAuth.createBrowserSession(
          "desktop-bootstrap-token",
          requestMetadata,
        );
        const administrativeSession = yield* serverAuth.authenticateHttpRequest(
          makeCookieRequest(sessions.cookieName, administrativeExchange.sessionToken),
        );
        const pairingCredential = yield* serverAuth.issuePairingCredential({
          label: "Julius iPhone",
        });
        const listedPairingLinks = yield* serverAuth.listPairingLinks();
        const clientExchange = yield* serverAuth.createBrowserSession(
          pairingCredential.credential,
          {
            ...requestMetadata,
            deviceType: "mobile",
            os: "iOS",
            browser: "Safari",
            ipAddress: "192.168.1.88",
          },
        );
        const clientSession = yield* serverAuth.authenticateHttpRequest(
          makeCookieRequest(sessions.cookieName, clientExchange.sessionToken),
        );
        const clientsBeforeRevoke = yield* serverAuth.listClientSessions(
          administrativeSession.sessionId,
        );
        const revokedCount = yield* serverAuth.revokeOtherClientSessions(
          administrativeSession.sessionId,
        );
        const clientsAfterRevoke = yield* serverAuth.listClientSessions(
          administrativeSession.sessionId,
        );

        expect(listedPairingLinks.map((entry) => entry.id)).toContain(pairingCredential.id);
        expect(listedPairingLinks.find((entry) => entry.id === pairingCredential.id)?.label).toBe(
          "Julius iPhone",
        );
        expect(clientsBeforeRevoke).toHaveLength(2);
        expect(
          clientsBeforeRevoke.find((entry) => entry.sessionId === administrativeSession.sessionId)
            ?.current,
        ).toBe(true);
        expect(
          clientsBeforeRevoke.find((entry) => entry.sessionId === clientSession.sessionId)?.current,
        ).toBe(false);
        expect(
          clientsBeforeRevoke.find((entry) => entry.sessionId === clientSession.sessionId)?.client
            .label,
        ).toBe("Julius iPhone");
        expect(
          clientsBeforeRevoke.find((entry) => entry.sessionId === clientSession.sessionId)?.client
            .deviceType,
        ).toBe("mobile");
        expect(revokedCount).toBe(1);
        expect(clientsAfterRevoke).toHaveLength(1);
        expect(clientsAfterRevoke[0]?.sessionId).toBe(administrativeSession.sessionId);
      }).pipe(
        Effect.provide(
          layerEnvironmentAuth({
            desktopBootstrapToken: "desktop-bootstrap-token",
          }),
        ),
      ),
  );
});
