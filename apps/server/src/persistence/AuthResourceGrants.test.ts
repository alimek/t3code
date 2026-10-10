import { expect, it } from "@effect/vitest";
import { AuthAllResourceGrant, AuthResourceGrant, AuthSessionId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import * as AuthPairingLinks from "./AuthPairingLinks.ts";
import * as AuthSessions from "./AuthSessions.ts";
import { runMigrations } from "./Migrations.ts";
import * as SqlitePersistence from "./Sqlite.ts";

const layer = Layer.merge(AuthPairingLinks.layer, AuthSessions.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const selected = Schema.decodeUnknownSync(AuthResourceGrant)({
  version: 1,
  projects: { mode: "explicit", ids: ["deleted-project"] },
  providers: {
    mode: "explicit",
    instances: [
      { instanceId: "codex_personal", models: { mode: "all" } },
      { instanceId: "codex_work", models: { mode: "explicit", ids: ["canonical-model"] } },
    ],
  },
});
const empty = Schema.decodeUnknownSync(AuthResourceGrant)({
  version: 1,
  projects: { mode: "explicit", ids: [] },
  providers: { mode: "explicit", instances: [] },
});

it.effect("round-trips legacy, all, explicit and empty grants through both repositories", () =>
  Effect.gen(function* () {
    const links = yield* AuthPairingLinks.AuthPairingLinkRepository;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const now = yield* DateTime.now;
    const expiresAt = DateTime.add(now, { hours: 1 });
    for (const [index, grant] of [
      undefined,
      null,
      AuthAllResourceGrant,
      selected,
      empty,
    ].entries()) {
      const id = `grant-${index}`;
      const metadata = {
        ...(grant !== undefined ? { resourceGrant: grant } : {}),
        authorityRevision: 7,
        deviceId: "device-anchor",
      };
      yield* links.create({
        id,
        credential: id,
        method: "one-time-token",
        scopes: ["orchestration:read"],
        subject: id,
        label: null,
        proofKeyThumbprint: null,
        createdAt: now,
        expiresAt,
        ...metadata,
      });
      const session = {
        sessionId: AuthSessionId.make(id),
        subject: id,
        method: "bearer-access-token" as const,
        scopes: ["orchestration:read"] as const,
        client: {
          label: null,
          ipAddress: null,
          userAgent: null,
          deviceType: "unknown" as const,
          os: null,
          browser: null,
        },
        issuedAt: now,
        expiresAt,
        ...metadata,
      };
      yield* sessions.createIfAbsent(session);
      const storedLink = Option.getOrThrow(yield* links.getByCredential({ credential: id }));
      const storedSession = Option.getOrThrow(
        yield* sessions.getById({ sessionId: session.sessionId }),
      );
      expect(storedLink.resourceGrant).toEqual(grant ?? null);
      expect(storedSession.resourceGrant).toEqual(grant ?? null);
      expect(storedLink.authorityRevision).toBe(7);
      expect(storedSession.deviceId).toBe("device-anchor");
      expect(
        Option.getOrThrow(
          yield* links.consumeAvailable({
            credential: id,
            proofKeyThumbprint: null,
            now,
            consumedAt: now,
          }),
        ).resourceGrant,
      ).toEqual(grant ?? null);
      yield* sessions.createReplacingActive({
        session: { ...session, sessionId: AuthSessionId.make(`replacement-${index}`) },
        revokedAt: now,
        replaceSessionId: session.sessionId,
      });
      expect(
        (yield* sessions.listActive({ now })).find(
          (row) => row.sessionId === `replacement-${index}`,
        )?.resourceGrant,
      ).toEqual(grant ?? null);
    }
  }).pipe(Effect.provide(layer)),
);

it.effect("malformed explicit storage fails closed in lookups, lists and consumption", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const links = yield* AuthPairingLinks.AuthPairingLinkRepository;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    const now = yield* DateTime.now;
    const later = DateTime.add(now, { hours: 1 });
    yield* links.create({
      id: "bad",
      credential: "bad",
      method: "one-time-token",
      scopes: [],
      subject: "bad",
      label: null,
      proofKeyThumbprint: null,
      createdAt: now,
      expiresAt: later,
    });
    yield* sessions.create({
      sessionId: AuthSessionId.make("bad"),
      subject: "bad",
      scopes: [],
      method: "bearer-access-token",
      client: {
        label: null,
        ipAddress: null,
        userAgent: null,
        deviceType: "unknown",
        os: null,
        browser: null,
      },
      issuedAt: now,
      expiresAt: later,
    });
    for (const malformed of [
      "{",
      "null",
      "{}",
      JSON.stringify({ ...selected, version: 2 }),
      JSON.stringify({ ...selected, projects: { mode: "explicit" } }),
    ]) {
      yield* sql`UPDATE auth_pairing_links SET resource_grant = ${malformed}, consumed_at = NULL WHERE id = 'bad'`;
      yield* sql`UPDATE auth_sessions SET resource_grant = ${malformed} WHERE session_id = 'bad'`;
      for (const operation of [
        links.getByCredential({ credential: "bad" }).pipe(Effect.asVoid),
        links.listActive({ now }).pipe(Effect.asVoid),
        links
          .consumeAvailable({
            credential: "bad",
            proofKeyThumbprint: null,
            now,
            consumedAt: now,
          })
          .pipe(Effect.asVoid),
        sessions.getById({ sessionId: AuthSessionId.make("bad") }).pipe(Effect.asVoid),
        sessions.listActive({ now }).pipe(Effect.asVoid),
      ]) {
        expect((yield* Effect.flip(operation))._tag).toBe("PersistenceDecodeError");
      }
    }
  }).pipe(Effect.provide(layer)),
);

it.effect("migrations 61 and 62 preserve old rows with unrestricted SQL NULL grants", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 60 });
    yield* sql`INSERT INTO auth_pairing_links (id, credential, method, scopes, subject, created_at, expires_at) VALUES ('legacy', 'legacy', 'one-time-token', '[]', 'legacy', '2026-01-01T00:00:00.000Z', '2099-01-01T00:00:00.000Z')`;
    yield* sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at) VALUES ('legacy', 'legacy', '[]', 'bearer-access-token', '2026-01-01T00:00:00.000Z', '2099-01-01T00:00:00.000Z')`;
    yield* runMigrations({ toMigrationInclusive: 62 });
    const links = yield* AuthPairingLinks.AuthPairingLinkRepository;
    const sessions = yield* AuthSessions.AuthSessionRepository;
    expect(
      Option.getOrThrow(yield* links.getByCredential({ credential: "legacy" })).resourceGrant,
    ).toBeNull();
    expect(
      Option.getOrThrow(yield* sessions.getById({ sessionId: AuthSessionId.make("legacy") }))
        .resourceGrant,
    ).toBeNull();
    expect(yield* runMigrations({ toMigrationInclusive: 62 })).toEqual([]);
  }).pipe(
    Effect.provide(
      Layer.merge(AuthPairingLinks.layer, AuthSessions.layer).pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
      ),
    ),
  ),
);
