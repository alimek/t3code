import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

// SQL NULL is the legacy unrestricted grant. JSON null is not a valid explicit grant.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const table of ["auth_pairing_links", "auth_sessions"]) {
    yield* sql`ALTER TABLE ${sql(table)} ADD COLUMN resource_grant TEXT`;
    yield* sql`ALTER TABLE ${sql(table)} ADD COLUMN authority_revision INTEGER`;
    yield* sql`ALTER TABLE ${sql(table)} ADD COLUMN device_id TEXT`;
  }
});
