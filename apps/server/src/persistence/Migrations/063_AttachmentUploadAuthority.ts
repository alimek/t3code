import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE attachment_upload_authority (
    attachment_id TEXT PRIMARY KEY, binding_json TEXT NOT NULL, expires_at INTEGER NOT NULL
  )`;
});
