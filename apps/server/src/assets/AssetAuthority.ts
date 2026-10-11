import {
  AuthSessionId,
  AuthEnvironmentScope,
  AuthResourceGrant,
  AssetResource,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  AuthFilesystemReadScope,
  ProjectId,
  ThreadId,
  resourceGrantAllowsProject,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Clock from "effect/Clock";
import * as SqlClient from "effect/sql/SqlClient";
import * as SessionStore from "../auth/SessionStore.ts";
import {
  authorizeOwnerThreadIds,
  threadReadDenied,
  trustedReadAuthority,
  type ThreadReadAuthority,
} from "../auth/ResourceAuthorization.ts";
import { resourceRestricted, authorizeExtraTool } from "../auth/ToolAuthorization.ts";
import * as WorkspaceAccess from "../auth/workspaceReadAccess.ts";
import {
  parseThreadSegmentFromAttachmentId,
  toSafeThreadAttachmentSegment,
} from "../attachmentStore.ts";

/** Signed capabilities retain the issuing connection's ceiling and live device revision. */
export const AssetBinding = Schema.Struct({
  sessionId: AuthSessionId,
  deviceId: AuthSessionId,
  authorityRevision: Schema.Number,
  scopes: Schema.Array(AuthEnvironmentScope),
  resourceGrant: AuthResourceGrant,
});
export type AssetBinding = typeof AssetBinding.Type;
export const encodeBinding = Schema.encodeSync(Schema.fromJsonString(AssetBinding));
const decodeBinding = Schema.decodeUnknownEffect(Schema.fromJsonString(AssetBinding));

export const current = Effect.fnUntraced(function* (
  authority: ThreadReadAuthority,
  scope: AuthEnvironmentScope,
) {
  if (authority === trustedReadAuthority) return undefined;
  const value = yield* authority.current;
  if (!value.scopes.includes(scope)) return yield* threadReadDenied();
  return value;
});
export const captureBinding = Effect.fnUntraced(function* (
  authority: ThreadReadAuthority,
  scope: AuthEnvironmentScope,
) {
  const value = yield* current(authority, scope);
  if (value === undefined) return undefined;
  if (
    value.sessionId === undefined ||
    value.deviceId === undefined ||
    value.authorityRevision === undefined
  )
    return yield* threadReadDenied();
  return {
    sessionId: value.sessionId,
    deviceId: value.deviceId,
    authorityRevision: value.authorityRevision,
    scopes: value.scopes,
    resourceGrant: value.resourceGrant,
  } satisfies AssetBinding;
});
export const bindingAuthority = Effect.fnUntraced(function* (binding: AssetBinding) {
  const service = yield* Effect.serviceOption(SessionStore.SessionStore);
  if (Option.isNone(service)) return yield* threadReadDenied();
  const sessions = service.value;
  const authority: ThreadReadAuthority = {
    current: sessions.getAuthority(binding.sessionId, binding).pipe(
      Effect.mapError(() => threadReadDenied()),
      Effect.flatMap((live) =>
        live.deviceId !== binding.deviceId || live.authorityRevision !== binding.authorityRevision
          ? Effect.fail(threadReadDenied())
          : Effect.succeed({ ...live, sessionId: binding.sessionId }),
      ),
    ),
  };
  return authority;
});

/** Pending ids are device-owned; persisted ids must resolve to one registered thread owner. */
export const authorizeAttachment = Effect.fnUntraced(
  function* (
    authority: ThreadReadAuthority,
    attachmentId: string,
    scope: AuthEnvironmentScope,
    projectId?: ProjectId,
  ) {
    const value = yield* current(authority, scope);
    if (value === undefined) return;
    const service = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(service)) return yield* threadReadDenied();
    const sql = service.value;
    const segment = parseThreadSegmentFromAttachmentId(attachmentId);
    if (segment === null) return yield* threadReadDenied();
    if (segment === "pending") {
      const rows = yield* sql<{
        binding_json: string;
        expires_at: number;
      }>`SELECT binding_json, expires_at FROM attachment_upload_authority WHERE attachment_id = ${attachmentId}`;
      if (rows.length === 0) {
        if (!resourceRestricted(value.resourceGrant)) return;
        return yield* threadReadDenied();
      }
      const binding = yield* decodeBinding(rows[0]!.binding_json).pipe(
        Effect.mapError(() => threadReadDenied()),
      );
      if (
        binding.deviceId !== value.deviceId ||
        binding.authorityRevision !== value.authorityRevision ||
        rows[0]!.expires_at <= (yield* Clock.currentTimeMillis)
      )
        return yield* threadReadDenied();
      const source = yield* current(yield* bindingAuthority(binding), scope);
      if (
        source === undefined ||
        (projectId !== undefined && !resourceGrantAllowsProject(source.resourceGrant, projectId))
      )
        return yield* threadReadDenied();
    } else {
      if (!resourceRestricted(value.resourceGrant)) return;
      const rows = yield* sql<{
        thread_id: string;
      }>`SELECT thread_id FROM orchestration_v2_projection_threads`;
      const owners = rows.filter((r) => toSafeThreadAttachmentSegment(r.thread_id) === segment);
      if (owners.length !== 1) return yield* threadReadDenied();
      yield* authorizeOwnerThreadIds(sql, value.resourceGrant, [
        ThreadId.make(owners[0]!.thread_id),
      ]);
    }
    const latest = yield* current(authority, scope);
    if (latest?.authorityRevision !== value.authorityRevision) return yield* threadReadDenied();
  },
  Effect.mapError(() => threadReadDenied()),
);

export const authorizeAsset = Effect.fnUntraced(
  function* (authority: ThreadReadAuthority, resource: AssetResource, workspaceRoot?: string) {
    const scope =
      resource._tag === "workspace-file" ||
      resource._tag === "draft-workspace-file" ||
      resource._tag === "media-file"
        ? AuthFilesystemReadScope
        : AuthOrchestrationReadScope;
    const value = yield* current(authority, scope);
    if (value === undefined) return;
    if (resource._tag === "github-media" || resource._tag === "native-app-icon")
      return yield* authorizeExtraTool(authority, scope);
    if (resource._tag === "attachment")
      return yield* authorizeAttachment(authority, resource.attachmentId, scope);
    if (!resourceRestricted(value.resourceGrant)) return;
    const service = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(service)) return yield* threadReadDenied();
    const access = yield* WorkspaceAccess.make.pipe(
      Effect.provideService(SqlClient.SqlClient, service.value),
    );
    if ("threadId" in resource) yield* access.thread(authority, resource.threadId, scope);
    if (resource._tag === "tool-output-image") {
      const service = yield* Effect.serviceOption(SqlClient.SqlClient);
      if (Option.isNone(service)) return yield* threadReadDenied();
      const sql = service.value;
      const rows =
        yield* sql`SELECT turn_item_id FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ${resource.itemId} AND thread_id = ${resource.threadId}`;
      if (rows.length !== 1) return yield* threadReadDenied();
      return;
    }
    if (!resourceRestricted(value.resourceGrant)) return;
    const cwd = "cwd" in resource ? resource.cwd : workspaceRoot;
    if (cwd === undefined) return yield* threadReadDenied();
    yield* access.admit(
      authority,
      { cwd, ...("threadId" in resource ? { threadId: resource.threadId } : {}) },
      scope,
    );
  },
  Effect.mapError(() => threadReadDenied()),
);

export const authorizeUploadClaim = Effect.fnUntraced(function* (
  authority: ThreadReadAuthority,
  attachmentId: string,
  projectId: ProjectId,
) {
  yield* authorizeAttachment(authority, attachmentId, AuthOrchestrationOperateScope, projectId);
});
