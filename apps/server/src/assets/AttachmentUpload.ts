import * as Base64 from "effect/encoding/Base64";
import {
  ChatAttachmentId,
  PersistChatAttachmentsError,
  type PersistChatAttachmentsInput,
  type PersistChatAttachmentsResult,
  type EnvironmentAuthorizationError,
} from "@t3tools/contracts";
import { trustedMutationAuthority, type MutationAuthority } from "../auth/CommandAuthorization.ts";
import { authorizeOwnerThreadIds, threadReadDenied } from "../auth/ResourceAuthorization.ts";
import { attachmentRelativePath, createDeterministicAttachmentId } from "../attachmentStore.ts";
import { parseBase64DataUrl } from "../imageMime.ts";
import { AuthOrchestrationOperateScope } from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import {
  AssetBinding,
  bindingAuthority,
  captureBinding,
  encodeBinding,
  authorizeAttachment,
  current,
} from "./AssetAuthority.ts";
import {
  ATTACHMENT_UPLOAD_URL_TTL_MS,
  type AttachmentCreateUploadUrlInput,
  AttachmentUploadSigningKeyError,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as HttpServerRequest from "effect/http/HttpServerRequest";

import {
  attachmentFileExtension,
  createPendingAttachmentId,
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  resolveAttachmentPathById,
  sweepStalePendingAttachments,
} from "../attachmentStore.ts";
import { resolveAttachmentRelativePath } from "../attachmentPaths.ts";
import {
  base64UrlDecodeUtf8,
  base64UrlEncode,
  signPayload,
  timingSafeEqualBase64Url,
} from "../auth/utils.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import { inferImageExtension } from "../imageMime.ts";

export const ATTACHMENT_UPLOAD_ROUTE_PREFIX = "/api/attachments/upload";

// Asset download tokens share this key, but their signed claim kind is different.
const SIGNING_SECRET_NAME = "asset-access-signing-key";
const PENDING_ATTACHMENT_SWEEP_INTERVAL_MS = 15 * 60_000;
const lastPendingSweepByDirectory = new Map<string, number>();

const AttachmentUploadClaims = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("attachment-upload"),
  type: Schema.Literals(["image", "file"]).pipe(
    Schema.withDecodingDefault(Effect.succeed("image" as const)),
  ),
  attachmentId: Schema.String,
  name: Schema.String,
  mimeType: Schema.String,
  sizeBytes: Schema.Number,
  expiresAt: Schema.Number,
  binding: Schema.optionalKey(AssetBinding),
});
export type AttachmentUploadClaims = typeof AttachmentUploadClaims.Type;

const attachmentUploadClaimsJson = Schema.fromJsonString(AttachmentUploadClaims);
const decodeAttachmentUploadClaims = Schema.decodeUnknownOption(attachmentUploadClaimsJson);
const encodeAttachmentUploadClaims = Schema.encodeSync(attachmentUploadClaimsJson);

export class AttachmentUpload extends Context.Service<
  AttachmentUpload,
  {
    readonly persistChatAttachments: (
      authority: MutationAuthority,
      input: PersistChatAttachmentsInput,
    ) => Effect.Effect<
      PersistChatAttachmentsResult["attachments"],
      PersistChatAttachmentsError | EnvironmentAuthorizationError
    >;
  }
>()("t3/assets/AttachmentUpload") {}

function decodeClaims(encodedPayload: string): AttachmentUploadClaims | null {
  try {
    return Option.getOrNull(decodeAttachmentUploadClaims(base64UrlDecodeUtf8(encodedPayload)));
  } catch {
    return null;
  }
}

const loadSigningSecret = Effect.gen(function* () {
  const secretStore = yield* ServerSecretStore.ServerSecretStore;
  return yield* secretStore.getOrCreateRandom(SIGNING_SECRET_NAME, 32);
});

export const issueAttachmentUploadUrl = Effect.fn("AttachmentUpload.issueUrl")(function* (
  input: AttachmentCreateUploadUrlInput,
  authority: ThreadReadAuthority = trustedReadAuthority,
) {
  const binding = yield* captureBinding(authority, AuthOrchestrationOperateScope);
  const secret = yield* loadSigningSecret.pipe(
    Effect.mapError((cause) => new AttachmentUploadSigningKeyError({ cause })),
  );
  const config = yield* ServerConfig.ServerConfig;
  const nowMs = yield* Clock.currentTimeMillis;
  const previousSweep = lastPendingSweepByDirectory.get(config.attachmentsDir);
  if (
    previousSweep === undefined ||
    nowMs - previousSweep >= PENDING_ATTACHMENT_SWEEP_INTERVAL_MS
  ) {
    lastPendingSweepByDirectory.set(config.attachmentsDir, nowMs);
    const swept = sweepStalePendingAttachments({
      attachmentsDir: config.attachmentsDir,
      nowMs,
    });
    if (swept.deleted > 0) {
      yield* Effect.logInfo("Removed expired attachment uploads.", { deleted: swept.deleted });
    }
  }

  const attachmentType = input.type ?? "image";
  const attachmentId = createPendingAttachmentId(
    attachmentType === "file" ? attachmentFileExtension(input.name) : undefined,
  );
  const expiresAt = nowMs + ATTACHMENT_UPLOAD_URL_TTL_MS;
  if (binding !== undefined) {
    const service = yield* Effect.serviceOption(SqlClient.SqlClient);
    if (Option.isNone(service))
      return yield* new AttachmentUploadSigningKeyError({
        cause: "Upload authority storage unavailable",
      });
    const sql = service.value;
    yield* current(authority, AuthOrchestrationOperateScope);
    yield* sql`DELETE FROM attachment_upload_authority WHERE expires_at <= ${nowMs}`.pipe(
      Effect.mapError((cause) => new AttachmentUploadSigningKeyError({ cause })),
    );
    if (
      (yield* captureBinding(authority, AuthOrchestrationOperateScope))?.authorityRevision !==
      binding.authorityRevision
    )
      return yield* threadReadDenied();
    yield* sql`INSERT INTO attachment_upload_authority (attachment_id, binding_json, expires_at) VALUES (${attachmentId}, ${encodeBinding(binding)}, ${expiresAt})`.pipe(
      Effect.mapError((cause) => new AttachmentUploadSigningKeyError({ cause })),
    );
  }
  const encodedPayload = base64UrlEncode(
    encodeAttachmentUploadClaims({
      version: 1,
      kind: "attachment-upload",
      type: attachmentType,
      attachmentId,
      name: input.name,
      mimeType: input.mimeType,
      sizeBytes: input.sizeBytes,
      expiresAt,
      ...(binding === undefined ? {} : { binding }),
    }),
  );

  if (
    binding !== undefined &&
    (yield* captureBinding(authority, AuthOrchestrationOperateScope))?.authorityRevision !==
      binding.authorityRevision
  )
    return yield* threadReadDenied();
  return {
    attachmentId,
    relativeUrl: `${ATTACHMENT_UPLOAD_ROUTE_PREFIX}/${encodedPayload}.${signPayload(encodedPayload, secret)}`,
    expiresAt,
  };
});

export const validateAttachmentUploadToken = Effect.fn("AttachmentUpload.validateToken")(function* (
  token: string,
) {
  const [encodedPayload, signature, unexpectedSegment] = token.split(".");
  if (!encodedPayload || !signature || unexpectedSegment) {
    return null;
  }

  const secret = yield* loadSigningSecret.pipe(
    Effect.tapError((cause) =>
      Effect.logError("Failed to load the attachment upload signing key.", { cause }),
    ),
    Effect.orElseSucceed(() => null),
  );
  if (!secret || !timingSafeEqualBase64Url(signature, signPayload(encodedPayload, secret))) {
    return null;
  }

  const claims = decodeClaims(encodedPayload);
  if (!claims || claims.expiresAt <= (yield* Clock.currentTimeMillis)) {
    return null;
  }
  if (claims.binding !== undefined) {
    const authority = yield* bindingAuthority(claims.binding);
    const live = yield* current(authority, AuthOrchestrationOperateScope).pipe(Effect.result);
    if (live._tag === "Failure") return null;
  }
  return claims;
});

export type StoreAttachmentUploadResult =
  | { readonly ok: true }
  | { readonly ok: false; readonly status: number; readonly detail: string };

export const storeAttachmentUpload = Effect.fn("AttachmentUpload.store")(function* (
  claims: AttachmentUploadClaims,
  body: Uint8Array | HttpServerRequest.HttpServerRequest["stream"],
) {
  if (body instanceof Uint8Array && body.byteLength !== claims.sizeBytes) {
    return {
      ok: false,
      status: 400,
      detail: `Body was ${body.byteLength} bytes, expected ${claims.sizeBytes}.`,
    } satisfies StoreAttachmentUploadResult;
  }

  const config = yield* ServerConfig.ServerConfig;
  const extension =
    claims.type === "file"
      ? attachmentFileExtension(claims.name)
      : inferImageExtension({ mimeType: claims.mimeType, fileName: claims.name });
  const relativePath = `${claims.attachmentId}${extension}`;
  const finalPath = resolveAttachmentRelativePath({
    attachmentsDir: config.attachmentsDir,
    relativePath,
  });
  const crypto = yield* Crypto.Crypto;
  const partId = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
  const partPath = resolveAttachmentRelativePath({
    attachmentsDir: config.attachmentsDir,
    relativePath: `${relativePath}.${partId}.part`,
  });
  if (!finalPath || !partPath) {
    return { ok: false, status: 500, detail: "Failed to resolve attachment path." };
  }

  const authority =
    claims.binding === undefined ? trustedReadAuthority : yield* bindingAuthority(claims.binding);
  const check = authorizeAttachment(authority, claims.attachmentId, AuthOrchestrationOperateScope);
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  let receivedBytes = 0;
  const bodyStream = body instanceof Uint8Array ? Stream.make(body) : body;
  return yield* Effect.gen(function* () {
    yield* check;
    yield* fileSystem.makeDirectory(path.dirname(finalPath), { recursive: true });
    yield* check;
    yield* Stream.run(
      bodyStream.pipe(
        Stream.tap(() => check),
        Stream.takeWhile((chunk) => {
          receivedBytes += chunk.byteLength;
          return receivedBytes <= claims.sizeBytes;
        }),
      ),
      fileSystem.sink(partPath),
    );
    if (receivedBytes !== claims.sizeBytes) {
      return {
        ok: false,
        status: 400,
        detail: `Body was ${receivedBytes} bytes, expected ${claims.sizeBytes}.`,
      } satisfies StoreAttachmentUploadResult;
    }
    yield* check;
    yield* fileSystem.rename(partPath, finalPath);
    return { ok: true } satisfies StoreAttachmentUploadResult;
  }).pipe(
    Effect.catchTags({
      EnvironmentAuthorizationError: () =>
        Effect.succeed({
          ok: false,
          status: 404,
          detail: "Not Found",
        } satisfies StoreAttachmentUploadResult),
    }),
    Effect.catch((cause) =>
      Effect.logError("Failed to persist attachment upload.", {
        attachmentId: claims.attachmentId,
        cause,
      }).pipe(
        Effect.as({
          ok: false,
          status: 500,
          detail: "Failed to persist upload.",
        } satisfies StoreAttachmentUploadResult),
      ),
    ),
    Effect.ensuring(
      fileSystem.remove(partPath, { force: true }).pipe(Effect.orElseSucceed(() => undefined)),
    ),
  );
});

export const deletePendingAttachment = Effect.fn("AttachmentUpload.deletePending")(function* (
  attachmentId: string,
  authority: ThreadReadAuthority = trustedReadAuthority,
) {
  yield* authorizeAttachment(authority, attachmentId, AuthOrchestrationOperateScope);
  if (parseThreadSegmentFromAttachmentId(attachmentId) !== PENDING_ATTACHMENT_THREAD_SEGMENT) {
    return;
  }

  const config = yield* ServerConfig.ServerConfig;
  const attachmentPath = resolveAttachmentPathById({
    attachmentsDir: config.attachmentsDir,
    attachmentId,
  });
  if (!attachmentPath) {
    return;
  }

  const fileSystem = yield* FileSystem.FileSystem;
  yield* authorizeAttachment(authority, attachmentId, AuthOrchestrationOperateScope);
  yield* fileSystem.remove(attachmentPath, { force: true }).pipe(Effect.orElseSucceed(() => {}));
});

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const persistChatAttachments = Effect.fn("AttachmentUpload.persistChatAttachments")(function* (
    authority: MutationAuthority,
    input: PersistChatAttachmentsInput,
  ) {
    const check = Effect.gen(function* () {
      if (authority === trustedMutationAuthority) return;
      const value = yield* authority.current;
      if (!value.scopes.includes(AuthOrchestrationOperateScope)) return yield* threadReadDenied();
      yield* authorizeOwnerThreadIds(sql, value.resourceGrant, [input.threadId]).pipe(
        Effect.mapError(() => threadReadDenied()),
      );
      if (
        (yield* current(authority, AuthOrchestrationOperateScope))?.authorityRevision !==
        value.authorityRevision
      )
        return yield* threadReadDenied();
    });
    yield* check;
    return yield* Effect.forEach(
      input.attachments.map((attachment, index) => ({ attachment, index })),
      Effect.fn("AttachmentUpload.persistChatAttachment")(function* ({ attachment, index }) {
        const parsed = parseBase64DataUrl(attachment.dataUrl);
        if (parsed === null || parsed.mimeType !== attachment.mimeType.toLowerCase()) {
          return yield* new PersistChatAttachmentsError({
            message: `Attachment ${attachment.name} has an invalid image payload.`,
          });
        }
        const bytes = yield* Effect.fromResult(Base64.decode(parsed.base64)).pipe(
          Effect.mapError(
            (cause) =>
              new PersistChatAttachmentsError({
                message: `Attachment ${attachment.name} is not valid base64.`,
                cause,
              }),
          ),
        );
        if (bytes.byteLength !== attachment.sizeBytes) {
          return yield* new PersistChatAttachmentsError({
            message: `Attachment ${attachment.name} size does not match its payload.`,
          });
        }
        const rawId = createDeterministicAttachmentId(
          input.threadId,
          `${input.messageId}:${index}`,
        );
        if (rawId === null) {
          return yield* new PersistChatAttachmentsError({
            message: "Could not allocate an attachment identifier.",
          });
        }
        const persisted = {
          type: "image" as const,
          id: ChatAttachmentId.make(rawId),
          name: attachment.name,
          mimeType: attachment.mimeType,
          sizeBytes: attachment.sizeBytes,
        };
        yield* check;
        yield* fileSystem
          .writeFile(path.join(config.attachmentsDir, attachmentRelativePath(persisted)!), bytes)
          .pipe(
            Effect.mapError(
              (cause) =>
                new PersistChatAttachmentsError({
                  message: `Could not persist attachment ${attachment.name}.`,
                  cause,
                }),
            ),
          );
        return persisted;
      }),
      { concurrency: 2 },
    );
  });
  return AttachmentUpload.of({ persistChatAttachments });
});

export const layer = Layer.effect(AttachmentUpload, make);
