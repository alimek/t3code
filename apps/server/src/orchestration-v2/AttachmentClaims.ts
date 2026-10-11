import { openMediaFile, streamMediaFile } from "../assets/MediaFile.ts";
import * as Stream from "effect/Stream";
import { authorizeUploadClaim } from "../assets/AssetAuthority.ts";
import {
  authorizeAttachmentClaim,
  mutationDenied,
  trustedMutationAuthority,
  type MutationAuthority,
} from "../auth/CommandAuthorization.ts";
import * as SqlClient from "effect/sql/SqlClient";
import * as FileSystem from "effect/FileSystem";
import {
  ChatAttachmentId,
  getProviderAttachmentLimitError,
  type ChatAttachment,
  type OrchestrationV2ServerCommand,
  type ThreadId,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import {
  parseThreadSegmentFromAttachmentId,
  PENDING_ATTACHMENT_THREAD_SEGMENT,
  planAttachmentClaim,
  resolveAttachmentPath,
} from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";

export class AttachmentClaimError extends Schema.TaggedError<AttachmentClaimError>()(
  "AttachmentClaimError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const validateAttachmentLimits = Effect.fn("AttachmentClaims.validateAttachmentLimits")(
  function* (attachments: ReadonlyArray<ChatAttachment>) {
    const error = getProviderAttachmentLimitError(attachments);
    if (error) return yield* new AttachmentClaimError({ message: error });
  },
);

export interface ClaimedAttachments {
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly claimedPaths: ReadonlyArray<string>;
}

export function attachmentIsPendingUpload(attachment: ChatAttachment): boolean {
  return parseThreadSegmentFromAttachmentId(attachment.id) === PENDING_ATTACHMENT_THREAD_SEGMENT;
}

/** Remove partial claims only before dispatch, or after proving they were not accepted. */
export const releaseClaimedAttachments = Effect.fn("AttachmentClaims.releaseClaimedAttachments")(
  function* (claimedPaths: ReadonlyArray<string>) {
    if (claimedPaths.length === 0) return;
    const fileSystem = yield* FileSystem.FileSystem;
    yield* Effect.forEach(claimedPaths, (path) => fileSystem.remove(path).pipe(Effect.ignore), {
      concurrency: 1,
      discard: true,
    }).pipe(Effect.uninterruptible);
  },
);

/**
 * Claims pending uploads into the target thread's attachment store before the
 * command enters the orchestrator: verifies the staged file, copies it under a
 * thread-scoped id, and rewrites the attachment ref. A copy, not a move — the
 * pending file stays behind as the retry source for a failed bootstrap, and
 * the periodic pending sweep reclaims it later. Already-claimed attachments
 * pass through untouched.
 */
export const claimPendingAttachments = Effect.fn("AttachmentClaims.claimPendingAttachments")(
  function* (
    authority: MutationAuthority,
    input: {
      readonly threadId: string;
      readonly attachments: ReadonlyArray<ChatAttachment>;
      readonly command?: OrchestrationV2ServerCommand;
      readonly additionalCommands?: ReadonlyArray<OrchestrationV2ServerCommand>;
      readonly creatingThread?: { readonly threadId: ThreadId; readonly projectId: ProjectId };
    },
  ) {
    // App-owned trusted work needs no SQL admission; external claims fail closed without app context.
    const withAdmission = <A, E, R>(operation: () => Effect.Effect<A, E, R>) =>
      Effect.gen(function* () {
        const effect = Effect.suspend(operation);
        if (authority === trustedMutationAuthority) return yield* effect;
        if (input.command === undefined) return yield* mutationDenied();
        const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
        if (Option.isNone(sql)) return yield* mutationDenied();
        yield* sql.value.withTransaction(
          authorizeAttachmentClaim(sql.value, authority, {
            ...input,
            command: input.command,
            attachmentIds: input.attachments.map((attachment) => attachment.id),
          }),
        );
        {
          const rows = yield* sql.value<{
            project_id: string;
          }>`SELECT project_id FROM orchestration_v2_projection_threads WHERE thread_id = ${input.threadId}`;
          const projectId =
            input.creatingThread?.projectId ??
            (rows[0] === undefined ? undefined : ProjectId.make(rows[0].project_id));
          if (projectId === undefined) return yield* mutationDenied();
          for (const attachment of input.attachments) {
            if (attachmentIsPendingUpload(attachment))
              yield* authorizeUploadClaim(authority, attachment.id, projectId);
          }
        }
        return yield* effect;
      });
    yield* withAdmission(() => Effect.void);
    yield* validateAttachmentLimits(input.attachments);
    if (
      new Set(input.attachments.map((attachment) => attachment.id)).size !==
      input.attachments.length
    ) {
      return yield* new AttachmentClaimError({
        message: "Duplicate attachment ids are not allowed.",
      });
    }
    if (!input.attachments.some(attachmentIsPendingUpload)) {
      return { attachments: input.attachments, claimedPaths: [] } satisfies ClaimedAttachments;
    }
    const serverConfig = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const claimedPaths: string[] = [];
    const attachments = yield* Effect.forEach(
      input.attachments,
      (attachment) =>
        Effect.gen(function* () {
          if (!attachmentIsPendingUpload(attachment)) {
            return attachment;
          }
          const claim = yield* withAdmission(() =>
            Effect.sync(() =>
              planAttachmentClaim({
                attachmentsDir: serverConfig.attachmentsDir,
                threadId: input.threadId,
                attachmentId: attachment.id,
              }),
            ),
          );
          if (!claim.ok) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: ${claim.reason}.`,
            });
          }
          const info = yield* withAdmission(() =>
            fileSystem.stat(claim.currentPath).pipe(
              Effect.mapError(
                (cause) =>
                  new AttachmentClaimError({
                    message: `Attachment '${attachment.name}' cannot be sent: attachment not found.`,
                    cause,
                  }),
              ),
            ),
          );
          if (Number(info.size) !== attachment.sizeBytes) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: stored size does not match.`,
            });
          }
          const normalized: ChatAttachment = {
            ...attachment,
            id: ChatAttachmentId.make(claim.finalId),
            mimeType: attachment.mimeType.toLowerCase(),
          };
          const expectedPath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment: normalized,
          });
          if (expectedPath !== claim.finalPath) {
            return yield* new AttachmentClaimError({
              message: `Attachment '${attachment.name}' cannot be sent: attachment type does not match the upload.`,
            });
          }
          // A copy, not a hard link: an agent editing the delivered file in
          // place must not mutate the retry source. fs.copyFile cannot be
          // cancelled, so the copy and its rollback registration stay in one
          // uninterruptible region: an interrupt landing mid-copy still waits
          // for the write to settle and records the path before cleanup runs.
          const copy = Effect.gen(function* () {
            if (authority === trustedMutationAuthority)
              return yield* fileSystem.copyFile(claim.currentPath, claim.finalPath);
            const canonical = yield* fileSystem.realPath(claim.currentPath);
            const file = yield* openMediaFile(canonical);
            if (file === null || file.info.size !== BigInt(attachment.sizeBytes))
              return yield* mutationDenied();
            const stream =
              file.info.size === 0n ? Stream.empty : streamMediaFile(file, 0n, file.info.size);
            if (stream === null) return yield* mutationDenied();
            yield* withAdmission(() =>
              Stream.run(
                stream.pipe(Stream.tap(() => withAdmission(() => Effect.void))),
                fileSystem.sink(claim.finalPath, { flag: "wx" }),
              ),
            );
          }).pipe(Effect.scoped);
          yield* withAdmission(() =>
            Effect.sync(() => claimedPaths.push(claim.finalPath)).pipe(
              Effect.andThen(copy),
              Effect.mapError(
                (cause) =>
                  new AttachmentClaimError({
                    message: `Failed to claim attachment '${attachment.name}' for this thread.`,
                    cause,
                  }),
              ),
              Effect.uninterruptible,
            ),
          );
          return normalized;
        }),
      { concurrency: 1 },
    ).pipe(Effect.onError(() => releaseClaimedAttachments(claimedPaths)));
    return { attachments, claimedPaths } satisfies ClaimedAttachments;
  },
  Effect.catchTags({ SqlError: () => Effect.fail(mutationDenied()) }),
);
