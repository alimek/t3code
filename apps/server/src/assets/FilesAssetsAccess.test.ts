import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Icons from "./NativeAppIconResolver.ts";
import * as Favicons from "../project/ProjectFaviconResolver.ts";
import * as GitHubCredentials from "../sourceControl/GitHubCredentials.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import { expect, it } from "@effect/vitest";
import {
  AuthAllResourceGrant,
  AuthSessionId,
  AuthStandardClientScopes,
  AuthOrchestrationOperateScope,
  AuthFilesystemWriteScope,
  AuthFilesystemReadScope,
  EnvironmentId,
  EnvironmentAuthorizationError,
  ChatAttachmentId,
  CommandId,
  MessageId,
  ThreadId,
  AuthResourceGrant,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/sql/SqlClient";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import { HttpServerResponse, HttpClient } from "effect/http";
import * as HttpRouter from "effect/http/HttpRouter";
import * as Config from "../config.ts";
import * as Sessions from "../auth/SessionStore.ts";
import * as SecretStore from "../auth/ServerSecretStore.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import { type MutationAuthority } from "../auth/CommandAuthorization.ts";
import * as Paths from "../workspace/WorkspacePaths.ts";
import * as Entries from "../workspace/WorkspaceEntries.ts";
import * as Files from "../workspace/WorkspaceFileSystem.ts";
import * as FileAccess from "../workspace/FileAccess.ts";
import {
  activeA,
  activeB,
  projectA,
  projectB,
  grantA,
  grantNone,
  collectionStoreLayer,
  makeCollectionFixture,
} from "../orchestration-v2/projectCollectionTestFixtures.ts";
import { claimPendingAttachments } from "../orchestration-v2/AttachmentClaims.ts";
import { createAttachmentId } from "../attachmentStore.ts";
import { issueAssetUrl, resolveAsset, ASSET_ROUTE_PREFIX } from "./AssetAccess.ts";
import {
  issueAttachmentUploadUrl,
  validateAttachmentUploadToken,
  storeAttachmentUpload,
  deletePendingAttachment,
  ATTACHMENT_UPLOAD_ROUTE_PREFIX,
} from "./AttachmentUpload.ts";
import * as AttachmentUpload from "./AttachmentUpload.ts";
import * as AssetAccess from "./AssetAccess.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import { assetFileResponse, layerAssetRoute, layerAttachmentUploadRoute } from "../http.ts";

const config = Config.layerTest(process.cwd(), { prefix: "t3-files-assets-proof-" });
const secrets = SecretStore.layer.pipe(
  Layer.provideMerge(config),
  Layer.provideMerge(NodeServices.layer),
);
const sessions = Sessions.layer.pipe(
  Layer.provideMerge(collectionStoreLayer),
  Layer.provideMerge(secrets),
  Layer.provide(
    Layer.succeed(Environment.ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(EnvironmentId.make("files-assets-proof")),
    }),
  ),
);
const layer = sessions.pipe(
  Layer.provideMerge(NodeHttpPlatform.layer.pipe(Layer.provide(NodeServices.layer))),
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(Paths.layer.pipe(Layer.provide(NodeServices.layer))),
  Layer.provideMerge(
    Layer.mergeAll(
      Layer.mock(Orchestrator.OrchestratorV2)({}),
      Layer.mock(Icons.NativeAppIconResolver)({}),
      Layer.mock(Favicons.ProjectFaviconResolver)({}),
      Layer.mock(GitHubCredentials.GitHubCredentials)({}),
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make(() => Effect.die("Unexpected outbound HTTP in files/assets proof")),
      ),
    ),
  ),
);
const encodeGrant = Schema.encodeSync(Schema.fromJsonString(AuthResourceGrant));
const isDenied = Schema.is(EnvironmentAuthorizationError);
const denied = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.result,
    Effect.map((result) => {
      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) expect(isDenied(result.failure)).toBe(true);
    }),
  );

const requests = Effect.gen(function* () {
  const context = yield* Effect.context<Layer.Success<typeof layer>>();
  const { handler, dispose } = HttpRouter.toWebHandler(
    Layer.merge(layerAssetRoute, layerAttachmentUploadRoute).pipe(
      Layer.provideMerge(Layer.succeedContext(context)),
    ),
    { disableLogger: true },
  );
  yield* Effect.addFinalizer(() => Effect.promise(dispose));
  return (url: string, init?: RequestInit) =>
    Effect.promise(() => handler(new Request(`http://test.local${url}`, init), context));
});

const fixture = Effect.gen(function* () {
  yield* makeCollectionFixture;
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const config = yield* Config.ServerConfig;
  const sessions = yield* Sessions.SessionStore;
  const base = yield* fs.makeTempDirectoryScoped({ prefix: "files-assets-roots-" });
  const a = `${base}/a`,
    b = `${a}/hidden`,
    outside = `${base}/outside`;
  for (const root of [a, b, outside]) yield* fs.makeDirectory(root, { recursive: true });
  yield* fs.writeFileString(`${a}/allowed.txt`, "hello visible\n");
  yield* fs.writeFileString(`${a}/page.html`, "<p>allowed</p>");
  yield* fs.writeFileString(`${b}/secret.html`, "<p>hidden</p>");
  yield* fs.writeFileString(`${b}/secret.txt`, "secret hidden\n");
  yield* fs.writeFileString(`${outside}/output.mp4`, "outside bytes");
  yield* fs.writeFileString(`${a}/clip.mp4`, "0123456789");
  yield* fs.symlink(b, `${a}/escape`);
  yield* sql`UPDATE projection_projects SET workspace_root = ${a} WHERE project_id = ${projectA}`;
  yield* sql`UPDATE projection_projects SET workspace_root = ${b} WHERE project_id = ${projectB}`;
  const scopes = AuthStandardClientScopes;
  for (const [id, grant] of [
    ["friend", grantA],
    ["other", grantA],
    ["owner", AuthAllResourceGrant],
  ] as const) {
    yield* sql`INSERT INTO auth_sessions (session_id, subject, scopes, method, issued_at, expires_at, resource_grant, device_id, authority_revision)
      VALUES (${id}, ${id}, ${JSON.stringify(scopes)}, 'bearer-access-token', '2026-01-01T00:00:00.000Z', '2099-01-01T00:00:00.000Z', ${encodeGrant(grant)}, ${id}, 0)`;
  }
  const authority = (id: string): Exclude<MutationAuthority, symbol> => ({
    current: sessions.getAuthority(AuthSessionId.make(id)).pipe(
      Effect.mapError(
        () =>
          new EnvironmentAuthorizationError({
            requiredScope: AuthFilesystemReadScope,
            message: "denied",
          }),
      ),
      Effect.map((value) => ({ ...value, sessionId: AuthSessionId.make(id) })),
    ),
  });
  const friend = authority("friend"),
    owner = authority("owner"),
    other = authority("other");
  const cut = (grant: AuthResourceGrant = grantNone) =>
    sql`UPDATE auth_sessions SET resource_grant = ${encodeGrant(grant)}, authority_revision = authority_revision + 1 WHERE session_id = 'friend'`;
  const resolve = (url: string) => {
    const suffix = url.slice(ASSET_ROUTE_PREFIX.length + 1),
      i = suffix.indexOf("/");
    return resolveAsset(suffix.slice(0, i), suffix.slice(i + 1));
  };
  return { sql, fs, config, a, b, outside, friend, owner, other, cut, resolve, authority };
});

it.effect(
  "allows basic files, hides nested owners and denies traversal/symlinks and scope cuts before writes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entries = Entries.WorkspaceEntries.of({
        list: () =>
          Effect.succeed({
            entries: [
              { path: "allowed.txt", kind: "file" },
              { path: "hidden/secret.txt", kind: "file" },
              { path: "escape/secret.txt", kind: "file" },
            ],
            truncated: false,
          }),
        search: () =>
          Effect.succeed({
            entries: [
              { path: "allowed.txt", kind: "file" },
              { path: "hidden/secret.txt", kind: "file" },
            ],
            truncated: false,
          }),
        searchContents: () => Effect.die("Restricted content search must not scan hidden files"),
        browse: () => Effect.die("unused"),
        refresh: () => Effect.void,
      });
      const paths = yield* Paths.make;
      const files = yield* Files.make.pipe(
        Effect.provideService(Paths.WorkspacePaths, paths),
        Effect.provideService(Entries.WorkspaceEntries, entries),
      );
      const access = yield* FileAccess.FileAccess.pipe(
        Effect.provide(FileAccess.layer),
        Effect.provideService(Files.WorkspaceFileSystem, files),
        Effect.provideService(Entries.WorkspaceEntries, entries),
      );
      yield* f.sql`UPDATE auth_sessions SET scopes = ${JSON.stringify([AuthFilesystemReadScope, AuthFilesystemWriteScope])} WHERE session_id = 'friend'`;
      expect(
        (yield* access.readFile(f.friend, { cwd: f.a, relativePath: "allowed.txt" })).contents,
      ).toContain("visible");
      expect((yield* access.list(f.friend, { cwd: f.a })).entries.map((x) => x.path)).toEqual([
        "allowed.txt",
      ]);
      expect(
        (yield* access.search(f.friend, { cwd: f.a, query: "txt", limit: 20 })).entries.map(
          (x) => x.path,
        ),
      ).toEqual(["allowed.txt"]);
      expect(
        (yield* access.searchContents(f.friend, {
          cwd: f.a,
          query: "visible",
          limit: 20,
          caseSensitive: false,
          useRegex: false,
          wholeWord: true,
        })).matches[0]?.lineContent,
      ).toContain("visible");
      expect(
        (yield* access.searchContents(f.friend, {
          cwd: f.a,
          query: "(hello|visible)",
          limit: 20,
          caseSensitive: false,
          useRegex: true,
          wholeWord: true,
        })).matches[0]?.matchRanges,
      ).toEqual([
        { start: 0, end: 5 },
        { start: 6, end: 13 },
      ]);
      for (const relativePath of [
        "hidden/secret.txt",
        "escape/secret.txt",
        "../outside/output.mp4",
        `${f.outside}/output.mp4`,
      ]) {
        yield* denied(access.readFile(f.friend, { cwd: f.a, relativePath }));
        yield* denied(access.writeFile(f.friend, { cwd: f.a, relativePath, contents: "BAD" }));
      }
      yield* access.writeFile(f.friend, {
        cwd: f.a,
        relativePath: "new/sub.txt",
        contents: "allowed write",
      });
      expect(yield* f.fs.readFileString(`${f.a}/new/sub.txt`)).toBe("allowed write");
      expect(
        (yield* access.readFile(f.owner, { cwd: f.a, relativePath: `${f.outside}/output.mp4` }))
          .contents,
      ).toBe("outside bytes");
      const reached = yield* Deferred.make<void>(),
        resume = yield* Deferred.make<void>();
      const gatedFs = FileSystem.FileSystem.of({
        ...f.fs,
        realPath: (path) =>
          f.fs
            .realPath(path)
            .pipe(
              Effect.tap(() =>
                path === `${f.a}/allowed.txt`
                  ? Deferred.succeed(reached, undefined).pipe(
                      Effect.andThen(Deferred.await(resume)),
                    )
                  : Effect.void,
              ),
            ),
      });
      const gatedAccess = yield* FileAccess.FileAccess.pipe(
        Effect.provide(FileAccess.layer),
        Effect.provideService(FileSystem.FileSystem, gatedFs),
        Effect.provideService(Files.WorkspaceFileSystem, files),
        Effect.provideService(Entries.WorkspaceEntries, entries),
      );
      const pending = yield* gatedAccess
        .writeFile(f.friend, { cwd: f.a, relativePath: "allowed.txt", contents: "BAD" })
        .pipe(Effect.result, Effect.forkChild);
      yield* Deferred.await(reached);
      yield* f.cut();
      yield* Deferred.succeed(resume, undefined);
      expect(Result.isFailure(yield* Fiber.join(pending))).toBe(true);
      expect(yield* f.fs.readFileString(`${f.a}/allowed.txt`)).toBe("hello visible\n");
    }).pipe(Effect.provide(layer)),
);

it.effect("binds signed GET/HEAD/range to current device revision and registered owners", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const input = {
      resource: { _tag: "media-file" as const, threadId: activeA, path: `${f.a}/clip.mp4` },
      workspaceRoot: f.a,
      authority: f.friend,
    };
    const issued = yield* issueAssetUrl(input);
    const request = yield* requests;
    const actualHead = yield* request(issued.relativeUrl, { method: "HEAD" });
    expect(actualHead.status).toBe(200);
    expect(actualHead.headers.get("content-length")).toBe("10");
    expect(yield* Effect.promise(() => actualHead.text())).toBe("");
    const actualRange = yield* request(issued.relativeUrl, { headers: { range: "bytes=2-4" } });
    expect(actualRange.status).toBe(206);
    expect(yield* Effect.promise(() => actualRange.text())).toBe("234");
    const asset = yield* f.resolve(issued.relativeUrl);
    expect(asset?.kind).toBe("file");
    if (asset?.kind !== "file") return;
    const head = HttpServerResponse.toWeb(
      yield* assetFileResponse(asset, undefined, undefined, "HEAD"),
    );
    expect(head.headers.get("content-length")).toBe("10");
    expect(yield* Effect.promise(() => head.text())).toBe("");
    const range = HttpServerResponse.toWeb(yield* assetFileResponse(asset, "bytes=2-4"));
    expect(range.status).toBe(206);
    expect(range.headers.get("content-range")).toBe("bytes 2-4/10");
    expect(yield* Effect.promise(() => range.text())).toBe("234");
    for (const [threadId, path] of [
      [activeB, `${f.a}/clip.mp4`],
      [activeA, `${f.outside}/output.mp4`],
      [activeA, `${f.a}/escape/secret.txt`],
    ] as const)
      yield* denied(issueAssetUrl({ ...input, resource: { _tag: "media-file", threadId, path } }));
    const owner = yield* issueAssetUrl({
      ...input,
      authority: f.owner,
      resource: {
        _tag: "media-file",
        threadId: ThreadId.make("foreign-thread"),
        path: `${f.outside}/output.mp4`,
      },
    });
    expect((yield* f.resolve(owner.relativeUrl))?.kind).toBe("file");
    const ceiling = yield* issueAssetUrl({
      resource: { _tag: "workspace-file", threadId: activeA, path: "page.html" },
      workspaceRoot: f.a,
      authority: {
        current: f.owner.current.pipe(Effect.map((value) => ({ ...value, resourceGrant: grantA }))),
      },
    });
    expect(
      yield* f.resolve(ceiling.relativeUrl.replace(/\/page\.html$/, "/hidden/secret.html")),
    ).toBeNull();
    yield* f.cut(grantA);
    expect(yield* f.resolve(issued.relativeUrl)).toBeNull();
    for (const method of ["GET", "HEAD"])
      expect(
        (yield* request(issued.relativeUrl, { method, headers: { range: "bytes=2-4" } })).status,
      ).toBe(404);
    yield* denied(assetFileResponse(asset, undefined, undefined, "HEAD"));
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "binds upload content/claim/delete to device and ceiling; cut during stream publishes no upload",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.sql`UPDATE auth_sessions SET scopes = ${JSON.stringify([AuthOrchestrationOperateScope])} WHERE session_id = 'friend'`;
      const narrow = {
        current: f.owner.current.pipe(Effect.map((value) => ({ ...value, resourceGrant: grantA }))),
      };
      const narrowedUpload = yield* issueAttachmentUploadUrl(
        { type: "file", name: "narrow.mp4", mimeType: "video/mp4", sizeBytes: 13 },
        narrow,
      );
      const narrowedClaims = yield* validateAttachmentUploadToken(
        narrowedUpload.relativeUrl.slice(ATTACHMENT_UPLOAD_ROUTE_PREFIX.length + 1),
      );
      expect(narrowedClaims).not.toBeNull();
      if (narrowedClaims === null) return;
      expect(
        (yield* storeAttachmentUpload(narrowedClaims, new TextEncoder().encode("outside bytes")))
          .ok,
      ).toBe(true);
      yield* denied(
        claimPendingAttachments(f.owner, {
          threadId: activeB,
          attachments: [
            {
              type: "file",
              id: ChatAttachmentId.make(narrowedUpload.attachmentId),
              name: "narrow.mp4",
              mimeType: "video/mp4",
              sizeBytes: 13,
            },
          ],
          command: {
            type: "message.dispatch",
            commandId: CommandId.make("narrow-claim"),
            threadId: activeB,
            messageId: MessageId.make("narrow-message"),
            text: "file",
            attachments: [
              {
                type: "file",
                id: ChatAttachmentId.make(narrowedUpload.attachmentId),
                name: "narrow.mp4",
                mimeType: "video/mp4",
                sizeBytes: 13,
              },
            ],
            createdBy: "user",
            creationSource: "web",
            dispatchMode: { type: "start_immediately" },
          },
        }),
      );
      yield* deletePendingAttachment(narrowedUpload.attachmentId, f.owner);
      expect(
        yield* f.fs.exists(`${f.config.attachmentsDir}/${narrowedUpload.attachmentId}.mp4`),
      ).toBe(false);
      const issued = yield* issueAttachmentUploadUrl(
        { type: "file", name: "output.mp4", mimeType: "video/mp4", sizeBytes: 13 },
        f.friend,
      );
      const token = issued.relativeUrl.slice(ATTACHMENT_UPLOAD_ROUTE_PREFIX.length + 1);
      const claims = yield* validateAttachmentUploadToken(token);
      expect(claims).not.toBeNull();
      if (claims === null) return;
      const request = yield* requests;
      expect(
        (yield* request(issued.relativeUrl, {
          method: "POST",
          body: yield* f.fs.readFileString(`${f.outside}/output.mp4`),
        })).status,
      ).toBe(204);
      yield* denied(deletePendingAttachment(issued.attachmentId, f.other));
      const attachment = {
        type: "file" as const,
        id: ChatAttachmentId.make(issued.attachmentId),
        name: "output.mp4",
        mimeType: "video/mp4",
        sizeBytes: 13,
      };
      const command = {
        type: "message.dispatch" as const,
        commandId: CommandId.make("claim-proof"),
        threadId: activeA,
        messageId: MessageId.make("claim-message"),
        text: "file",
        attachments: [attachment],
        createdAt: "2026-10-11T00:00:00.000Z",
        createdBy: "user" as const,
        creationSource: "web" as const,
        dispatchMode: { type: "start_immediately" as const },
      };
      yield* denied(
        claimPendingAttachments(f.other, { threadId: activeA, attachments: [attachment], command }),
      );
      const claimed = yield* claimPendingAttachments(f.friend, {
        threadId: activeA,
        attachments: [attachment],
        command,
      });
      expect(claimed.claimedPaths).toHaveLength(1);
      expect(yield* f.fs.readFileString(claimed.claimedPaths[0]!)).toBe("outside bytes");
      yield* f.sql`UPDATE auth_sessions SET scopes = ${JSON.stringify(AuthStandardClientScopes)} WHERE session_id = 'friend'`;
      const persisted = yield* issueAssetUrl({
        resource: { _tag: "attachment", attachmentId: claimed.attachments[0]!.id },
        authority: f.friend,
      });
      expect((yield* f.resolve(persisted.relativeUrl))?.kind).toBe("file");
      const hidden = createAttachmentId(activeB, "mp4")!;
      yield* f.fs.writeFileString(`${f.config.attachmentsDir}/${hidden}.mp4`, "hidden");
      yield* denied(
        issueAssetUrl({
          resource: { _tag: "attachment", attachmentId: hidden },
          authority: f.friend,
        }),
      );
      const reached = yield* Deferred.make<void>(),
        resume = yield* Deferred.make<void>();
      const next = yield* issueAttachmentUploadUrl(
        { name: "cut.png", mimeType: "image/png", sizeBytes: 6 },
        f.friend,
      );
      const nextClaims = yield* validateAttachmentUploadToken(
        next.relativeUrl.slice(ATTACHMENT_UPLOAD_ROUTE_PREFIX.length + 1),
      );
      if (nextClaims === null) return;
      const stream = Stream.fromEffect(
        Deferred.succeed(reached, undefined).pipe(
          Effect.andThen(Deferred.await(resume)),
          Effect.as(new TextEncoder().encode("pixels")),
        ),
      );
      const store = yield* storeAttachmentUpload(nextClaims, stream).pipe(Effect.forkChild);
      yield* Deferred.await(reached);
      yield* f.cut();
      yield* Deferred.succeed(resume, undefined);
      expect((yield* Fiber.join(store)).ok).toBe(false);
      expect(yield* f.fs.exists(`${f.config.attachmentsDir}/${next.attachmentId}.png`)).toBe(false);
      expect(
        (yield* f.fs.readDirectory(f.config.attachmentsDir)).some((x) => x.endsWith(".part")),
      ).toBe(false);
      expect(yield* f.resolve(persisted.relativeUrl)).toBeNull();
      expect(yield* validateAttachmentUploadToken(token)).toBeNull();
      expect(
        (yield* request(issued.relativeUrl, { method: "POST", body: "outside bytes" })).status,
      ).toBe(404);
      expect(
        (yield* storeAttachmentUpload(claims, new TextEncoder().encode("outside bytes"))).ok,
      ).toBe(false);
      yield* denied(deletePendingAttachment(issued.attachmentId, f.friend));
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "admits persisted chat images without read scope and denies hidden thread ids and cuts before writes",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const uploads = yield* AttachmentUpload.AttachmentUpload;
      yield* f.sql`UPDATE auth_sessions SET scopes = ${JSON.stringify([AuthOrchestrationOperateScope])} WHERE session_id = 'friend'`;
      const attachments = [
        {
          type: "image" as const,
          name: "chat.png",
          mimeType: "image/png",
          sizeBytes: 6,
          dataUrl: "data:image/png;base64,cGl4ZWxz",
        },
      ];
      const input = { threadId: activeA, messageId: MessageId.make("chat-image"), attachments };
      yield* denied(uploads.persistChatAttachments(f.friend, { ...input, threadId: activeB }));
      const persisted = yield* uploads.persistChatAttachments(f.friend, input);
      expect(persisted).toHaveLength(1);
      expect(yield* f.fs.readFileString(`${f.config.attachmentsDir}/${persisted[0]!.id}.png`)).toBe(
        "pixels",
      );
      yield* f.cut();
      yield* denied(
        uploads.persistChatAttachments(f.friend, {
          ...input,
          messageId: MessageId.make("cut-image"),
        }),
      );
      expect(yield* f.fs.readDirectory(f.config.attachmentsDir)).toHaveLength(1);
    }).pipe(Effect.provide(AttachmentUpload.layer.pipe(Layer.provideMerge(layer)))),
);

it.effect(
  "retained HEAD, range and body deny a newly hidden file owner without a session cut",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const child = `${f.a}/child`;
      yield* f.fs.makeDirectory(child);
      yield* f.fs.writeFileString(`${child}/clip.mp4`, "0123456789");
      const issued = yield* issueAssetUrl({
        resource: { _tag: "media-file", threadId: activeA, path: `${child}/clip.mp4` },
        workspaceRoot: f.a,
        authority: f.friend,
      });
      const positive = yield* f.resolve(issued.relativeUrl);
      if (positive?.kind !== "file") return yield* Effect.die("Expected admitted file");
      expect((yield* assetFileResponse(positive, undefined, undefined, "HEAD")).status).toBe(200);
      const range = HttpServerResponse.toWeb(yield* assetFileResponse(positive, "bytes=2-4"));
      expect(range.status).toBe(206);
      expect(yield* Effect.promise(() => range.text())).toBe("234");
      // Fresh descriptor: the positive range consumed its stream.
      const retained = yield* f.resolve(issued.relativeUrl);
      if (retained?.kind !== "file") return yield* Effect.die("Expected retained file");
      const prepared = yield* assetFileResponse(retained, "bytes=2-4");
      yield* f.sql`UPDATE projection_projects SET workspace_root = ${child} WHERE project_id = ${projectB}`;
      yield* denied(assetFileResponse(retained, undefined, undefined, "HEAD"));
      yield* denied(assetFileResponse(retained, "bytes=2-4"));
      const request = yield* requests;
      for (const method of ["GET", "HEAD"])
        expect(
          (yield* request(issued.relativeUrl, { method, headers: { range: "bytes=2-4" } })).status,
        ).toBe(404);
      if (prepared.body._tag !== "Stream") return yield* Effect.die("Expected response stream");
      const body = prepared.body.stream.pipe(
        Stream.mapError((error) => {
          if (!isDenied(error))
            throw new Error("Unexpected response stream failure", { cause: error });
          return error;
        }),
      );
      yield* denied(Stream.runCollect(body));
      const owner = yield* issueAssetUrl({
        resource: { _tag: "media-file", threadId: activeA, path: `${child}/clip.mp4` },
        authority: f.owner,
      });
      const ownerFile = yield* f.resolve(owner.relativeUrl);
      if (ownerFile?.kind !== "file") return yield* Effect.die("Expected owner file");
      expect((yield* assetFileResponse(ownerFile, undefined, undefined, "HEAD")).status).toBe(200);
    }).pipe(Effect.provide(layer)),
);

it.effect("asset resolution rechecks file ownership after canonicalization awaits", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const child = `${f.a}/child`;
    yield* f.fs.makeDirectory(child);
    yield* f.fs.writeFileString(`${child}/clip.mp4`, "0123456789");
    const issued = yield* issueAssetUrl({
      resource: { _tag: "media-file", threadId: activeA, path: `${child}/clip.mp4` },
      workspaceRoot: f.a,
      authority: f.friend,
    });
    const reached = yield* Deferred.make<void>(),
      resume = yield* Deferred.make<void>();
    let targetResolutions = 0;
    const gated = FileSystem.FileSystem.of({
      ...f.fs,
      realPath: (target) =>
        f.fs.realPath(target).pipe(
          Effect.tap(() =>
            // Resolve/open and the initial file-owner admission have finished before the third canonical-path await.
            target.endsWith("/child/clip.mp4") && ++targetResolutions === 3
              ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(resume)))
              : Effect.void,
          ),
        ),
    });
    const resolving = yield* f
      .resolve(issued.relativeUrl)
      .pipe(Effect.provideService(FileSystem.FileSystem, gated), Effect.forkChild);
    yield* Deferred.await(reached);
    yield* f.sql`UPDATE projection_projects SET workspace_root = ${child} WHERE project_id = ${projectB}`;
    yield* Deferred.succeed(resume, undefined);
    expect(yield* Fiber.join(resolving)).toBeNull();
  }).pipe(Effect.provide(layer)),
);

it.effect("asset service preserves owner absolute media and filesystem-only draft admission", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const assets = yield* AssetAccess.AssetAccess;
    const owner = yield* assets.createUrl(f.owner, {
      resource: {
        _tag: "media-file",
        threadId: ThreadId.make("foreign-thread"),
        path: `${f.outside}/output.mp4`,
      },
    });
    expect((yield* f.resolve(owner.relativeUrl))?.kind).toBe("file");
    yield* f.sql`UPDATE auth_sessions SET scopes = ${JSON.stringify([AuthFilesystemReadScope])} WHERE session_id = 'friend'`;
    const draft = yield* assets.createUrl(f.friend, {
      resource: { _tag: "draft-workspace-file", cwd: f.a, path: "page.html" },
    });
    expect((yield* f.resolve(draft.relativeUrl))?.kind).toBe("file");
    yield* denied(
      assets.createUrl(f.friend, {
        resource: { _tag: "draft-workspace-file", cwd: f.b, path: "secret.html" },
      }),
    );
  }).pipe(
    Effect.provide(
      AssetAccess.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ProjectService.ProjectService)({}),
            Layer.mock(ThreadManagement.ThreadManagementService)({}),
            Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({}),
          ),
        ),
        Layer.provideMerge(layer),
      ),
    ),
  ),
);
