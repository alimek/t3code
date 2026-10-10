// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import {
  trustedReadAuthority,
  threadReadDenied,
  type ThreadReadAuthority,
} from "./ResourceAuthorization.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";

/** Paths are admitted against persisted project/thread roots, never a caller's cwd hint. */
export const requireRegisteredThreadPath = Effect.fn("resourceAuthorization.threadPath")(function* (
  authority: ThreadReadAuthority,
  threadId: ThreadId,
  requested: string,
) {
  if (authority === trustedReadAuthority) return;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const roots = yield* threads.getReadWorkspaceRoots(authority, threadId);
  const current = yield* authority.current;
  if (
    current.resourceGrant?.projects.mode !== "explicit" &&
    current.resourceGrant?.providers.mode !== "explicit"
  )
    return;
  const canonical = yield* Effect.tryPromise({
    try: async () => ({
      path: await NodeFSP.realpath(requested),
      roots: await Promise.all(roots.map((root) => NodeFSP.realpath(root).catch(() => undefined))),
    }),
    catch: () => threadReadDenied(),
  });
  if (
    !canonical.roots.some(
      (root) =>
        root !== undefined &&
        (canonical.path === root || canonical.path.startsWith(`${root}${NodePath.sep}`)),
    )
  )
    return yield* threadReadDenied();
});
