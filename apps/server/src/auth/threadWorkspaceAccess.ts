// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { type ThreadId, type ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import {
  trustedReadAuthority,
  threadReadDenied,
  type ThreadReadAuthority,
} from "./ResourceAuthorization.ts";
import type * as SqlClient from "effect/sql/SqlClient";
import {
  authorizeMutationTarget,
  registeredMutationRoots,
  mutationDenied,
  trustedMutationAuthority,
  type MutationAuthority,
} from "./CommandAuthorization.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";

/** Launch/retry use the same persisted-root association without requiring read scope or hydration. */
export const requireRegisteredMutationPath = Effect.fn("resourceAuthorization.mutationPath")(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    target: {
      readonly projectId: ProjectId;
      readonly threadId?: ThreadId;
      readonly allowCreate?: boolean;
    },
    requested: string,
  ) {
    if (authority === trustedMutationAuthority) return;
    yield* authorizeMutationTarget(sql, authority, target);
    const current = yield* authority.current;
    if (
      current.resourceGrant.projects.mode !== "explicit" &&
      current.resourceGrant.providers.mode !== "explicit"
    )
      return yield* authorizeMutationTarget(sql, authority, target);
    const roots = yield* registeredMutationRoots(sql, target);
    yield* requireCanonicalRoot(roots, requested).pipe(Effect.mapError(() => mutationDenied()));
    yield* authorizeMutationTarget(sql, authority, target);
  },
);

const requireCanonicalRoot = Effect.fnUntraced(function* (
  roots: ReadonlyArray<string>,
  requested: string,
) {
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
  yield* requireCanonicalRoot(roots, requested);
});
