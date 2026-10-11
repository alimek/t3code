// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { type CommandId, type ThreadId, type ProjectId } from "@t3tools/contracts";
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
  withRegisteredCommandWorkspace,
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
  const admittedRoots = roots.filter((_, index) => {
    const root = canonical.roots[index];
    return (
      root !== undefined &&
      (canonical.path === root || canonical.path.startsWith(`${root}${NodePath.sep}`))
    );
  });
  if (admittedRoots.length === 0) return yield* threadReadDenied();
  return admittedRoots;
});

/** Check filesystem paths outside SQL, then bind only this launch claim to its registered roots. */
export const admitRegisteredCommandWorkspace = Effect.fn("resourceAuthorization.launchWorkspace")(
  function* (
    sql: SqlClient.SqlClient,
    authority: MutationAuthority,
    target: {
      readonly commandId: CommandId;
      readonly projectId: ProjectId;
      readonly threadId: ThreadId;
      readonly allowCreate: boolean;
    },
    requested: string,
  ) {
    if (authority === trustedMutationAuthority) return authority;
    yield* authorizeMutationTarget(sql, authority, target);
    const current = yield* authority.current;
    if (
      current.resourceGrant.projects.mode === "all" &&
      current.resourceGrant.providers.mode === "all"
    )
      return authority;
    const roots = yield* registeredMutationRoots(sql, target);
    const admittedRoots = yield* requireCanonicalRoot(roots, requested).pipe(
      Effect.mapError(() => mutationDenied()),
    );
    yield* authorizeMutationTarget(sql, authority, target);
    return withRegisteredCommandWorkspace(authority, {
      commandId: target.commandId,
      projectId: target.projectId,
      threadId: target.threadId,
      path: requested,
      roots: admittedRoots,
    });
  },
);

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
