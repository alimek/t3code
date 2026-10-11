// @effect-diagnostics nodeBuiltinImport:off -- path containment is shared with registered workspace admission.
import * as NodePath from "node:path";
import {
  AuthOrchestrationReadScope,
  EnvironmentAuthorizationError,
  ProjectId,
  resourceGrantAllowsProject,
  type AuthEnvironmentScope,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/sql/SqlClient";
import { trustedReadAuthority, type ThreadReadAuthority } from "./ResourceAuthorization.ts";

const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

/** Only service-owned actions install this final-effect check; raw drivers remain internal. */
export const workspaceEffectAdmission = Context.Reference<WorkspaceActionAdmission | undefined>(
  "t3/auth/workspaceEffectAdmission",
  { defaultValue: () => undefined },
);
export type WorkspaceActionAdmission = {
  readonly authority: ThreadReadAuthority;
  readonly input: WorkspaceReadInput;
  readonly scope: AuthEnvironmentScope;
  readonly projectId: ProjectId | undefined;
  readonly checkSql: Effect.Effect<void, EnvironmentAuthorizationError>;
  readonly beforeEffect: (
    cwd: string,
    scope?: AuthEnvironmentScope,
  ) => Effect.Effect<void, EnvironmentAuthorizationError>;
};
export const beforeWorkspaceEffect = (cwd: string, scope?: AuthEnvironmentScope) =>
  workspaceEffectAdmission.pipe(
    Effect.flatMap((admission) =>
      admission === undefined ? Effect.void : admission.beforeEffect(cwd, scope),
    ),
  );
/** Best-effort Git fallbacks must never turn denied authority into another effect. */
export const recoverWorkspaceError =
  <A>(fallback: () => A) =>
  <B, E, R>(operation: Effect.Effect<B, E, R>) =>
    Effect.catchIf(
      operation,
      (error: E): error is Exclude<E, EnvironmentAuthorizationError> =>
        !isEnvironmentAuthorizationError(error),
      () => Effect.succeed(fallback()),
    );
export const beforeCurrentWorkspaceEffect = workspaceEffectAdmission.pipe(
  Effect.flatMap((admission) =>
    admission === undefined ? Effect.void : admission.beforeEffect(admission.input.cwd),
  ),
);
export type WorkspaceActionCallback<A> = (
  admission: WorkspaceActionAdmission,
  result: A,
) => Effect.Effect<void, EnvironmentAuthorizationError>;

export type WorkspaceReadInput = {
  readonly cwd: string;
  readonly projectId?: ProjectId | undefined;
  readonly threadId?: ThreadId | undefined;
};

const within = (candidate: string, root: string) => {
  const relative = NodePath.relative(root, candidate);
  return (
    relative === "" ||
    (relative !== ".." &&
      !relative.startsWith(`..${NodePath.sep}`) &&
      !NodePath.isAbsolute(relative))
  );
};
const validRoot = (root: unknown): root is string =>
  typeof root === "string" &&
  NodePath.isAbsolute(root) &&
  !root.includes("\0") &&
  !root.split(NodePath.sep).includes("..") &&
  NodePath.dirname(root) !== root;

/** Raw registered owners precede canonical I/O; callers retain their own action scope. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* FileSystem.FileSystem;
  const denied = (scope: AuthEnvironmentScope) =>
    new EnvironmentAuthorizationError({
      requiredScope: scope,
      message: "This connection cannot read the requested workspace resource.",
    });
  const current = Effect.fnUntraced(function* (
    authority: ThreadReadAuthority,
    scope: AuthEnvironmentScope,
  ) {
    if (authority === trustedReadAuthority) return undefined;
    if (!authority?.current) return yield* denied(scope);
    const value = yield* authority.current.pipe(Effect.mapError(() => denied(scope)));
    if (!value.resourceGrant || !value.scopes.includes(scope)) return yield* denied(scope);
    return value;
  });
  const thread = Effect.fnUntraced(function* (
    authority: ThreadReadAuthority,
    threadId: ThreadId,
    scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
  ) {
    const value = yield* current(authority, scope);
    if (value === undefined) return;
    const rows = yield* sql<{ readonly project: string; readonly root: string }>`
      SELECT t.project_id AS project, p.workspace_root AS root FROM orchestration_v2_projection_threads t
      JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.thread_id = ${threadId} AND t.deleted_at IS NULL AND p.deleted_at IS NULL
    `.pipe(Effect.mapError(() => denied(scope)));
    if (
      rows.length !== 1 ||
      !resourceGrantAllowsProject(value.resourceGrant, ProjectId.make(rows[0]!.project))
    )
      return yield* denied(scope);
    const latest = yield* current(authority, scope);
    if (
      !latest ||
      !resourceGrantAllowsProject(latest.resourceGrant, ProjectId.make(rows[0]!.project))
    )
      return yield* denied(scope);
    if (latest.resourceGrant.projects.mode !== "all" && !validRoot(rows[0]!.root))
      return yield* denied(scope);
    return rows[0]!;
  });
  const roots = Effect.fnUntraced(function* (scope: AuthEnvironmentScope) {
    return yield* sql<{
      readonly project: string;
      readonly thread: string | null;
      readonly root: string | null;
    }>`
      SELECT project_id AS project, NULL AS thread, workspace_root AS root
      FROM projection_projects WHERE deleted_at IS NULL
      UNION ALL
      SELECT t.project_id, t.thread_id,
        CASE WHEN json_valid(t.payload_json) THEN
          CASE WHEN json_type(t.payload_json, '$.worktreePath') = 'text'
            THEN json_extract(t.payload_json, '$.worktreePath') END END
      FROM orchestration_v2_projection_threads t
      JOIN projection_projects p ON p.project_id = t.project_id
      WHERE t.deleted_at IS NULL AND p.deleted_at IS NULL
      ORDER BY project, thread, root
    `.pipe(Effect.mapError(() => denied(scope)));
  });
  const admit = Effect.fn("workspaceReadAccess.admit")(function* (
    authority: ThreadReadAuthority,
    input: WorkspaceReadInput,
    scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
  ) {
    const value = yield* current(authority, scope);
    if (value === undefined) return;
    // Owner credentials retain host cwd compatibility, but still reload the action scope.
    if (
      value.resourceGrant.projects.mode === "all" &&
      value.resourceGrant.providers.mode === "all" &&
      input.projectId === undefined &&
      input.threadId === undefined
    )
      return;
    type RegisteredRoot = Effect.Success<ReturnType<typeof roots>>[number];
    let originalAssociation: ReadonlyArray<RegisteredRoot> | undefined;
    // ponytail: retry unrelated registration churn once; deny if it keeps changing.
    for (let attempt = 0; attempt < 2; attempt++) {
      const value = yield* current(authority, scope);
      if (value === undefined) return;
      if (input.threadId !== undefined) yield* thread(authority, input.threadId, scope);
      const registered = yield* roots(scope);
      if (
        input.projectId !== undefined &&
        !registered.some(
          (r) =>
            r.project === input.projectId &&
            r.thread === null &&
            resourceGrantAllowsProject(value.resourceGrant, ProjectId.make(r.project)),
        )
      )
        return yield* denied(scope);
      if (
        input.threadId !== undefined &&
        input.projectId !== undefined &&
        !registered.some((r) => r.thread === input.threadId && r.project === input.projectId)
      )
        return yield* denied(scope);
      if (!validRoot(input.cwd)) return yield* denied(scope);
      const candidates = registered.filter((r): r is typeof r & { readonly root: string } =>
        validRoot(r.root),
      );
      const threadProject =
        input.threadId === undefined
          ? undefined
          : registered.find((r) => r.thread === input.threadId)?.project;
      const allowed = (row: (typeof candidates)[number]) =>
        registered.some(
          (parent) =>
            parent.thread === null && parent.project === row.project && validRoot(parent.root),
        ) &&
        resourceGrantAllowsProject(value.resourceGrant, ProjectId.make(row.project)) &&
        (input.projectId === undefined || row.project === input.projectId) &&
        (threadProject === undefined || row.project === threadProject) &&
        (input.threadId === undefined || row.thread === null || row.thread === input.threadId);
      const requested = NodePath.resolve(input.cwd);
      const rawMatches = candidates.filter((r) => within(requested, NodePath.resolve(r.root)));
      const rawDepth = Math.max(...rawMatches.map((r) => NodePath.resolve(r.root).length));
      if (rawMatches.some((r) => NodePath.resolve(r.root).length === rawDepth && !allowed(r)))
        return yield* denied(scope);
      if (!candidates.some(allowed)) return yield* denied(scope);
      const canonical = yield* fs.realPath(input.cwd).pipe(Effect.mapError(() => denied(scope)));
      const matches = yield* Effect.forEach(candidates, (row) =>
        fs.realPath(row.root).pipe(
          Effect.map((root) => ({ ...row, canonical: root })),
          Effect.orElseSucceed(() => undefined),
        ),
      );
      const containing = matches.flatMap((r) =>
        r !== undefined && within(canonical, r.canonical) ? [r] : [],
      );
      const depth = Math.max(...containing.map((r) => r.canonical.length));
      const owners = containing.filter((r) => r.canonical.length === depth);
      if (owners.length === 0 || owners.some((r) => !allowed(r))) return yield* denied(scope);
      // Git/VCS discovery walks ancestors. A registered child cannot route reads into a foreign
      // repository above its boundary, even when the child itself has no repository marker.
      for (let directory = canonical; ; directory = NodePath.dirname(directory)) {
        const repository = yield* Effect.all([
          fs.exists(NodePath.join(directory, ".git")),
          fs.exists(NodePath.join(directory, ".jj")),
        ]).pipe(Effect.mapError(() => denied(scope)));
        if (repository.some(Boolean)) {
          const repoOwners = matches.flatMap((r) =>
            r !== undefined && within(directory, r.canonical) ? [r] : [],
          );
          const repoDepth = Math.max(...repoOwners.map((r) => r.canonical.length));
          if (
            !repoOwners.some((r) => r.canonical.length === repoDepth) ||
            repoOwners.some(
              (r) =>
                r.canonical.length === repoDepth &&
                (!allowed(r) || !owners.some((owner) => owner.project === r.project)),
            )
          )
            return yield* denied(scope);
          break;
        }
        if (NodePath.dirname(directory) === directory) break;
      }
      // No SQL transaction spans realPath. Changes to the actual association invalidate admission.
      originalAssociation ??= registered.filter(
        (r) =>
          (r.thread === null && owners.some((owner) => owner.project === r.project)) ||
          owners.some(
            (owner) =>
              owner.project === r.project && owner.thread === r.thread && owner.root === r.root,
          ) ||
          (input.threadId !== undefined && r.thread === input.threadId),
      );
      const latest = yield* current(authority, scope);
      const liveRoots = yield* roots(scope);
      const sameRoot = (a: RegisteredRoot, b: RegisteredRoot) =>
        a.project === b.project && a.thread === b.thread && a.root === b.root;
      if (
        !latest ||
        originalAssociation.some((r) => !liveRoots.some((live) => sameRoot(r, live))) ||
        owners.some(
          (r) => !resourceGrantAllowsProject(latest.resourceGrant, ProjectId.make(r.project)),
        )
      )
        return yield* denied(scope);
      if (
        registered.length === liveRoots.length &&
        registered.every((r, i) => sameRoot(r, liveRoots[i]!))
      )
        return owners;
    }
    return yield* denied(scope);
  });
  const repository = Effect.fnUntraced(function* (
    authority: ThreadReadAuthority,
    input: WorkspaceReadInput,
    root: string,
    scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
  ) {
    const source = yield* admit(authority, input, scope);
    const destination = yield* admit(authority, { ...input, cwd: root }, scope);
    if (
      source !== undefined &&
      (destination === undefined ||
        destination.some((r) => !source.some((s) => s.project === r.project)))
    )
      return yield* denied(scope);
    yield* admit(authority, input, scope);
  });
  const read = <A, E, R>(
    authority: ThreadReadAuthority,
    input: WorkspaceReadInput,
    operation: Effect.Effect<A, E, R>,
    scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
  ) =>
    run(
      authority,
      input,
      operation.pipe(Effect.tapError(() => admit(authority, input, scope))),
      scope,
    );
  const capture = Effect.fn("workspaceReadAccess.capture")(function* (
    authority: ThreadReadAuthority,
    requestedInput: WorkspaceReadInput,
    scope: AuthEnvironmentScope,
  ) {
    const input = { ...requestedInput };
    const owners = yield* admit(authority, input, scope);
    const owner =
      input.threadId === undefined ? undefined : yield* thread(authority, input.threadId, scope);
    const projectId = owner === undefined ? owners?.[0]?.project : owner.project;
    const association =
      owners === undefined
        ? []
        : yield* roots(scope).pipe(
            Effect.map((rows) =>
              rows.filter(
                (r) =>
                  (r.thread === null && owners.some((o) => o.project === r.project)) ||
                  owners.some(
                    (o) => o.project === r.project && o.thread === r.thread && o.root === r.root,
                  ) ||
                  (input.threadId !== undefined && r.thread === input.threadId),
              ),
            ),
          );
    const checkSql = Effect.gen(function* () {
      const value = yield* current(authority, scope);
      if (value === undefined) return;
      if (association.length === 0) return;
      const live = yield* roots(scope);
      if (
        association.some(
          (r) =>
            !resourceGrantAllowsProject(value.resourceGrant, ProjectId.make(r.project)) ||
            !live.some(
              (l) => l.project === r.project && l.thread === r.thread && l.root === r.root,
            ),
        )
      )
        return yield* denied(scope);
      if (owner !== undefined && input.threadId !== undefined) {
        const actual = yield* thread(authority, input.threadId, scope);
        if (actual?.project !== owner.project || actual.root !== owner.root)
          return yield* denied(scope);
      }
      yield* current(authority, scope);
    });
    const beforeEffect = Effect.fnUntraced(function* (
      cwd: string,
      requiredScope: AuthEnvironmentScope = scope,
    ) {
      yield* checkSql;
      yield* repository(authority, input, cwd, requiredScope);
      yield* checkSql;
    });
    const admission: WorkspaceActionAdmission = {
      authority,
      input,
      scope,
      projectId: projectId === undefined ? undefined : ProjectId.make(projectId),
      checkSql,
      beforeEffect,
    };
    yield* admission.beforeEffect(input.cwd);
    return admission;
  });
  const run = <A, E, R>(
    authority: ThreadReadAuthority,
    input: WorkspaceReadInput,
    operation: Effect.Effect<A, E, R>,
    scope: AuthEnvironmentScope,
    afterSuccess?: WorkspaceActionCallback<A>,
  ) =>
    Effect.gen(function* () {
      const inherited = yield* workspaceEffectAdmission;
      const admission = yield* capture(authority, input, scope);
      const checked =
        inherited === undefined
          ? admission
          : {
              ...admission,
              beforeEffect: (cwd: string, scope?: AuthEnvironmentScope) =>
                inherited
                  .beforeEffect(cwd)
                  .pipe(Effect.andThen(admission.beforeEffect(cwd, scope))),
              checkSql: inherited.checkSql.pipe(Effect.andThen(admission.checkSql)),
            };
      return yield* operation.pipe(
        Effect.tap(() => checked.beforeEffect(checked.input.cwd)),
        Effect.tap((result) =>
          afterSuccess === undefined ? Effect.void : afterSuccess(checked, result),
        ),
        Effect.provideService(workspaceEffectAdmission, checked),
      );
    });
  return { admit, read, thread, repository, capture, run };
});
