import {
  AuthSourceControlWriteScope,
  AuthOrchestrationReadScope,
  type AuthEnvironmentScope,
  EnvironmentAuthorizationError,
} from "@t3tools/contracts";
import * as WorkspaceReadAccess from "../auth/workspaceReadAccess.ts";
import { type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import {
  GitManagerError,
  GitCommandError,
  type VcsSwitchRefInput,
  type VcsSwitchRefResult,
  type VcsCreateRefInput,
  type VcsCreateRefResult,
  type VcsCreateWorktreeInput,
  type VcsCreateWorktreeResult,
  type VcsListRefsInput,
  type VcsListRefsResult,
  type GitManagerServiceError,
  type GitPreparePullRequestThreadInput,
  type GitPreparePullRequestThreadResult,
  type GitPullRequestRefInput,
  type VcsPullResult,
  type VcsRemoveWorktreeInput,
  type GitResolvePullRequestResult,
  type GitRunStackedActionInput,
  type GitRunStackedActionResult,
  type VcsStatusInput,
  type VcsStatusLocalResult,
  type VcsStatusRemoteResult,
  type VcsStatusResult,
} from "@t3tools/contracts";

import * as GitManager from "./GitManager.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";

export class GitWorkflowService extends Context.Service<
  GitWorkflowService,
  {
    readonly isRepository: (
      authority: ThreadReadAuthority,
      cwd: string,
    ) => Effect.Effect<boolean, GitManagerServiceError | EnvironmentAuthorizationError>;
    readonly hasCommit: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
        readonly refName: string;
      },
    ) => Effect.Effect<boolean, GitCommandError | EnvironmentAuthorizationError>;
    readonly status: (
      authority: ThreadReadAuthority,
      input: VcsStatusInput,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<VcsStatusResult, GitManagerServiceError | EnvironmentAuthorizationError>;
    readonly localStatus: (
      authority: ThreadReadAuthority,
      input: VcsStatusInput,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<
      VcsStatusLocalResult,
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
    readonly remoteStatus: (
      authority: ThreadReadAuthority,
      input: VcsStatusInput,
      options?: GitManager.GitRemoteStatusOptions,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<
      VcsStatusRemoteResult | null,
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
    readonly invalidateLocalStatus: (
      authority: ThreadReadAuthority,
      cwd: string,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<void, EnvironmentAuthorizationError>;
    readonly invalidateRemoteStatus: (
      authority: ThreadReadAuthority,
      cwd: string,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<void, EnvironmentAuthorizationError>;
    readonly invalidateStatus: (
      authority: ThreadReadAuthority,
      cwd: string,
      scope?: AuthEnvironmentScope,
    ) => Effect.Effect<void, EnvironmentAuthorizationError>;
    readonly pullCurrentBranch: (
      authority: ThreadReadAuthority,
      cwd: string,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<VcsPullResult>,
    ) => Effect.Effect<VcsPullResult, GitCommandError | EnvironmentAuthorizationError>;
    readonly runStackedAction: (
      authority: ThreadReadAuthority,
      input: GitRunStackedActionInput,
      options?: GitManager.GitRunStackedActionOptions,
    ) => Effect.Effect<
      GitRunStackedActionResult,
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
    readonly resolvePullRequest: (
      authority: ThreadReadAuthority,
      input: GitPullRequestRefInput,
    ) => Effect.Effect<
      GitResolvePullRequestResult,
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
    readonly preparePullRequestThread: (
      authority: ThreadReadAuthority,
      input: GitPreparePullRequestThreadInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<GitPreparePullRequestThreadResult>,
    ) => Effect.Effect<
      GitPreparePullRequestThreadResult,
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
    readonly listRefs: (
      authority: ThreadReadAuthority,
      input: VcsListRefsInput,
    ) => Effect.Effect<VcsListRefsResult, GitCommandError | EnvironmentAuthorizationError>;
    readonly createWorktree: (
      authority: ThreadReadAuthority,
      input: VcsCreateWorktreeInput,
      options?: GitVcsDriver.CreateWorktreeOptions,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<VcsCreateWorktreeResult>,
    ) => Effect.Effect<VcsCreateWorktreeResult, GitCommandError | EnvironmentAuthorizationError>;
    readonly listLocalBranchNames: (
      authority: ThreadReadAuthority,
      cwd: string,
    ) => Effect.Effect<string[], GitCommandError | EnvironmentAuthorizationError>;
    readonly fetchRemote: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
        readonly remoteName: string;
        readonly refName?: string;
      },
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<void>,
    ) => Effect.Effect<void, GitCommandError | EnvironmentAuthorizationError>;
    readonly remoteExists: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
        readonly remoteName: string;
      },
    ) => Effect.Effect<boolean, GitCommandError | EnvironmentAuthorizationError>;
    readonly remoteBranchExists: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
        readonly remoteName: string;
        readonly refName: string;
      },
    ) => Effect.Effect<boolean, GitCommandError | EnvironmentAuthorizationError>;
    readonly resolveRemoteTrackingCommit: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
        readonly refName: string;
        readonly fallbackRemoteName: string;
      },
    ) => Effect.Effect<
      { readonly commitSha: string; readonly remoteRefName: string },
      GitCommandError | EnvironmentAuthorizationError
    >;
    readonly removeWorktree: (
      authority: ThreadReadAuthority,
      input: VcsRemoveWorktreeInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<void>,
    ) => Effect.Effect<void, GitCommandError | EnvironmentAuthorizationError>;
    readonly pruneWorktrees: (
      authority: ThreadReadAuthority,
      input: {
        readonly cwd: string;
      },
    ) => Effect.Effect<void, GitCommandError | EnvironmentAuthorizationError>;
    readonly deleteLocalBranch: (
      authority: ThreadReadAuthority,
      input: GitVcsDriver.GitDeleteLocalBranchInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<void>,
    ) => Effect.Effect<void, GitCommandError | EnvironmentAuthorizationError>;
    readonly createRef: (
      authority: ThreadReadAuthority,
      input: VcsCreateRefInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<VcsCreateRefResult>,
    ) => Effect.Effect<VcsCreateRefResult, GitCommandError | EnvironmentAuthorizationError>;
    readonly switchRef: (
      authority: ThreadReadAuthority,
      input: VcsSwitchRefInput,
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<VcsSwitchRefResult>,
    ) => Effect.Effect<VcsSwitchRefResult, GitCommandError | EnvironmentAuthorizationError>;
    readonly renameBranch: (
      authority: ThreadReadAuthority,
      input: {
        readonly exactName?: boolean;
        readonly cwd: string;
        readonly oldBranch: string;
        readonly newBranch: string;
      },
      afterSuccess?: WorkspaceReadAccess.WorkspaceActionCallback<{ readonly branch: string }>,
    ) => Effect.Effect<
      { readonly branch: string },
      GitManagerServiceError | EnvironmentAuthorizationError
    >;
  }
>()("t3/git/GitWorkflowService") {}

function nonRepositoryLocalStatus(): VcsStatusLocalResult {
  return {
    isRepo: false,
    hasPrimaryRemote: false,
    isDefaultRef: false,
    refName: null,
    hasWorkingTreeChanges: false,
    workingTree: {
      files: [],
      insertions: 0,
      deletions: 0,
    },
  };
}

function nonRepositoryStatus(): VcsStatusResult {
  return {
    ...nonRepositoryLocalStatus(),
    hasUpstream: false,
    aheadCount: 0,
    behindCount: 0,
    aheadOfDefaultCount: 0,
    pr: null,
  };
}

function nonRepositoryListRefs(): VcsListRefsResult {
  return {
    refs: [],
    isRepo: false,
    hasPrimaryRemote: false,
    nextCursor: null,
    totalCount: 0,
  };
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const access = yield* WorkspaceReadAccess.make;
  const registry = yield* VcsDriverRegistry.VcsDriverRegistry;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const gitManager = yield* GitManager.GitManager;

  const ensureGit = Effect.fn("GitWorkflowService.ensureGit")(function* (
    operation: string,
    cwd: string,
  ) {
    const handle = yield* registry.resolve({ cwd }).pipe(
      Effect.mapError((cause) =>
        cause._tag === "EnvironmentAuthorizationError"
          ? cause
          : new GitManagerError({
              operation,
              cwd,
              detail: "Failed to resolve the VCS driver for this Git workflow.",
              cause,
            }),
      ),
    );
    if (handle.kind !== "git") {
      return yield* new GitManagerError({
        operation,
        cwd,
        detail: `The ${operation} workflow currently supports Git repositories only; detected ${handle.kind}. (${cwd})`,
      });
    }
    return handle;
  });

  const ensureGitCommand = Effect.fn("GitWorkflowService.ensureGitCommand")(function* (
    operation: string,
    cwd: string,
  ) {
    const handle = yield* registry.resolve({ cwd }).pipe(
      Effect.mapError((cause) =>
        cause._tag === "EnvironmentAuthorizationError"
          ? cause
          : new GitCommandError({
              operation,
              command: "vcs-route",
              cwd,
              detail: "Failed to resolve the VCS driver for this Git command.",
              cause,
            }),
      ),
    );
    if (handle.kind !== "git") {
      return yield* new GitCommandError({
        operation,
        command: "vcs-route",
        cwd,
        detail: `The ${operation} command currently supports Git repositories only; detected ${handle.kind}.`,
      });
    }
    return handle;
  });

  const detectGitRepositoryForStatus = Effect.fn("GitWorkflowService.detectGitRepositoryForStatus")(
    function* (
      operation: string,
      cwd: string,
      authority: ThreadReadAuthority,
      scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
    ) {
      const handle = yield* registry.detect({ cwd }).pipe(
        Effect.mapError((cause) =>
          cause._tag === "EnvironmentAuthorizationError"
            ? cause
            : new GitManagerError({
                operation,
                cwd,
                detail: "Failed to detect a VCS repository for this Git workflow.",
                cause,
              }),
        ),
      );
      if (!handle) {
        return false;
      }
      yield* access.repository(authority, { cwd }, handle.repository.rootPath, scope);
      if (handle.kind !== "git") {
        return yield* new GitManagerError({
          operation,
          cwd,
          detail: `The ${operation} workflow currently supports Git repositories only; detected ${handle.kind}. (${cwd})`,
        });
      }
      return true;
    },
  );

  const detectGitRepositoryForCommand = Effect.fn(
    "GitWorkflowService.detectGitRepositoryForCommand",
  )(function* (
    operation: string,
    cwd: string,
    authority: ThreadReadAuthority,
    scope: AuthEnvironmentScope = AuthOrchestrationReadScope,
  ) {
    const handle = yield* registry.detect({ cwd }).pipe(
      Effect.mapError((cause) =>
        cause._tag === "EnvironmentAuthorizationError"
          ? cause
          : new GitCommandError({
              operation,
              command: "vcs-route",
              cwd,
              detail: "Failed to detect a VCS repository for this Git command.",
              cause,
            }),
      ),
    );
    if (!handle) {
      return false;
    }
    yield* access.repository(authority, { cwd }, handle.repository.rootPath, scope);
    if (handle.kind !== "git") {
      return yield* new GitCommandError({
        operation,
        command: "vcs-route",
        cwd,
        detail: `The ${operation} command currently supports Git repositories only; detected ${handle.kind}.`,
      });
    }
    return true;
  });

  return GitWorkflowService.of({
    isRepository: (authority, cwd) =>
      access.read(
        authority,
        { cwd },
        registry.detect({ cwd }).pipe(
          Effect.tap((handle) =>
            handle === null
              ? Effect.void
              : access.repository(authority, { cwd }, handle.repository.rootPath),
          ),
          Effect.map((handle) => handle?.kind === "git"),
          Effect.mapError((cause) =>
            cause._tag === "EnvironmentAuthorizationError"
              ? cause
              : new GitManagerError({
                  operation: "GitWorkflowService.isRepository",
                  cwd,
                  detail: "Failed to detect a VCS repository for this Git workflow.",
                  cause,
                }),
          ),
        ),
      ),
    hasCommit: (authority, input) =>
      access.read(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.hasCommit", input.cwd).pipe(
          Effect.tap((handle) => access.repository(authority, input, handle.repository.rootPath)),
          Effect.andThen(access.admit(authority, input)),
          Effect.andThen(
            git.execute({
              operation: "GitWorkflowService.hasCommit",
              cwd: input.cwd,
              args: ["rev-parse", "--verify", `${input.refName}^{commit}`],
              allowNonZeroExit: true,
            }),
          ),
          Effect.map((result) => result.exitCode === 0),
        ),
      ),
    status: (authority, input, scope) =>
      access.read(
        authority,
        input,
        detectGitRepositoryForStatus("GitWorkflowService.status", input.cwd, authority, scope).pipe(
          Effect.flatMap((isGitRepository) =>
            isGitRepository
              ? gitManager.status(authority, input, scope)
              : Effect.succeed(nonRepositoryStatus()),
          ),
        ),
        scope,
      ),
    localStatus: (authority, input, scope) =>
      access.read(
        authority,
        input,
        detectGitRepositoryForStatus(
          "GitWorkflowService.localStatus",
          input.cwd,
          authority,
          scope,
        ).pipe(
          Effect.flatMap((isGitRepository) =>
            isGitRepository
              ? gitManager.localStatus(authority, input, scope)
              : Effect.succeed(nonRepositoryLocalStatus()),
          ),
        ),
        scope,
      ),
    remoteStatus: (authority, input, options, scope) =>
      access.read(
        authority,
        input,
        detectGitRepositoryForStatus(
          "GitWorkflowService.remoteStatus",
          input.cwd,
          authority,
          scope,
        ).pipe(
          Effect.flatMap((isGitRepository) =>
            isGitRepository
              ? gitManager.remoteStatus(authority, input, options, scope)
              : Effect.succeed(null),
          ),
        ),
        scope,
      ),
    invalidateLocalStatus: gitManager.invalidateLocalStatus,
    invalidateRemoteStatus: gitManager.invalidateRemoteStatus,
    invalidateStatus: gitManager.invalidateStatus,
    pullCurrentBranch: (authority, cwd, afterSuccess) =>
      access.run(
        authority,
        { cwd },
        ensureGitCommand("GitWorkflowService.pullCurrentBranch", cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              { cwd },
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(cwd)),
          Effect.andThen(git.pullCurrentBranch(cwd)),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
    runStackedAction: (authority, input, options) =>
      access.run(
        authority,
        input,
        ensureGit("GitWorkflowService.runStackedAction", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(gitManager.runStackedAction(authority, input, options)),
        ),
        AuthSourceControlWriteScope,
      ),
    resolvePullRequest: (authority, input) =>
      access.read(
        authority,
        input,
        ensureGit("GitWorkflowService.resolvePullRequest", input.cwd).pipe(
          Effect.tap((handle) => access.repository(authority, input, handle.repository.rootPath)),
          Effect.andThen(gitManager.resolvePullRequest(authority, input)),
        ),
      ),
    preparePullRequestThread: (authority, input, afterSuccess) =>
      access.runWorktree(
        authority,
        input,
        ensureGit("GitWorkflowService.preparePullRequestThread", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(gitManager.preparePullRequestThread(authority, input)),
          Effect.tap((result) =>
            result.worktreePath === null
              ? Effect.void
              : WorkspaceReadAccess.beforeWorkspaceEffect(result.worktreePath),
          ),
        ),
        {
          additionalScope:
            input.mode === "worktree" && input.threadId !== undefined
              ? "orchestration:operate"
              : undefined,
          afterSuccess,
        },
      ),
    listRefs: (authority, input) =>
      access.read(
        authority,
        input,
        detectGitRepositoryForCommand("GitWorkflowService.listRefs", input.cwd, authority).pipe(
          Effect.flatMap((isGitRepository) =>
            isGitRepository
              ? access.admit(authority, input).pipe(Effect.andThen(git.listRefs(input)))
              : Effect.succeed(nonRepositoryListRefs()),
          ),
        ),
      ),
    createWorktree: (authority, input, options, afterSuccess) =>
      access.runWorktree(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.createWorktree", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(gitManager.createWorktree(authority, input, options)),
          Effect.tap((result) => WorkspaceReadAccess.beforeWorkspaceEffect(result.worktree.path)),
        ),
        { destination: input.path, afterSuccess },
      ),
    listLocalBranchNames: (authority, cwd) =>
      access.read(
        authority,
        { cwd },
        ensureGitCommand("GitWorkflowService.listLocalBranchNames", cwd).pipe(
          Effect.tap((handle) => access.repository(authority, { cwd }, handle.repository.rootPath)),
          Effect.andThen(access.admit(authority, { cwd })),
          Effect.andThen(git.listLocalBranchNames(cwd)),
        ),
      ),
    fetchRemote: (authority, input, afterSuccess) =>
      access.run(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.fetchRemote", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd)),
          Effect.andThen(git.fetchRemote(input)),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
    remoteExists: (authority, input) =>
      access.read(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.remoteExists", input.cwd).pipe(
          Effect.tap((handle) => access.repository(authority, input, handle.repository.rootPath)),
          Effect.andThen(access.admit(authority, input)),
          Effect.andThen(git.remoteExists(input)),
        ),
      ),
    remoteBranchExists: (authority, input) =>
      access.read(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.remoteBranchExists", input.cwd).pipe(
          Effect.tap((handle) => access.repository(authority, input, handle.repository.rootPath)),
          Effect.andThen(access.admit(authority, input)),
          Effect.andThen(git.remoteBranchExists(input)),
        ),
      ),
    resolveRemoteTrackingCommit: (authority, input) =>
      access.read(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.resolveRemoteTrackingCommit", input.cwd).pipe(
          Effect.tap((handle) => access.repository(authority, input, handle.repository.rootPath)),
          Effect.andThen(access.admit(authority, input)),
          Effect.andThen(git.resolveRemoteTrackingCommit(input)),
        ),
      ),
    removeWorktree: (authority, input, afterSuccess) =>
      access.runWorktree(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.removeWorktree", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(git.removeWorktree(input)),
        ),
        { destination: input.path, afterSuccess },
      ),
    pruneWorktrees: (authority, input) =>
      access.runWorktree(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.pruneWorktrees", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(git.pruneWorktrees(input)),
        ),
      ),
    deleteLocalBranch: (authority, input, afterSuccess) =>
      access.run(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.deleteLocalBranch", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd)),
          Effect.andThen(git.deleteLocalBranch(input)),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
    createRef: (authority, input, afterSuccess) =>
      access.run(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.createRef", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd)),
          Effect.andThen(git.createRef(input)),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
    switchRef: (authority, input, afterSuccess) =>
      access.run(
        authority,
        input,
        ensureGitCommand("GitWorkflowService.switchRef", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd)),
          Effect.andThen(Effect.scoped(git.switchRef(input))),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
    renameBranch: (authority, input, afterSuccess) =>
      access.run(
        authority,
        input,
        ensureGit("GitWorkflowService.renameBranch", input.cwd).pipe(
          Effect.tap((handle) =>
            access.repository(
              authority,
              input,
              handle.repository.rootPath,
              AuthSourceControlWriteScope,
            ),
          ),
          Effect.andThen(WorkspaceReadAccess.beforeWorkspaceEffect(input.cwd)),
          Effect.andThen(git.renameBranch(input)),
        ),
        AuthSourceControlWriteScope,
        afterSuccess,
      ),
  });
});

export const layer = Layer.effect(GitWorkflowService, make);
