import { AuthFilesystemReadScope, EnvironmentAuthorizationError } from "@t3tools/contracts";
import * as WorkspaceReadAccess from "../auth/workspaceReadAccess.ts";
import { trustedReadAuthority, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import {
  VcsRepositoryDetectionError,
  VcsUnsupportedOperationError,
  type ReviewDiffFileContentsInput,
  type ReviewDiffFileContentsResult,
  type ReviewDiffPreviewError,
  type ReviewDiffPreviewInput,
  type ReviewDiffPreviewResult,
} from "@t3tools/contracts";

import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import { isFilesystemRoot, managedWorktreesDirectories } from "../worktreesDirectory.ts";

export class ReviewService extends Context.Service<
  ReviewService,
  {
    readonly getDiffPreview: (
      authority: ThreadReadAuthority,
      input: ReviewDiffPreviewInput,
    ) => Effect.Effect<
      ReviewDiffPreviewResult,
      ReviewDiffPreviewError | EnvironmentAuthorizationError
    >;
    readonly getDiffFileContents: (
      authority: ThreadReadAuthority,
      input: ReviewDiffFileContentsInput,
    ) => Effect.Effect<
      ReviewDiffFileContentsResult,
      ReviewDiffPreviewError | EnvironmentAuthorizationError
    >;
  }
>()("t3/review/ReviewService") {}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const access = yield* WorkspaceReadAccess.make;
  const config = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const vcsRegistry = yield* VcsDriverRegistry.VcsDriverRegistry;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const settings = yield* ServerSettings.ServerSettingsService;

  const canonicalizePath = (value: string) => {
    const resolvedPath = path.resolve(value);
    return fileSystem.realPath(resolvedPath).pipe(
      Effect.catchTags({
        PlatformError: (cause) =>
          cause.reason._tag === "NotFound"
            ? Effect.succeed(resolvedPath)
            : Effect.fail(
                new VcsRepositoryDetectionError({
                  operation: "ReviewService.assertWorkspaceBoundCwd.canonicalizePath",
                  cwd: resolvedPath,
                  detail: "Failed to resolve a path while validating the review workspace.",
                  cause,
                }),
              ),
      }),
    );
  };

  const isWithinRoot = (candidate: string, root: string) => {
    const relative = path.relative(root, candidate);
    return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
  };

  const assertWorkspaceBoundCwd = Effect.fn("ReviewService.assertWorkspaceBoundCwd")(function* (
    operation: "ReviewService.getDiffPreview" | "ReviewService.getDiffFileContents",
    cwd: string,
  ) {
    const worktreesDirectories = yield* settings.getSettings.pipe(
      Effect.orElseSucceed(() => ({ worktreesDirectory: "", previousWorktreesDirectories: [] })),
    );
    const [candidate, workspaceRoot, worktreesRoots] = yield* Effect.all([
      canonicalizePath(cwd),
      canonicalizePath(config.cwd),
      // A managed root that cannot be resolved, or resolves to a filesystem
      // root through a symlink, is skipped rather than failing every review.
      Effect.forEach(
        managedWorktreesDirectories(worktreesDirectories, config.worktreesDir, path),
        (directory) => canonicalizePath(directory).pipe(Effect.orElseSucceed(() => null)),
      ).pipe(
        Effect.map((roots) =>
          roots.filter((root): root is string => root !== null && !isFilesystemRoot(root, path)),
        ),
      ),
    ]);

    if (
      isWithinRoot(candidate, workspaceRoot) ||
      worktreesRoots.some((root) => isWithinRoot(candidate, root))
    ) {
      return;
    }

    return yield* new VcsRepositoryDetectionError({
      operation,
      cwd,
      detail:
        operation === "ReviewService.getDiffPreview"
          ? "Review diff preview cwd must stay within the configured workspace root."
          : "Review diff file contents cwd must stay within the configured workspace root.",
    });
  });

  const getDiffPreview = Effect.fn("ReviewService.getDiffPreview")(function* (
    authority: ThreadReadAuthority,
    input: ReviewDiffPreviewInput,
  ) {
    const grant =
      authority === trustedReadAuthority ? undefined : (yield* authority.current).resourceGrant;
    if (grant === undefined || (grant.projects.mode === "all" && grant.providers.mode === "all"))
      yield* assertWorkspaceBoundCwd("ReviewService.getDiffPreview", input.cwd);

    yield* access.admit(authority, input, AuthFilesystemReadScope);
    const handle = yield* vcsRegistry.detect({ cwd: input.cwd, requestedKind: "auto" });
    if (!handle) {
      return {
        cwd: input.cwd,
        generatedAt: yield* DateTime.now,
        sources: [],
      };
    }

    yield* access.repository(authority, input, handle.repository.rootPath, AuthFilesystemReadScope);
    const getDriverDiffPreview = handle.driver.getDiffPreview;
    if (!getDriverDiffPreview) {
      if (handle.kind === "git") {
        return yield* git.getReviewDiffPreview(input);
      }
      return yield* new VcsUnsupportedOperationError({
        operation: "ReviewService.getDiffPreview",
        kind: handle.kind,
        detail: `The ${handle.kind} VCS driver does not support review diff previews.`,
      });
    }

    return yield* getDriverDiffPreview(input);
  });

  const getDiffFileContents = Effect.fn("ReviewService.getDiffFileContents")(function* (
    authority: ThreadReadAuthority,
    input: ReviewDiffFileContentsInput,
  ) {
    const grant =
      authority === trustedReadAuthority ? undefined : (yield* authority.current).resourceGrant;
    if (grant === undefined || (grant.projects.mode === "all" && grant.providers.mode === "all"))
      yield* assertWorkspaceBoundCwd("ReviewService.getDiffFileContents", input.cwd);

    yield* access.admit(authority, input, AuthFilesystemReadScope);
    const handle = yield* vcsRegistry.detect({ cwd: input.cwd, requestedKind: "auto" });
    if (handle?.kind !== "git") {
      return yield* new VcsUnsupportedOperationError({
        operation: "ReviewService.getDiffFileContents",
        kind: handle?.kind ?? "unknown",
        detail: "Unchanged diff expansion currently requires a Git repository.",
      });
    }

    yield* access.repository(authority, input, handle.repository.rootPath, AuthFilesystemReadScope);
    return yield* git.getReviewDiffFileContents(input);
  });

  return ReviewService.of({
    getDiffPreview: (authority, input) =>
      access.read(authority, input, getDiffPreview(authority, input), AuthFilesystemReadScope),
    getDiffFileContents: (authority, input) =>
      access.read(authority, input, getDiffFileContents(authority, input), AuthFilesystemReadScope),
  });
});

export const layer = Layer.effect(ReviewService, make);
