import {
  authorizeCommand,
  authorizeCommandReceipt,
  authorizeMutationTarget,
  authorizeThreadMutation,
  receiptForMutationAuthority,
  trustedMutationAuthority,
  type MutationAuthority,
} from "../auth/CommandAuthorization.ts";
import {
  admitRegisteredCommandWorkspace,
  requireRegisteredMutationPath,
} from "../auth/threadWorkspaceAccess.ts";
import { trustedReadAuthority } from "../auth/ResourceAuthorization.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  EnvironmentAuthorizationError,
  type ChatAttachment,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type OrchestrationV2ThreadProjection,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
  type ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import {
  buildTemporaryWorktreeBranchName,
  flattenTemporaryWorktreeBranchName,
  isTemporaryWorktreeBranch,
  WORKTREE_BRANCH_PREFIX,
} from "@t3tools/shared/git";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as Orchestrator from "./Orchestrator.ts";
import { makeProviderFailure } from "./ProviderFailure.ts";
import { randomUuidV4 } from "./RandomUuid.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export type ThreadLaunchWorkspaceStrategy =
  | { readonly type: "root"; readonly branch?: string | undefined }
  | {
      readonly type: "existing_worktree";
      readonly worktreePath: string;
      readonly branch?: string | undefined;
    }
  | {
      readonly type: "worktree";
      readonly baseRef: string;
      readonly branch?: string | undefined;
      readonly startFromOrigin?: boolean | undefined;
    };

export interface ThreadLaunchInitialMessage {
  readonly messageId?: MessageId;
  readonly scheduledTaskId?: ScheduledTaskId;
  readonly senderThreadId?: ThreadId;
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
}

export interface ThreadLaunchInput {
  readonly commandId: CommandId;
  readonly threadId?: ThreadId;
  readonly reuseExistingThread?: boolean;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly generateTitle?: boolean;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
  readonly initialMessage?: ThreadLaunchInitialMessage;
  readonly importedNativeThread?: {
    readonly ref: {
      readonly driver: ProviderDriverKind;
      readonly nativeId: string;
      readonly strength: "strong";
    };
    readonly metadata?: OrchestrationV2ProviderThreadNativeMetadata;
  };
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
}

export function launchCreateCommand(input: ThreadLaunchInput, threadId: ThreadId) {
  return {
    type: "thread.create" as const,
    commandId: input.commandId,
    threadId,
    projectId: input.projectId,
    title: input.title,
    modelSelection: input.modelSelection,
    runtimeMode: input.runtimeMode,
    interactionMode: input.interactionMode,
    branch: input.workspaceStrategy.branch ?? null,
    worktreePath:
      input.workspaceStrategy.type === "existing_worktree"
        ? input.workspaceStrategy.worktreePath
        : null,
    ...(input.importedNativeThread === undefined
      ? {}
      : { importedNativeThread: input.importedNativeThread }),
    createdBy: input.createdBy,
    creationSource: input.creationSource,
  };
}

export function initialMessageCommand(
  input: ThreadLaunchInput,
  threadId: ThreadId,
  message: ThreadLaunchInitialMessage & { readonly messageId: MessageId },
  workspaceStrategy = input.workspaceStrategy,
) {
  return {
    ...message,
    type: "message.dispatch" as const,
    commandId: CommandId.make(`${input.commandId}:initial-message`),
    threadId,
    ...(input.generateTitle === true ? { titleSeed: input.title } : {}),
    modelSelection: input.modelSelection,
    dispatchMode: { type: "defer_start" as const, workspaceStrategy },
    createdBy: input.createdBy,
    creationSource: input.creationSource,
  };
}

/** What workspace preparation reads from a launch; a retry rebuilds it from the run. */
type PreparationInput = Pick<
  ThreadLaunchInput,
  "commandId" | "projectId" | "workspaceStrategy" | "initialMessage"
> & {
  /**
   * Set when a retry reuses the worktree its failed attempt created and
   * recorded. Its setup is tracked like a new one, but the thread already
   * records the workspace, and a branch rename may still be running.
   */
  readonly reusedWorktree?: { readonly baseRef: string };
};

export interface ThreadLaunchRetryInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
}

export interface ThreadLaunchResult {
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection;
  readonly resumed: boolean;
}

export class ThreadLaunchError extends Schema.TaggedError<ThreadLaunchError>()(
  "ThreadLaunchError",
  {
    operation: Schema.Literals([
      "resolve-project",
      "read-receipt",
      "generate-metadata",
      "provision-worktree",
      "run-setup-script",
      "create-thread",
      "update-thread",
      "dispatch-message",
      "release-run",
      "fail-run",
    ]),
    commandId: CommandId,
    projectId: ProjectId,
    threadId: Schema.optional(ThreadId),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Thread launch ${this.commandId} failed during ${this.operation}.`;
  }
}

export class ThreadLaunchService extends Context.Service<
  ThreadLaunchService,
  {
    readonly admit: (
      authority: MutationAuthority,
      input: ThreadLaunchInput,
    ) => Effect.Effect<ThreadLaunchInput & { readonly threadId: ThreadId }, ThreadLaunchError>;
    readonly launch: (
      authority: MutationAuthority,
      input: ThreadLaunchInput,
    ) => Effect.Effect<ThreadLaunchResult, ThreadLaunchError>;
    /** Dispatches prepared-run.retry and prepares the run's workspace again. */
    readonly retryPreparation: (
      authority: MutationAuthority,
      input: ThreadLaunchRetryInput,
    ) => Effect.Effect<Orchestrator.OrchestratorV2DispatchResult, Orchestrator.OrchestratorV2Error>;
  }
>()("t3/orchestration-v2/ThreadLaunchService") {}

const isThreadLaunchError = Schema.is(ThreadLaunchError);
const isProjectionError = Schema.is(Orchestrator.OrchestratorProjectionError);
const isAuthorizationError = Schema.is(EnvironmentAuthorizationError);

function failureDetail(error: unknown): string {
  if (isThreadLaunchError(error)) {
    const cause = error.cause;
    const detail = cause instanceof Error ? cause.message : String(cause);
    return `Workspace preparation failed during ${error.operation.replaceAll("-", " ")}: ${detail}`;
  }
  return `Workspace preparation failed: ${error instanceof Error ? error.message : String(error)}`;
}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const projects = yield* ProjectService.ProjectService;
  const setupTracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
  const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
  const terminals = yield* TerminalManager.TerminalManager;
  const git = yield* GitWorkflow.GitWorkflowService;
  const setupScripts = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const textGeneration = yield* TextGeneration.TextGeneration;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const preparationScope = yield* Scope.make("sequential");
  const scheduledLaunches = yield* Ref.make<ReadonlySet<CommandId>>(new Set());
  yield* Effect.addFinalizer(() => Scope.close(preparationScope, Exit.void));

  const mapError =
    (input: PreparationInput, operation: ThreadLaunchError["operation"], threadId?: ThreadId) =>
    (cause: unknown) =>
      isThreadLaunchError(cause)
        ? cause
        : new ThreadLaunchError({
            operation,
            commandId: input.commandId,
            projectId: input.projectId,
            ...(threadId === undefined ? {} : { threadId }),
            cause,
          });

  /** Shared launch admission also runs before intake claims any pending files. */
  const admitLaunch = Effect.fn("ThreadLaunchService.admitLaunch")(function* (
    authority: MutationAuthority,
    input: ThreadLaunchInput,
  ) {
    const messageCommandId = CommandId.make(`${input.commandId}:initial-message`);
    const admittedReceipts = yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* authorizeMutationTarget(sql, authority, { projectId: input.projectId });
        if (input.reuseExistingThread === true && input.threadId === undefined)
          return yield* new ThreadLaunchError({
            operation: "update-thread",
            commandId: input.commandId,
            projectId: input.projectId,
            cause: "Reusing an existing thread requires a thread id.",
          });
        yield* authorizeCommandReceipt(sql, authority, input.commandId);
        yield* authorizeCommandReceipt(sql, authority, messageCommandId);
        const launchReceipt = yield* receipts.getByCommandId(input.commandId);
        const messageReceipt = yield* receipts.getByCommandId(messageCommandId);
        for (const receipt of [launchReceipt, messageReceipt]) {
          if (Option.isSome(receipt) && receipt.value.status === "rejected") {
            const safe = yield* receiptForMutationAuthority(authority, receipt.value);
            return yield* mapError(
              input,
              safe.commandId === messageCommandId
                ? "dispatch-message"
                : input.reuseExistingThread === true
                  ? "update-thread"
                  : "create-thread",
              input.threadId,
            )(
              new Orchestrator.OrchestratorCommandPreviouslyRejectedError({
                commandId: safe.commandId,
                commandType: safe.commandType,
                detail: safe.error ?? "Previously rejected.",
              }),
            );
          }
        }
        return { launchReceipt, messageReceipt };
      }),
    );
    const { launchReceipt, messageReceipt } = admittedReceipts;
    const recovered =
      Option.isSome(launchReceipt) &&
      launchReceipt.value.status === "accepted" &&
      launchReceipt.value.commandType === "thread.create"
        ? launchReceipt.value.threadId
        : undefined;
    const threadId =
      input.threadId ?? recovered ?? (yield* ids.allocate.thread({ projectId: input.projectId }));
    yield* authorizeMutationTarget(sql, authority, {
      threadId,
      projectId: input.projectId,
      allowCreate: input.reuseExistingThread !== true,
    });
    for (const receipt of [launchReceipt, messageReceipt]) {
      if (
        Option.isSome(receipt) &&
        !Orchestrator.canReplayCommandReceipt(receipt.value.threadId, threadId)
      )
        return yield* mapError(
          input,
          receipt.value.commandId === messageCommandId
            ? "dispatch-message"
            : input.reuseExistingThread === true
              ? "update-thread"
              : "create-thread",
          threadId,
        )(
          new Orchestrator.OrchestratorCommandIdConflictError({
            commandId: receipt.value.commandId,
            commandType: receipt.value.commandType,
            receiptThreadId: receipt.value.threadId,
            commandThreadId: threadId,
          }),
        );
    }
    // Source/target admission precedes canonical workspace I/O. The final claim carries the path.
    yield* authorizeCommand(sql, authority, {
      ...launchCreateCommand(input, threadId),
      worktreePath: null,
    });
    const initialMessage =
      input.initialMessage === undefined
        ? undefined
        : {
            ...input.initialMessage,
            messageId:
              input.initialMessage.messageId ??
              (yield* ids.allocate.message({ threadId, ordinal: 1 })),
          };
    if (initialMessage !== undefined) {
      yield* authorizeCommand(
        sql,
        authority,
        initialMessageCommand(input, threadId, initialMessage),
        input.reuseExistingThread === true ? undefined : { threadId, projectId: input.projectId },
      );
    }
    if (input.workspaceStrategy.type === "existing_worktree")
      yield* admitRegisteredCommandWorkspace(
        sql,
        authority,
        {
          commandId: input.commandId,
          threadId,
          projectId: input.projectId,
          allowCreate: input.reuseExistingThread !== true,
        },
        input.workspaceStrategy.worktreePath,
      );
    return { ...input, threadId, ...(initialMessage === undefined ? {} : { initialMessage }) };
  });

  const readReceipt = (
    authority: MutationAuthority,
    input: ThreadLaunchInput,
    commandId: CommandId,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          yield* authorizeCommandReceipt(sql, authority, commandId);
          const receipt = yield* receipts.getByCommandId(commandId);
          return Option.isSome(receipt)
            ? Option.some(yield* receiptForMutationAuthority(authority, receipt.value))
            : receipt;
        }),
      )
      .pipe(Effect.mapError(mapError(input, "read-receipt", input.threadId)));

  const validateReusableThread = Effect.fn("ThreadLaunchService.validateReusableThread")(function* (
    input: ThreadLaunchInput,
    threadId: ThreadId,
  ) {
    const projection = yield* threads
      .getThreadRecords(trustedReadAuthority, threadId, ["runs"])
      .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
    if (
      projection.thread.projectId !== input.projectId ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      (yield* threads
        .getMessageCount(trustedReadAuthority, threadId)
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)))) > 0 ||
      projection.runs.length > 0
    ) {
      return yield* mapError(
        input,
        "update-thread",
        threadId,
      )("Only an empty active thread in the target project can change workspace during launch.");
    }
  });

  const prepareInBackground = Effect.fn("ThreadLaunchService.prepareInBackground")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError(mapError(input, "resolve-project", threadId)),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(mapError(input, "resolve-project", threadId)("Project no longer exists.")),
          onSome: Effect.succeed,
        }),
      ),
    );

    const reused = input.reusedWorktree;
    const tracked = input.workspaceStrategy.type === "worktree" || reused !== undefined;
    let createdWorktreePath: string | null = null;
    let setupTerminalId: string | null = null;
    let workspaceRecorded = false;
    if (input.workspaceStrategy.type === "worktree") {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: input.workspaceStrategy.baseRef,
        stages: ["fetch", "checkout", "setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    } else if (reused !== undefined) {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: reused.baseRef,
        stages: ["setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    }
    yield* Effect.gen(function* () {
      const initialMessage = input.initialMessage;
      const generateBranchNameFor = (cwd: string, message: ThreadLaunchInitialMessage) =>
        Effect.gen(function* () {
          const settings = resolveProjectSettings(
            yield* serverSettings.getSettings,
            input.projectId,
          ).settings;
          const modelSelection =
            settings.sourceControlWriterModelSelection === null
              ? settings.textGenerationModelSelection
              : ServerSettings.resolveSourceControlWriterModelSelection(
                  settings,
                  yield* providerRegistry.getProviders,
                );
          return yield* textGeneration
            .generateBranchName({
              naming: {
                mode: settings.branchNamingMode,
                prefix: settings.branchNamePrefix,
                instructions: settings.branchNameInstructions,
              },
              cwd,
              message: message.text,
              attachments: message.attachments,
              ...(message.context ? { context: message.context } : {}),
              modelSelection,
            })
            .pipe(
              Effect.map((result) => ({
                branch: result.branch,
                exactName: settings.branchNamingMode === "custom",
              })),
            );
        });

      // The server owns worktree naming: without an explicit branch, provision
      // under a temporary `t3/<hash>` name so the worktree never waits on
      // name generation, then rename in the background below.
      const requestedBranch = input.workspaceStrategy.branch;
      let branch: string | null;
      if (input.workspaceStrategy.type === "worktree" && requestedBranch === undefined) {
        const uuid = yield* randomUuidV4;
        branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
      } else {
        branch = requestedBranch ?? null;
      }
      let worktreePath =
        input.workspaceStrategy.type === "existing_worktree"
          ? input.workspaceStrategy.worktreePath
          : null;
      if (input.workspaceStrategy.type === "worktree") {
        if (runId !== null) {
          yield* threads
            .dispatch(trustedMutationAuthority, {
              type: "prepared-run.progress",
              commandId: CommandId.make(`${input.commandId}:progress:worktree`),
              threadId,
              runId,
              phase: "worktree",
            })
            .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
        }
        let startRef = input.workspaceStrategy.baseRef;
        // "Start from origin" is a stored default; repos without the requested
        // remote branch fall back to the local base branch.
        const startFromOrigin =
          input.workspaceStrategy.startFromOrigin === true &&
          (yield* git
            .remoteExists({ cwd: project.workspaceRoot, remoteName: "origin" })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))));
        yield* setupTracker.stageStatus(threadId, "fetch", startFromOrigin ? "running" : "skipped");
        if (startFromOrigin) {
          yield* git
            .fetchRemote({
              cwd: project.workspaceRoot,
              remoteName: "origin",
              refName: input.workspaceStrategy.baseRef,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          const remoteBaseExists = yield* git
            .remoteBranchExists({
              cwd: project.workspaceRoot,
              refName: input.workspaceStrategy.baseRef,
              remoteName: "origin",
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          if (remoteBaseExists) {
            startRef = yield* git
              .resolveRemoteTrackingCommit({
                cwd: project.workspaceRoot,
                refName: input.workspaceStrategy.baseRef,
                fallbackRemoteName: "origin",
              })
              .pipe(
                Effect.map((resolved) => resolved.commitSha),
                Effect.mapError(mapError(input, "provision-worktree", threadId)),
              );
          }
        }
        if (startFromOrigin) yield* setupTracker.stageStatus(threadId, "fetch", "done");
        if (
          branch !== null &&
          isTemporaryWorktreeBranch(branch) &&
          (yield* git
            .hasCommit({
              cwd: project.workspaceRoot,
              refName: `refs/heads/${WORKTREE_BRANCH_PREFIX}`,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))))
        ) {
          branch = flattenTemporaryWorktreeBranchName(branch);
        }
        yield* setupTracker.stageStatus(threadId, "checkout", "running");
        const worktree = yield* git
          .createWorktree(
            {
              cwd: project.workspaceRoot,
              refName: startRef,
              newRefName: branch!,
              baseRefName: input.workspaceStrategy.baseRef,
              path: null,
            },
            {
              progress: {
                onWorktreeClaimed: (path) =>
                  Effect.sync(() => {
                    createdWorktreePath = path;
                  }),
                onCheckoutProgress: (progress) =>
                  setupTracker.stage(threadId, "checkout", { percent: progress.percent }),
              },
            },
          )
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        worktreePath = worktree.worktree.path;
        branch = worktree.worktree.refName;
        createdWorktreePath = worktreePath;
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, worktreePath, branch }));
        yield* setupTracker.stageStatus(threadId, "checkout", "done");
      }

      // A reused worktree is already recorded, and rewriting it could undo
      // the first attempt's branch rename.
      if (reused === undefined) {
        yield* threads
          .dispatch(trustedMutationAuthority, {
            type: "thread.metadata.update",
            commandId: CommandId.make(`${input.commandId}:workspace`),
            threadId,
            branch,
            worktreePath,
          })
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      workspaceRecorded = true;

      // Rename temporary branches (server-invented above, or sent by clients
      // that name worktrees themselves) in the background so generation latency
      // never delays provisioning or the provider turn. The temporary name
      // simply sticks if generation or the rename fails.
      if (
        reused === undefined &&
        worktreePath !== null &&
        branch !== null &&
        initialMessage !== undefined &&
        isTemporaryWorktreeBranch(branch)
      ) {
        const oldBranch = branch;
        const worktreeCwd = worktreePath;
        yield* generateBranchNameFor(worktreeCwd, initialMessage).pipe(
          Effect.flatMap(({ branch: newBranch, exactName }) =>
            git.renameBranch({
              cwd: worktreeCwd,
              oldBranch,
              newBranch,
              ...(exactName ? { exactName: true } : {}),
            }),
          ),
          Effect.flatMap((renamed) =>
            threads.dispatch(trustedMutationAuthority, {
              type: "thread.metadata.update",
              commandId: CommandId.make(`${input.commandId}:branch-rename`),
              threadId,
              branch: renamed.branch,
              worktreePath: worktreeCwd,
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("Thread worktree branch rename failed", {
              commandId: input.commandId,
              threadId,
              oldBranch,
              cause,
            }),
          ),
          Effect.forkIn(preparationScope),
        );
      }

      const cwd = worktreePath ?? project.workspaceRoot;
      if (runId !== null) {
        yield* threads
          .dispatch(trustedMutationAuthority, {
            type: "prepared-run.progress",
            commandId: CommandId.make(`${input.commandId}:progress:setup`),
            threadId,
            runId,
            phase: "setup",
          })
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      yield* setupTracker.stageStatus(threadId, "setup-script", "running");
      const setup = yield* setupScripts
        .runForThread({
          threadId,
          projectId: input.projectId,
          projectCwd: project.workspaceRoot,
          worktreePath: cwd,
          ...(tracked
            ? {
                observeCompletion: {
                  onOutputLine: (line: string) =>
                    setupTracker.appendTail(threadId, "setup-script", line),
                },
              }
            : {}),
          project: {
            id: project.id,
            workspaceRoot: project.workspaceRoot,
            scripts: project.scripts,
          },
        })
        .pipe(Effect.mapError(mapError(input, "run-setup-script", threadId)));

      let awaitAsyncSetup = Effect.void;
      if (setup.status === "started") {
        setupTerminalId = setup.terminalId;
        yield* setupTracker.update(threadId, (snapshot) => ({
          ...snapshot,
          setupScript: {
            name: setup.scriptName,
            command: setup.scriptCommand,
            terminalId: setup.terminalId,
          },
        }));
        if (setup.completion) {
          const awaitCompletion = Effect.gen(function* () {
            const completion = yield* setup.completion!;
            yield* setupTracker.stage(threadId, "setup-script", {
              status: completion.exitCode === 0 ? "done" : "failed",
              detail: `exited with ${completion.exitCode ?? "no exit code"}`,
            });
            if (completion.exitCode !== 0 && !setup.async)
              return yield* mapError(
                input,
                "run-setup-script",
                threadId,
              )(`Setup script exited with ${completion.exitCode ?? "no exit code"}.`);
          });
          if (setup.async) {
            awaitAsyncSetup = awaitCompletion.pipe(
              Effect.catchCause((cause) =>
                setupTracker.stage(threadId, "setup-script", {
                  status: "failed",
                  detail: failureDetail(Cause.squash(cause)),
                }),
              ),
            );
          } else {
            yield* awaitCompletion;
          }
        } else {
          yield* setupTracker.stageStatus(threadId, "setup-script", "done");
        }
      } else {
        yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
      }
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (runId !== null) {
        yield* threads
          .dispatch(trustedMutationAuthority, {
            type: "prepared-run.release",
            commandId: CommandId.make(`${input.commandId}:release`),
            threadId,
            runId,
          })
          .pipe(Effect.mapError(mapError(input, "release-run", threadId)));
      }
      yield* setupTracker.stageStatus(threadId, "agent", "done");
      yield* awaitAsyncSetup;
      yield* setupTracker.finish(threadId, "done");
    }).pipe(
      Effect.onError((cause) =>
        Effect.gen(function* () {
          const cancelled = Cause.hasInterruptsOnly(cause);
          yield* setupTracker.finish(
            threadId,
            cancelled ? "cancelled" : "failed",
            cancelled ? null : failureDetail(Cause.squash(cause)),
          );
          // A cancelled setup leaves nothing behind. A failed one keeps a worktree
          // the thread recorded, so a retry reuses it, and removes one it never
          // recorded, which a retry would otherwise duplicate.
          if (tracked && createdWorktreePath && (cancelled || !workspaceRecorded)) {
            if (setupTerminalId)
              yield* terminals
                .close({ threadId, terminalId: setupTerminalId, deleteHistory: true })
                .pipe(Effect.ignore);
            const removedPath = createdWorktreePath;
            // The thread forgets the worktree only once it is gone; a failed
            // removal leaves the directory for the user to clean up rather than
            // reusing a checkout that may be half written.
            yield* git
              .removeWorktree({ cwd: project.workspaceRoot, path: removedPath, force: true })
              .pipe(
                Effect.andThen(
                  threads
                    .dispatch(trustedMutationAuthority, {
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:cancel-workspace`),
                      threadId,
                      worktreePath: null,
                      branch: null,
                    })
                    .pipe(Effect.ignore),
                ),
                Effect.catchCause((removeCause) =>
                  Effect.logWarning("Failed to remove an abandoned thread worktree", {
                    commandId: input.commandId,
                    threadId,
                    path: removedPath,
                    cause: removeCause,
                  }),
                ),
              );
          }
        }),
      ),
    );
  });

  const failPreparedRun = (
    input: Pick<PreparationInput, "commandId">,
    threadId: ThreadId,
    runId: RunId | null,
    cause: unknown,
  ) =>
    runId === null
      ? Effect.logWarning("Thread workspace preparation failed", {
          commandId: input.commandId,
          threadId,
          cause,
        })
      : threads
          .dispatch(trustedMutationAuthority, {
            type: "prepared-run.fail",
            commandId: CommandId.make(`${input.commandId}:fail`),
            threadId,
            runId,
            failure: makeProviderFailure({
              cause,
              message: failureDetail(cause),
              class: "validation_error",
              retryable: false,
            }),
          })
          .pipe(
            Effect.catchCause((persistCause) =>
              Effect.logWarning("Failed to persist thread workspace preparation failure", {
                commandId: input.commandId,
                threadId,
                cause,
                persistCause,
              }),
            ),
          );

  const reservePreparation = (commandId: CommandId) =>
    Ref.modify(scheduledLaunches, (scheduled) => {
      if (scheduled.has(commandId)) return [false, scheduled] as const;
      const next = new Set(scheduled);
      next.add(commandId);
      return [true, next] as const;
    });

  const releasePreparation = (commandId: CommandId) =>
    Ref.update(scheduledLaunches, (scheduled) => {
      const next = new Set(scheduled);
      next.delete(commandId);
      return next;
    });

  const schedulePreparation = Effect.fn("ThreadLaunchService.schedulePreparation")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    yield* prepareInBackground(input, threadId, runId).pipe(
      Effect.onError((cause) =>
        failPreparedRun(
          input,
          threadId,
          runId,
          Cause.hasInterruptsOnly(cause) ? "Worktree setup cancelled." : Cause.squash(cause),
        ),
      ),
      Effect.ignoreCause,
      Effect.ensuring(releasePreparation(input.commandId)),
      Effect.forkIn(preparationScope),
    );
  });

  const launch: ThreadLaunchService["Service"]["launch"] = Effect.fn("ThreadLaunchService.launch")(
    function* (authority, input) {
      const admitted = yield* admitLaunch(authority, input).pipe(
        Effect.mapError(mapError(input, "read-receipt")),
      );
      yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
        type: "thread.create",
        projectId: input.projectId,
      }).pipe(Effect.mapError(mapError(input, "resolve-project")));
      const project = yield* projects.getById(input.projectId).pipe(
        Effect.mapError(mapError(input, "resolve-project")),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(mapError(input, "resolve-project")("Project not found.")),
            onSome: Effect.succeed,
          }),
        ),
      );
      if (input.reuseExistingThread === true && input.threadId === undefined) {
        return yield* mapError(
          input,
          "update-thread",
        )("Reusing an existing thread requires a thread id.");
      }

      const launchReceipt = yield* readReceipt(authority, input, input.commandId);
      return yield* Effect.gen(function* () {
        // A retried launch has no client-supplied id to replay against, so
        // recover the thread id its accepted create was recorded under before
        // allocating another one; a fresh id would only collide with the
        // recorded receipt.
        const reusableLaunchReceipt =
          input.threadId === undefined &&
          Option.isSome(launchReceipt) &&
          launchReceipt.value.status === "accepted" &&
          launchReceipt.value.commandType === "thread.create"
            ? launchReceipt.value
            : undefined;
        const candidateThreadId = admitted.threadId;

        yield* admitLaunch(authority, admitted).pipe(
          Effect.mapError(mapError(input, "update-thread", candidateThreadId)),
        );
        if (reusableLaunchReceipt !== undefined) {
          const shell = yield* threads
            .getThreadShell(candidateThreadId)
            .pipe(Effect.mapError(mapError(input, "create-thread", candidateThreadId)));
          if (shell === null) {
            return yield* mapError(input, "create-thread", candidateThreadId)("Thread not found.");
          }
          if (shell.projectId !== input.projectId) {
            return yield* mapError(
              input,
              "resolve-project",
              candidateThreadId,
            )("Project identity changed.");
          }
        }

        if (input.reuseExistingThread === true && Option.isNone(launchReceipt)) {
          yield* validateReusableThread(input, candidateThreadId);
        }

        // A Scratch thread launched at the project root runs in a folder of its
        // own. Only the first attempt claims one; a retry replays its create.
        const workspaceStrategy: ThreadLaunchWorkspaceStrategy =
          input.workspaceStrategy.type === "root" && Option.isNone(launchReceipt)
            ? Option.match(
                yield* Effect.gen(function* () {
                  yield* admitLaunch(authority, admitted);
                  return yield* managedFolders.folderForThread({
                    projectId: input.projectId,
                    threadId: candidateThreadId,
                    text: input.initialMessage?.text ?? input.title,
                  });
                }).pipe(Effect.mapError(mapError(input, "provision-worktree", candidateThreadId))),
                {
                  onNone: () => input.workspaceStrategy,
                  onSome: (worktreePath) => ({ type: "existing_worktree", worktreePath }),
                },
              )
            : input.workspaceStrategy;
        const initialBranch = workspaceStrategy.branch ?? null;
        const initialWorktreePath =
          workspaceStrategy.type === "existing_worktree" ? workspaceStrategy.worktreePath : null;
        const claimAuthority =
          initialWorktreePath === null
            ? authority
            : yield* admitRegisteredCommandWorkspace(
                sql,
                authority,
                {
                  commandId: input.commandId,
                  projectId: input.projectId,
                  threadId: candidateThreadId,
                  allowCreate: input.reuseExistingThread !== true,
                },
                initialWorktreePath,
              ).pipe(Effect.mapError(mapError(input, "create-thread", candidateThreadId)));
        const claimDispatch =
          input.reuseExistingThread === true
            ? threads.dispatch(authority, {
                type: "thread.metadata.update",
                commandId: input.commandId,
                threadId: candidateThreadId,
                expectedEmpty: true,
              })
            : threads.dispatch(claimAuthority, {
                ...launchCreateCommand(input, candidateThreadId),
                branch: initialBranch,
                worktreePath: initialWorktreePath,
              });
        const claimed = yield* claimDispatch.pipe(
          Effect.mapError(
            mapError(
              input,
              input.reuseExistingThread === true ? "update-thread" : "create-thread",
              candidateThreadId,
            ),
          ),
        );
        const threadId =
          claimed.storedEvents.find((stored) => stored.event.type.startsWith("thread."))?.event
            .threadId ?? candidateThreadId;
        if (project.id !== input.projectId) {
          return yield* mapError(input, "resolve-project", threadId)("Project identity changed.");
        }

        let runId: RunId | null = null;
        let messageWasAlreadyAccepted = false;
        if (input.initialMessage !== undefined) {
          const messageCommandId = CommandId.make(`${input.commandId}:initial-message`);
          const messageReceipt = yield* readReceipt(authority, input, messageCommandId);
          messageWasAlreadyAccepted = Option.isSome(messageReceipt);
          const message = admitted.initialMessage!;
          const dispatched = yield* threads
            .dispatch(
              authority,
              initialMessageCommand(
                input,
                threadId,
                {
                  ...message,
                  messageId: message.messageId!,
                },
                workspaceStrategy,
              ),
            )
            .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId)));
          const runCreated = dispatched.storedEvents.find(
            (stored) => stored.event.type === "run.created",
          );
          runId = runCreated?.event.type === "run.created" ? runCreated.event.payload.id : null;
          if (runId === null) {
            return yield* mapError(
              input,
              "dispatch-message",
              threadId,
            )("Initial message was accepted without a durable run.");
          }
        }

        yield* admitLaunch(authority, { ...admitted, threadId }).pipe(
          Effect.mapError(mapError(input, "create-thread", threadId)),
        );
        const projection = yield* threads
          .getThreadProjection(trustedReadAuthority, threadId)
          .pipe(Effect.mapError(mapError(input, "create-thread", threadId)));
        const runIsPreparing =
          runId !== null &&
          projection.runs.some((run) => run.id === runId && run.status === "preparing");
        const shouldSchedule = runId === null ? Option.isNone(launchReceipt) : runIsPreparing;
        // A retried root launch prepares the folder its first attempt bound, so
        // a Scratch thread keeps its own. Other root launches bind no folder.
        const boundWorktreePath = projection.thread.worktreePath;
        const preparationStrategy: ThreadLaunchWorkspaceStrategy =
          Option.isSome(launchReceipt) &&
          workspaceStrategy.type === "root" &&
          boundWorktreePath !== null
            ? {
                type: "existing_worktree",
                worktreePath: boundWorktreePath,
                branch: workspaceStrategy.branch,
              }
            : workspaceStrategy;
        if (shouldSchedule) {
          yield* admitLaunch(authority, {
            ...admitted,
            threadId,
            workspaceStrategy: preparationStrategy,
          }).pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
          const ownsPreparation = yield* reservePreparation(input.commandId);
          if (ownsPreparation) {
            yield* Effect.gen(function* () {
              const preparationStillRequired =
                runId === null
                  ? true
                  : yield* threads
                      .getThreadRecords(trustedReadAuthority, threadId, ["runs"], {
                        runIds: [runId],
                      })
                      .pipe(
                        Effect.map((current) =>
                          current.runs.some(
                            (run) => run.id === runId && run.status === "preparing",
                          ),
                        ),
                        Effect.mapError(mapError(input, "update-thread", threadId)),
                      );
              if (preparationStillRequired) {
                yield* admitLaunch(authority, {
                  ...admitted,
                  threadId,
                  workspaceStrategy: preparationStrategy,
                }).pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
                yield* schedulePreparation(
                  { ...input, workspaceStrategy: preparationStrategy },
                  threadId,
                  runId,
                );
              } else {
                yield* releasePreparation(input.commandId);
              }
            }).pipe(Effect.onError(() => releasePreparation(input.commandId)));
          }
        }

        return {
          threadId,
          projection,
          resumed: Option.isSome(launchReceipt) || messageWasAlreadyAccepted,
        };
      });
    },
  );

  const admitRetry = (authority: MutationAuthority, input: ThreadLaunchRetryInput) =>
    Effect.gen(function* () {
      yield* authorizeThreadMutation(sql, authority, input.threadId, input.runId);
      yield* authorizeCommandReceipt(sql, authority, input.commandId);
      if (authority === trustedMutationAuthority) return;
      const stored = yield* sql<{ readonly project: string; readonly workspace: string | null }>`
        SELECT t.project_id AS project, json_extract(r.payload_json, '$.workspacePreparation.worktreePath') AS workspace
        FROM orchestration_v2_projection_runs r JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
        WHERE r.run_id = ${input.runId} AND r.thread_id = ${input.threadId}
      `;
      for (const row of stored)
        if (row.workspace !== null)
          yield* requireRegisteredMutationPath(
            sql,
            authority,
            {
              projectId: ProjectId.make(row.project),
              threadId: input.threadId,
            },
            row.workspace,
          );
    }).pipe(
      Effect.mapError(
        (cause) =>
          new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
      ),
    );

  const retryPreparation: ThreadLaunchService["Service"]["retryPreparation"] = Effect.fn(
    "ThreadLaunchService.retryPreparation",
  )(function* (authority, input) {
    const admit = admitRetry(authority, input);
    yield* admit;
    const dispatched = yield* threads.dispatch(authority, {
      type: "prepared-run.retry",
      commandId: input.commandId,
      threadId: input.threadId,
      runId: input.runId,
    });
    // A replayed retry finds the run already past preparation, or prepared by
    // the attempt that first reserved this command.
    // From here the run is preparing again; anything that stops preparation
    // from being scheduled must fail it, or it would wait in preparing forever.
    const scheduled = yield* Effect.gen(function* () {
      yield* admit;
      const projection = yield* threads.getThreadProjection(trustedReadAuthority, input.threadId);
      const run = projection.runs.find((candidate) => candidate.id === input.runId);
      const workspacePreparation = run?.workspacePreparation;
      if (run?.status !== "preparing" || workspacePreparation === undefined) return;
      yield* admit;
      if (!(yield* reservePreparation(input.commandId))) return;
      yield* admit.pipe(
        Effect.andThen(
          scheduleRetriedPreparation(authority, input, projection, run, workspacePreparation),
        ),
        Effect.onError(() => releasePreparation(input.commandId)),
      );
    }).pipe(Effect.exit);
    if (Exit.isFailure(scheduled)) {
      const failure = Cause.squash(scheduled.cause);
      if (isProjectionError(failure) && isAuthorizationError(failure.cause)) return yield* failure;
      yield* failPreparedRun(input, input.threadId, input.runId, Cause.squash(scheduled.cause));
    }
    return dispatched;
  });

  const scheduleRetriedPreparation = Effect.fn("ThreadLaunchService.scheduleRetriedPreparation")(
    function* (
      authority: MutationAuthority,
      input: ThreadLaunchRetryInput,
      projection: OrchestrationV2ThreadProjection,
      run: OrchestrationV2ThreadProjection["runs"][number],
      workspacePreparation: ThreadLaunchWorkspaceStrategy,
    ) {
      const message = projection.messages.find((candidate) => candidate.id === run.userMessageId);
      // A worktree the failed attempt already created is reused, not created again.
      const reuse =
        workspacePreparation.type === "worktree" &&
        projection.thread.worktreePath !== null &&
        projection.thread.branch !== null
          ? {
              strategy: {
                type: "existing_worktree" as const,
                worktreePath: projection.thread.worktreePath,
                branch: projection.thread.branch,
              },
              reusedWorktree: { baseRef: workspacePreparation.baseRef },
            }
          : null;
      const workspaceStrategy = reuse?.strategy ?? workspacePreparation;
      if (workspaceStrategy.type === "existing_worktree")
        yield* requireRegisteredMutationPath(
          sql,
          authority,
          {
            projectId: projection.thread.projectId,
            threadId: input.threadId,
          },
          workspaceStrategy.worktreePath,
        ).pipe(
          Effect.mapError(
            (cause) =>
              new Orchestrator.OrchestratorProjectionError({ threadId: input.threadId, cause }),
          ),
        );
      yield* admitRetry(authority, input);
      return yield* schedulePreparation(
        {
          commandId: input.commandId,
          projectId: projection.thread.projectId,
          workspaceStrategy,
          ...(reuse === null ? {} : { reusedWorktree: reuse.reusedWorktree }),
          ...(message === undefined
            ? {}
            : {
                initialMessage: {
                  text: message.text,
                  attachments: message.attachments,
                  ...(message.context ? { context: message.context } : {}),
                },
              }),
        },
        input.threadId,
        run.id,
      );
    },
  );

  return ThreadLaunchService.of({
    admit: (authority, input) =>
      admitLaunch(authority, input).pipe(Effect.mapError(mapError(input, "read-receipt"))),
    launch,
    retryPreparation,
  });
});

export const layer = Layer.effect(ThreadLaunchService, make);
