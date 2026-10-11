import { type WorkspaceActionAdmission } from "../auth/workspaceReadAccess.ts";
import { checkedSourceControlLinkAuthority } from "../auth/CommandAuthorization.ts";
import { sourceControlRepositorySelector } from "@t3tools/shared/sourceControl";
import {
  EnvironmentAuthorizationError,
  type CommandId,
  pullRequestHostOf,
  type GitRunStackedActionResult,
  type OrchestrationProjectShell,
  type SourceControlProviderKind,
  type ThreadId,
} from "@t3tools/contracts";
import { parseChangeRequestUrl } from "@t3tools/shared/changeRequestUrl";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";

import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProjectService from "../project/ProjectService.ts";

const isEnvironmentAuthorizationError = Schema.is(EnvironmentAuthorizationError);

export interface CreatedPullRequestKey {
  readonly host: string;
  readonly repository: string;
  readonly number: number;
  readonly url: string;
}

/**
 * The identity a stacked action's pull request should be linked under, or
 * null when the action did not leave one behind. Reading the host and
 * repository from the URL keeps the link host-level even when the checkout's
 * remote differs from where the PR was opened (a fork, say); the project is
 * only consulted when the URL is one this cannot read.
 */
export function createdPullRequestKey(
  result: Pick<GitRunStackedActionResult, "pr">,
  project: OrchestrationProjectShell | undefined,
): CreatedPullRequestKey | null {
  const { status, number, url } = result.pr;
  if ((status !== "created" && status !== "opened_existing") || number === undefined || !url) {
    return null;
  }
  const parsed = parseChangeRequestUrl(url);
  if (parsed !== null) return { ...parsed, url };
  const identity = project?.repositoryIdentity;
  const kind = identity?.provider as SourceControlProviderKind | undefined;
  const repository = sourceControlRepositorySelector(identity);
  if (!identity || kind === undefined || repository === null) return null;
  return {
    host: pullRequestHostOf(identity, kind),
    repository: repository.toLowerCase(),
    number,
    url,
  };
}

/**
 * Links the pull request a `create_pr`-shaped action produced to the thread it
 * ran beside. Ordinary link failures are best-effort; lost authority is retained.
 */
export const linkCreatedPullRequest = <E>(input: {
  readonly admission: WorkspaceActionAdmission;
  readonly threadId: ThreadId;
  readonly result: Pick<GitRunStackedActionResult, "pr">;
  readonly commandId: Effect.Effect<CommandId, E>;
}): Effect.Effect<
  void,
  EnvironmentAuthorizationError,
  Orchestrator.OrchestratorV2 | ProjectService.ProjectService
> =>
  Effect.gen(function* () {
    yield* input.admission.beforeEffect(input.admission.input.cwd);
    if (input.admission.input.threadId !== input.threadId)
      return yield* new EnvironmentAuthorizationError({
        requiredScope: input.admission.scope,
        message: "The Git action cannot link this thread.",
      });
    const engine = yield* Orchestrator.OrchestratorV2;
    const projects = yield* ProjectService.ProjectService;
    const thread = yield* engine
      .getThreadShell(input.threadId)
      .pipe(Effect.map(Option.fromNullishOr));
    if (Option.isNone(thread)) return;
    yield* input.admission.checkSql;
    if (
      input.admission.projectId !== undefined &&
      thread.value.projectId !== input.admission.projectId
    )
      return;
    const project = Option.getOrUndefined(yield* projects.getShell(thread.value.projectId));
    const key = createdPullRequestKey(input.result, project);
    if (key === null) return;
    const commandId = yield* input.commandId;
    const command = {
      type: "thread.pull-request.link" as const,
      commandId,
      threadId: input.threadId,
      ...key,
      source: "created" as const,
    };
    yield* engine
      .dispatch(checkedSourceControlLinkAuthority(input.admission, command), command)
      .pipe(Effect.asVoid);
  }).pipe(
    Effect.withSpan("linkCreatedPullRequest"),
    Effect.catchCause((cause) => {
      const error = Cause.findErrorOption(cause);
      if (Option.isSome(error) && isEnvironmentAuthorizationError(error.value))
        return Effect.fail(error.value);
      return Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(cause as Cause.Cause<never>)
        : Effect.logWarning("failed to link created pull request to thread", {
            threadId: input.threadId,
          });
    }),
  );
