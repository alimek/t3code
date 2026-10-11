import {
  recoverWorkspaceError,
  type WorkspaceActionAdmission,
} from "../auth/workspaceReadAccess.ts";
import {
  EnvironmentAuthorizationError,
  type GitRunStackedActionInput,
  type GitRunStackedActionResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as OrchestratorV2 from "../orchestration-v2/Orchestrator.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";

export const refreshPushedPullRequests = Effect.fn("refreshPushedPullRequests")(
  function* (
    admission: WorkspaceActionAdmission,
    input: Pick<GitRunStackedActionInput, "cwd" | "threadId" | "projectId">,
    result: Pick<GitRunStackedActionResult, "push">,
  ) {
    if (result.push.status !== "pushed") return;
    yield* admission.beforeEffect(input.cwd);
    const pullRequests = yield* PullRequestService.PullRequestService;
    if (input.threadId !== undefined) {
      const engine = yield* OrchestratorV2.OrchestratorV2;
      const thread = yield* engine.getThreadShell(input.threadId);
      yield* admission.beforeEffect(input.cwd);
      if (admission.projectId !== undefined && thread?.projectId !== admission.projectId)
        return yield* new EnvironmentAuthorizationError({
          requiredScope: admission.scope,
          message: "The Git action cannot refresh this thread owner.",
        });
      if (
        thread !== null &&
        (admission.projectId === undefined || admission.projectId === thread.projectId)
      ) {
        yield* admission.beforeEffect(input.cwd);
        yield* pullRequests.refreshAfterTurn(thread.projectId);
        return;
      }
    }
    if (
      input.projectId !== undefined &&
      (admission.projectId === undefined || admission.projectId === input.projectId)
    ) {
      yield* admission.beforeEffect(input.cwd);
      yield* pullRequests.refreshAfterTurn(input.projectId);
      return;
    }
    yield* admission.beforeEffect(input.cwd);
    const projects = yield* (yield* ProjectStore.ProjectStoreV2).listShells();
    yield* Effect.forEach(
      projects.filter(
        (project) =>
          project.workspaceRoot === input.cwd &&
          (admission.projectId === undefined || project.id === admission.projectId),
      ),
      (project) =>
        admission
          .beforeEffect(input.cwd)
          .pipe(Effect.andThen(pullRequests.refreshAfterTurn(project.id))),
      { discard: true },
    );
  },
  recoverWorkspaceError(() => undefined),
  Effect.asVoid,
);
