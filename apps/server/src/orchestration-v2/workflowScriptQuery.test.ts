import { trustedReadAuthority } from "../auth/ResourceAuthorization.ts";
import {
  AuthOrchestrationReadScope,
  AuthAllResourceGrant,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { it as effectIt } from "@effect/vitest";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as Layer from "effect/Layer";
import * as Effect from "effect/Effect";
import { afterAll, assert, describe } from "vite-plus/test";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import { readWorkflowScript } from "./workflowScriptQuery.ts";

const root = NodePath.join(NodeOS.homedir(), ".claude", "projects", "__wf_script_test__");
NodeFS.mkdirSync(root, { recursive: true });
const scriptPath = NodePath.join(root, "run.js");
NodeFS.writeFileSync(scriptPath, "export const meta = {};\n");
const outside = NodePath.join(NodeOS.tmpdir(), "wf-outside.js");
NodeFS.writeFileSync(outside, "evil\n");
const link = NodePath.join(root, "sneaky.js");
// Planted only where the host allows it; the escape test is skipped
// otherwise rather than passing vacuously on "not-found".
if (symlinksSupported) {
  NodeFS.rmSync(link, { force: true });
  NodeFS.symlinkSync(outside, link);
  if (!NodeFS.lstatSync(link).isSymbolicLink()) {
    throw new Error("test setup: sneaky.js must be a symlink");
  }
}

afterAll(() => {
  NodeFS.rmSync(root, { recursive: true, force: true });
  NodeFS.rmSync(outside, { force: true });
});

describe("readWorkflowScript containment", () => {
  effectIt.effect("serves a real script under the projects root", () =>
    Effect.gen(function* () {
      const result = yield* readWorkflowScript(trustedReadAuthority, {
        threadId: ThreadId.make("workflow-fixture"),
        scriptPath,
      });
      assert.include(result.contents, "export const meta");
      assert.equal(result.truncated, false);
    }).pipe(Effect.provide(Layer.mock(ThreadManagement.ThreadManagementService)({}))),
  );

  effectIt.effect("rejects relative and non-js paths", () =>
    Effect.gen(function* () {
      const relative = yield* Effect.exit(
        readWorkflowScript(trustedReadAuthority, {
          threadId: ThreadId.make("workflow-fixture"),
          scriptPath: "run.js",
        }),
      );
      assert.equal(relative._tag, "Failure");
      const nonJs = yield* Effect.exit(
        readWorkflowScript(trustedReadAuthority, {
          threadId: ThreadId.make("workflow-fixture"),
          scriptPath: scriptPath.replace(".js", ".ts"),
        }),
      );
      assert.equal(nonJs._tag, "Failure");
    }).pipe(Effect.provide(Layer.mock(ThreadManagement.ThreadManagementService)({}))),
  );

  effectIt.effect.skipIf(!symlinksSupported)(
    "rejects paths outside the root and symlink escapes",
    () =>
      Effect.gen(function* () {
        const escaped = yield* Effect.exit(
          readWorkflowScript(trustedReadAuthority, {
            threadId: ThreadId.make("workflow-fixture"),
            scriptPath: outside,
          }),
        );
        assert.equal(escaped._tag, "Failure");
        // A symlink INSIDE the root pointing outside must fail specifically on
        // realpath re-containment — a "not-found" would mean the link was
        // never exercised and the assertion proves nothing.
        const sneaky = yield* Effect.exit(
          readWorkflowScript(trustedReadAuthority, {
            threadId: ThreadId.make("workflow-fixture"),
            scriptPath: link,
          }).pipe(
            Effect.flip,
            Effect.map((error) => error.reason),
          ),
        );
        assert.equal(sneaky._tag, "Success");
        if (sneaky._tag === "Success") {
          assert.equal(sneaky.value, "outside-root");
        }
      }).pipe(Effect.provide(Layer.mock(ThreadManagement.ThreadManagementService)({}))),
  );
});

const restrictedAuthority = {
  current: Effect.succeed({
    scopes: [AuthOrchestrationReadScope],
    resourceGrant: {
      ...AuthAllResourceGrant,
      projects: { mode: "explicit" as const, ids: [ProjectId.make("workflow-project")] },
    },
  }),
};

effectIt.effect("restricted script reads require the owning thread's registered workspace", () =>
  Effect.gen(function* () {
    const owned = Layer.mock(ThreadManagement.ThreadManagementService)({
      getReadWorkspaceRoots: () => Effect.succeed([root]),
    });
    const visible = yield* readWorkflowScript(restrictedAuthority, {
      threadId: ThreadId.make("workflow-owner"),
      scriptPath,
    }).pipe(Effect.provide(owned));
    assert.include(visible.contents, "export const meta");
    const unrelated = Layer.mock(ThreadManagement.ThreadManagementService)({
      getReadWorkspaceRoots: () => Effect.succeed([NodeOS.tmpdir()]),
    });
    const error = yield* readWorkflowScript(restrictedAuthority, {
      threadId: ThreadId.make("other-allowed-thread"),
      scriptPath,
    }).pipe(Effect.provide(unrelated), Effect.flip);
    assert.equal(error.reason, "outside-root");
  }),
);
