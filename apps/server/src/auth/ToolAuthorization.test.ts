import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Sqlite from "../persistence/Sqlite.ts";
import { authorizeCommand, trustedMutationAuthority } from "./CommandAuthorization.ts";
import { describe, expect, it } from "@effect/vitest";
import {
  CommandId,
  ThreadId,
  NodeId,
  type OrchestrationV2ServerCommand,
  AuthAdministrativeScopes,
  AuthAllResourceGrant,
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  AuthResourceGrant,
  ProjectId,
  ProviderInstanceId,
  RpcScopeAuthorization,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as RpcTest from "effect/rpc/RpcTest";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Ref from "effect/Ref";
import * as Headers from "effect/http/Headers";
import type * as RpcMiddleware from "effect/rpc/RpcMiddleware";
import { trustedReadAuthority, type ThreadReadAuthority } from "./ResourceAuthorization.ts";
import { resourceRestricted, authorizeExtraTool } from "./ToolAuthorization.ts";
import * as RpcAuthorization from "./RpcAuthorization.ts";

const isResourceGrant = Schema.is(AuthResourceGrant);

const grants: ReadonlyArray<AuthResourceGrant> = [
  { ...AuthAllResourceGrant, projects: { mode: "explicit", ids: [] } },
  {
    ...AuthAllResourceGrant,
    projects: { mode: "explicit", ids: [ProjectId.make("every-known-project")] },
  },
  { ...AuthAllResourceGrant, providers: { mode: "explicit", instances: [] } },
  {
    ...AuthAllResourceGrant,
    providers: {
      mode: "explicit",
      instances: [{ instanceId: ProviderInstanceId.make("codex"), models: { mode: "all" } }],
    },
  },
];

describe("v1 current extra-tool denial", () => {
  it("distinguishes unrestricted legacy/all from explicit selections and malformed grants", () => {
    for (const grant of [undefined, null, AuthAllResourceGrant])
      expect(resourceRestricted(grant)).toBe(false);
    for (const grant of [
      ...grants,
      {},
      { projects: { mode: "all" }, providers: { mode: "all" } },
      { ...AuthAllResourceGrant, providers: null },
    ])
      expect(resourceRestricted(grant)).toBe(true);
    expect(new Set(Object.keys(RpcAuthorization.RPC_EXTRA_TOOLS))).toEqual(
      new Set(WsRpcGroup.requests.keys()),
    );
  });

  it.effect(
    "denies every protected RPC family, including subscriptions and all eleven PR writes, before handler effects",
    () =>
      Effect.gen(function* () {
        const current = yield* Ref.make<AuthResourceGrant>(AuthAllResourceGrant);
        const authority: ThreadReadAuthority = {
          current: Ref.get(current).pipe(
            Effect.map((resourceGrant) => ({ resourceGrant, scopes: AuthAdministrativeScopes })),
          ),
        };
        const middleware = yield* RpcScopeAuthorization.pipe(
          Effect.provide(RpcAuthorization.layer(AuthAdministrativeScopes, authority)),
        );
        let calls = 0;
        const operation = Effect.sync(() => {
          calls++;
          return {} as RpcMiddleware.SuccessValue;
        });
        for (const grant of [...grants, {} as AuthResourceGrant]) {
          yield* Ref.set(current, grant);
          for (const rpc of WsRpcGroup.requests.values()) {
            if (!RpcAuthorization.RPC_EXTRA_TOOLS[rpc._tag]) continue;
            const error = yield* middleware(operation, {
              rpc: rpc as unknown as Parameters<typeof middleware>[1]["rpc"],
              payload: {},
              headers: Headers.empty,
              client: {} as Parameters<typeof middleware>[1]["client"],
              requestId: 1n as unknown as Parameters<typeof middleware>[1]["requestId"],
            }).pipe(Effect.flip);
            expect(error).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
          }
        }
        expect(calls).toBe(0);
        yield* Ref.set(current, AuthAllResourceGrant);
        const rpc = WsRpcGroup.requests.get(WS_METHODS.scheduledTasksList)!;
        yield* middleware(operation, {
          rpc: rpc as unknown as Parameters<typeof middleware>[1]["rpc"],
          payload: {},
          headers: Headers.empty,
          client: {} as Parameters<typeof middleware>[1]["client"],
          requestId: 1n as unknown as Parameters<typeof middleware>[1]["requestId"],
        });
        expect(calls).toBe(1);
      }).pipe(Effect.scoped),
  );

  it.effect("reloads cuts and keeps action scopes independent, including write-only callers", () =>
    Effect.gen(function* () {
      const grant = yield* Ref.make(AuthAllResourceGrant);
      const authority: ThreadReadAuthority = {
        current: Ref.get(grant).pipe(
          Effect.map((resourceGrant) => ({ resourceGrant, scopes: [AuthSourceControlWriteScope] })),
        ),
      };
      yield* authorizeExtraTool(authority, AuthSourceControlWriteScope);
      expect(
        yield* authorizeExtraTool(authority, AuthOrchestrationReadScope).pipe(Effect.flip),
      ).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
      yield* Ref.set(grant, grants[0]!);
      expect(
        yield* authorizeExtraTool(authority, AuthSourceControlWriteScope).pipe(Effect.flip),
      ).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
      yield* authorizeExtraTool(trustedReadAuthority);
      expect(
        yield* authorizeExtraTool(undefined as unknown as ThreadReadAuthority).pipe(Effect.flip),
      ).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
      expect(isResourceGrant(grants[0])).toBe(true);
    }),
  );
});

it.effect("v1 WS protocol denies guessed schedule IDs before its real handler", () =>
  Effect.gen(function* () {
    const group = WsRpcGroup.omit(
      ...[...WsRpcGroup.requests.keys()].filter(
        (
          tag,
        ): tag is Exclude<
          keyof typeof RpcAuthorization.RPC_EXTRA_TOOLS,
          typeof WS_METHODS.scheduledTasksDelete
        > => tag !== WS_METHODS.scheduledTasksDelete,
      ),
    );
    let effects = 0;
    const authority = {
      current: Effect.succeed({ resourceGrant: grants[0]!, scopes: AuthAdministrativeScopes }),
    };
    const client = yield* RpcTest.makeClient(group).pipe(
      Effect.provide(
        Layer.mergeAll(
          group.toLayerHandler(WS_METHODS.scheduledTasksDelete, () =>
            Effect.sync(() => {
              effects++;
              return { id: "guessed" as import("@t3tools/contracts").ScheduledTaskId };
            }),
          ),
          RpcAuthorization.layer(AuthAdministrativeScopes, authority),
        ),
      ),
    );
    expect(
      yield* client[WS_METHODS.scheduledTasksDelete]({
        id: "guessed" as import("@t3tools/contracts").ScheduledTaskId,
      }).pipe(Effect.flip),
    ).toMatchObject({ _tag: "EnvironmentAuthorizationError" });
    expect(effects).toBe(0);
  }).pipe(Effect.scoped),
);

it.effect("v1 raw delegation and PR commands deny before SQL ownership or receipt effects", () =>
  Effect.gen(function* () {
    const original = yield* SqlClient.SqlClient;
    let queries = 0;
    const sql = new Proxy(original, {
      apply: (target, context, args) => {
        queries++;
        return Reflect.apply(target, context, args);
      },
    });
    const commandId = CommandId.make("guessed-command");
    const threadId = ThreadId.make("guessed-thread");
    const commands: ReadonlyArray<OrchestrationV2ServerCommand> = [
      {
        type: "delegated_task.wake-policy",
        commandId,
        parentThreadId: threadId,
        taskId: NodeId.make("guessed-task"),
        completionWake: "always",
      },
      {
        type: "thread.pull-request.watch",
        commandId,
        threadId,
        host: "github.com",
        repository: "acme/a",
        number: 1,
        watching: true,
      },
    ];
    for (const resourceGrant of grants) {
      const authority = {
        current: Effect.succeed({ resourceGrant, scopes: AuthAdministrativeScopes }),
      };
      for (const command of commands)
        expect(yield* authorizeCommand(sql, authority, command).pipe(Effect.flip)).toMatchObject({
          _tag: "EnvironmentAuthorizationError",
        });
    }
    for (const command of commands) yield* authorizeCommand(sql, trustedMutationAuthority, command);
    expect(queries).toBe(0);
  }).pipe(Effect.provide(Sqlite.layerMemory.pipe(Layer.provideMerge(NodeServices.layer)))),
);
