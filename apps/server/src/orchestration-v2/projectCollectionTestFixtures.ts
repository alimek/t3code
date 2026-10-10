import {
  type ApplicationStoredEvent,
  type AuthResourceGrant,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Result from "effect/Result";

import * as OrchestrationEventStore from "../persistence/OrchestrationEventStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProjectEnrichment from "../project/ProjectEnrichmentService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as LegacyImporter from "./legacy/LegacyV1ThreadImporter.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import * as ThreadCommands from "./ThreadCommandExecutor.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export const collectionStoreLayer = Layer.mergeAll(
  ProjectionStore.layer,
  ProjectStore.layer,
  OrchestrationEventStore.layer,
).pipe(Layer.provideMerge(Sqlite.layerMemory));

export const projectA = ProjectId.make("project:collection-a");
export const projectB = ProjectId.make("project:collection-b-secret");
export const grantA: AuthResourceGrant = {
  version: 1,
  projects: { mode: "explicit", ids: [projectA] },
  providers: { mode: "all" },
};
export const grantNone: AuthResourceGrant = { ...grantA, projects: { mode: "explicit", ids: [] } };
export const provider = ProviderInstanceId.make("fixture-provider");
export const now = DateTime.makeUnsafe("2026-10-11T00:00:00Z");
export const activeA = ThreadId.make("thread:collection-a");
export const activeB = ThreadId.make("thread:collection-b-secret");
export const archiveA = ThreadId.make("thread:collection-a-archive");
export const archiveB = ThreadId.make("thread:collection-b-secret-archive");

export function threadCreated(
  id: ThreadId,
  projectId: ProjectId,
  archivedAt: DateTime.Utc | null = null,
): OrchestrationV2DomainEvent {
  return {
    id: EventId.make(`create:${id}`),
    type: "thread.created",
    threadId: id,
    occurredAt: now,
    payload: {
      id,
      projectId,
      title: String(id),
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: provider,
      modelSelection: { instanceId: provider, model: "fixture" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  };
}

/** Real SQL/event codecs, fake metadata and unused command services; no provider runs. */
export const makeCollectionFixture = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const events = yield* OrchestrationEventStore.OrchestrationEventStore;
  const changes = yield* PubSub.unbounded<ProjectEnrichment.ProjectEnrichmentChange>();
  const enrichedRoots: Array<string> = [];
  const shellReads: Array<ThreadId> = [];
  const enrichment: ProjectEnrichment.ProjectEnrichment = {
    repositoryIdentity: null,
    faviconPath: null,
    repositoryIdentityResolved: true,
  };
  const metadata = ProjectEnrichment.ProjectEnrichmentService.of({
    getAvailable: (root) =>
      Effect.sync(() => {
        enrichedRoots.push(root);
        return enrichment;
      }),
    peek: () => Effect.succeed(enrichment),
    request: () => Effect.void,
    invalidate: () => Effect.void,
    subscribeChanges: PubSub.subscribe(changes),
  });
  const projectService = yield* ProjectService.make.pipe(
    Effect.provideService(ProjectEnrichment.ProjectEnrichmentService, metadata),
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(WorkspacePaths.WorkspacePaths)({}),
        Layer.mock(EventSink.EventSinkV2)({}),
        IdAllocator.layer,
        Layer.mock(LegacyImporter.LegacyV1ThreadImporter)({}),
        Layer.mock(ThreadCommands.ThreadCommandExecutor)({}),
      ),
    ),
  );
  const appendThread = Effect.fnUntraced(function* (event: OrchestrationV2DomainEvent) {
    const stored = yield* events.appendAgentEvents({ events: [event] });
    yield* projections.apply(event);
    return stored[0]!;
  });
  const appendProject = Effect.fnUntraced(function* (projectId: ProjectId) {
    const iso = DateTime.formatIso(now);
    const stored = yield* events.appendProjectEvent({
      eventId: EventId.make(`create:${projectId}`),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: iso,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: String(projectId),
        workspaceRoot: `/work/${projectId}`,
        defaultModelSelection: null,
        scripts: [],
        createdAt: iso,
        updatedAt: iso,
      },
    });
    yield* projects.apply(stored);
    return stored;
  });
  const updateThread = Effect.fnUntraced(function* (
    type: "thread.metadata-updated" | "thread.deleted" | "thread.archived" | "thread.unarchived",
    id: ThreadId,
    patch: Partial<OrchestrationV2AppThread>,
  ) {
    return yield* appendThread({
      id: EventId.make(`${type}:${id}`),
      type,
      threadId: id,
      occurredAt: now,
      payload: { ...(yield* projections.getThread(id)), ...patch, updatedAt: now },
    });
  });
  for (const projectId of [projectA, projectB]) yield* appendProject(projectId);
  for (const event of [
    threadCreated(activeA, projectA),
    threadCreated(archiveA, projectA, now),
    threadCreated(activeB, projectB),
    threadCreated(archiveB, projectB, now),
  ])
    yield* appendThread(event);

  const services = (live: Stream.Stream<ApplicationStoredEvent, unknown> = Stream.empty) =>
    Layer.mergeAll(
      Layer.succeed(ProjectEnrichment.ProjectEnrichmentService, metadata),
      Layer.succeed(ProjectService.ProjectService, projectService),
      Layer.succeed(OrchestrationEventStore.OrchestrationEventStore, {
        ...events,
        streamProjectedApplicationEvents: ({ project }) =>
          live.pipe(Stream.orDie, Stream.map(project)),
      }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        readShellSnapshot: (options) =>
          projections.readShellSnapshot(options).pipe(Effect.orDie, Effect.map(Effect.orDie)),
        getThreadShell: (id, options) =>
          projections.getThreadShell(id, options).pipe(
            Effect.orDie,
            Effect.tap(() =>
              Effect.sync(() => {
                shellReads.push(id);
              }),
            ),
          ),
        streamStoredEventsFrom: () =>
          live.pipe(
            Stream.orDie,
            Stream.filterMap((event) =>
              "aggregateKind" in event ? Result.failVoid : Result.succeed(event),
            ),
          ),
      }),
    );
  return {
    projections,
    projects,
    events,
    changes,
    enrichment,
    enrichedRoots,
    shellReads,
    appendThread,
    appendProject,
    updateThread,
    services,
    projectService,
  };
});
