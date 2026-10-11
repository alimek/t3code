import {
  CLIENT_GUARDED_RPC_SCOPES,
  type DeviceListInput,
  clientRpcRequiredScopes,
  authScopeRequiredResponse,
  AssetCreateUrlInput,
  AuthAccessReadScope,
  ServerSettingsPatch,
  ProviderInstanceMutation,
  requiredScopesForServerSettingsPatch,
  AuthSettingsWriteScope,
  AuthProvidersManageScope,
  AuthEnvironmentMaintainScope,
  AuthFilesystemReadScope,
  AuthFilesystemWriteScope,
  AuthDiagnosticsReadScope,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthPreviewOperateScope,
  AuthRelayReadScope,
  AuthRelayWriteScope,
  AuthTerminalOperateScope,
  ORCHESTRATION_V2_WS_METHODS,
  AuthTerminalReadScope,
  type AuthEnvironmentScope,
  EnvironmentAuthorizationError,
  RpcScopeAuthorization,
  WS_METHODS,
  WsRpcGroup,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Layer from "effect/Layer";
import { authorizeExtraTool } from "./ToolAuthorization.ts";
import type { ThreadReadAuthority } from "./ResourceAuthorization.ts";
import type * as RpcGroup from "effect/rpc/RpcGroup";

type WsRpcMethod = RpcGroup.Rpcs<typeof WsRpcGroup>["_tag"];

/**
 * Keep authorization coverage coupled to the RPC group itself. Adding an RPC to
 * `WsRpcGroup` without choosing a scope is a type error instead of a production
 * runtime failure.
 */
export const RPC_REQUIRED_SCOPES = {
  ...CLIENT_GUARDED_RPC_SCOPES,
  [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: AuthOrchestrationOperateScope,
  [ORCHESTRATION_V2_WS_METHODS.getWorkflowScript]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getTurnDiff]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getFullThreadDiff]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThreads]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThread]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.searchThreadStream]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getArchivedShellSnapshot]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.getTurnItem]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.launchThread]: AuthOrchestrationOperateScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeArchivedShell]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: AuthOrchestrationReadScope,
  [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: AuthOrchestrationReadScope,
  [WS_METHODS.projectsMutate]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverProbe]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetConfig]: AuthOrchestrationReadScope,
  [WS_METHODS.serverRefreshProviders]: AuthOrchestrationReadScope,
  [WS_METHODS.serverUpdateProvider]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthStart]: AuthProvidersManageScope,
  [WS_METHODS.providerConsumeResetCredit]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthComplete]: AuthProvidersManageScope,
  [WS_METHODS.chatGptReconnectProfile]: AuthProvidersManageScope,
  [WS_METHODS.chatGptImportProfile]: AuthProvidersManageScope,
  [WS_METHODS.chatGptHandoffSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.codexAuthCallbackSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthRespond]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthCancel]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthLogout]: AuthProvidersManageScope,
  [WS_METHODS.providerAuthSubscribe]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallStart]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallCancel]: AuthProvidersManageScope,
  [WS_METHODS.providerInstallSubscribe]: AuthOrchestrationReadScope,
  [WS_METHODS.providerInstallRemove]: AuthProvidersManageScope,
  [WS_METHODS.serverUpdateServer]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverUpdateServerWithProgress]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverCommitDesktopUpdate]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverUpsertKeybinding]: AuthSettingsWriteScope,
  [WS_METHODS.serverRemoveKeybinding]: AuthSettingsWriteScope,
  [WS_METHODS.serverGetSettings]: AuthOrchestrationReadScope,
  [WS_METHODS.serverUpdateSettings]: AuthSettingsWriteScope,
  [WS_METHODS.serverSearchAcpRegistry]: AuthOrchestrationReadScope,
  [WS_METHODS.serverPrepareAcpRegistryAgent]: AuthProvidersManageScope,
  [WS_METHODS.serverUninstallAcpRegistryManagedBinary]: AuthProvidersManageScope,
  [WS_METHODS.serverAcceptAcpRegistryUrlAuth]: AuthProvidersManageScope,
  [WS_METHODS.serverListAcpRegistrySessions]: AuthOrchestrationReadScope,
  [WS_METHODS.serverImportAcpRegistrySession]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverDeleteAcpRegistrySession]: AuthOrchestrationOperateScope,
  [WS_METHODS.serverListAcpRegistryProviders]: AuthOrchestrationReadScope,
  [WS_METHODS.serverSetAcpRegistryProvider]: AuthProvidersManageScope,
  [WS_METHODS.serverDisableAcpRegistryProvider]: AuthProvidersManageScope,
  [WS_METHODS.serverLogoutAcpRegistry]: AuthProvidersManageScope,
  [WS_METHODS.serverDiscoverSourceControl]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetTraceDiagnostics]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetProcessDiagnostics]: AuthDiagnosticsReadScope,
  // Load-balancing new threads reads host load; that is part of operating
  // threads, not of inspecting diagnostics.
  [WS_METHODS.serverGetHostResources]: AuthOrchestrationReadScope,
  [WS_METHODS.serverGetProcessResourceHistory]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetResourceTelemetryHistory]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverRetryResourceTelemetry]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverGetUsageSummary]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverRefreshUsageRates]: AuthDiagnosticsReadScope,
  [WS_METHODS.serverSignalProcess]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverReportClientActivity]: AuthOrchestrationReadScope,
  [WS_METHODS.serverReportHostPowerState]: AuthEnvironmentMaintainScope,
  [WS_METHODS.serverGetBackgroundPolicy]: AuthOrchestrationReadScope,
  [WS_METHODS.scheduledTasksList]: AuthOrchestrationReadScope,
  [WS_METHODS.scheduledTasksSubscribe]: AuthOrchestrationReadScope,
  [WS_METHODS.secretsAnswerRequest]: AuthOrchestrationOperateScope,
  // Delivery logs hold request bodies, so they need the same scope as the URL.
  [WS_METHODS.scheduledTasksListWebhookDeliveries]: AuthOrchestrationOperateScope,
  [WS_METHODS.scheduledTasksGetWebhookDelivery]: AuthOrchestrationOperateScope,
  [WS_METHODS.cloudGetRelayClientStatus]: AuthRelayReadScope,
  [WS_METHODS.cloudInstallRelayClient]: AuthRelayWriteScope,
  [WS_METHODS.pullRequestsList]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsListStats]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsSummary]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsRouting]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsRoutingIdentity]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsStack]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsLinkedThreads]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsDetail]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsPreview]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsChecks]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsActivity]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsThreadComments]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsDiffFileContents]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsFilesViewed]: AuthOrchestrationReadScope,
  // Read scope like the reads it un-caches: refreshing is part of reading, and a read-only
  // client pressing refresh must not be told it may not look again.
  [WS_METHODS.pullRequestsInvalidate]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsSubscribeRefreshes]: AuthOrchestrationReadScope,
  // The candidate list is a read like the detail beside it; asking somebody for a review is a
  // write like every other one.
  [WS_METHODS.pullRequestsReviewerCandidates]: AuthOrchestrationReadScope,
  [WS_METHODS.pullRequestsLabelCandidates]: AuthOrchestrationReadScope,
  [WS_METHODS.sourceControlLookupRepository]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeProjectClones]: AuthOrchestrationReadScope,
  [WS_METHODS.projectsListEntries]: AuthFilesystemReadScope,
  [WS_METHODS.projectsReadFile]: AuthFilesystemReadScope,
  [WS_METHODS.projectsSearchContents]: AuthFilesystemReadScope,
  [WS_METHODS.projectsSearchEntries]: AuthFilesystemReadScope,
  [WS_METHODS.projectsWriteFile]: AuthFilesystemWriteScope,
  [WS_METHODS.projectsEnsureScratch]: AuthOrchestrationOperateScope,
  [WS_METHODS.projectsCreateNew]: AuthOrchestrationOperateScope,
  [WS_METHODS.shellOpenInEditor]: AuthOrchestrationOperateScope,
  [WS_METHODS.filesystemBrowse]: AuthFilesystemReadScope,
  [WS_METHODS.agentSessionsScan]: AuthOrchestrationReadScope,
  [WS_METHODS.agentSessionsImport]: AuthOrchestrationOperateScope,
  [WS_METHODS.assetsCreateUrl]: AuthOrchestrationReadScope,
  [WS_METHODS.assetsPersistChatAttachments]: AuthOrchestrationOperateScope,
  [WS_METHODS.attachmentsCreateUploadUrl]: AuthOrchestrationOperateScope,
  [WS_METHODS.attachmentsDelete]: AuthOrchestrationOperateScope,
  [WS_METHODS.providerUploadFeedback]: AuthOrchestrationOperateScope,
  // An app's tool calls can change things on its server, like a user action.
  [WS_METHODS.mcpAppsCallTool]: AuthOrchestrationOperateScope,
  [WS_METHODS.mcpAppsToolInfo]: AuthOrchestrationReadScope,
  [WS_METHODS.mcpAppsReadResource]: AuthOrchestrationReadScope,
  [WS_METHODS.mcpAppsUpdateModelContext]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeVcsStatus]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeWorktreeSetup]: AuthOrchestrationReadScope,
  [WS_METHODS.worktreeSetupCancel]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeResourceTelemetry]: AuthDiagnosticsReadScope,
  [WS_METHODS.vcsRefreshStatus]: AuthOrchestrationReadScope,
  [WS_METHODS.gitResolvePullRequest]: AuthOrchestrationReadScope,
  [WS_METHODS.vcsListRefs]: AuthOrchestrationReadScope,
  [WS_METHODS.reviewGetDiffPreview]: AuthFilesystemReadScope,
  [WS_METHODS.reviewGetDiffFileContents]: AuthFilesystemReadScope,
  [WS_METHODS.terminalOpen]: AuthTerminalOperateScope,
  [WS_METHODS.terminalAttach]: AuthTerminalOperateScope,
  [WS_METHODS.terminalObserve]: AuthTerminalReadScope,
  [WS_METHODS.terminalWrite]: AuthTerminalOperateScope,
  [WS_METHODS.terminalResize]: AuthTerminalOperateScope,
  [WS_METHODS.terminalClear]: AuthTerminalOperateScope,
  [WS_METHODS.terminalRestart]: AuthTerminalOperateScope,
  [WS_METHODS.terminalClose]: AuthTerminalOperateScope,
  [WS_METHODS.subscribeTerminalEvents]: AuthTerminalReadScope,
  [WS_METHODS.subscribeTerminalMetadata]: AuthTerminalReadScope,
  [WS_METHODS.previewOpen]: AuthPreviewOperateScope,
  [WS_METHODS.previewNavigate]: AuthPreviewOperateScope,
  [WS_METHODS.previewResize]: AuthPreviewOperateScope,
  [WS_METHODS.previewAdjust]: AuthPreviewOperateScope,
  [WS_METHODS.previewRefresh]: AuthPreviewOperateScope,
  [WS_METHODS.previewClose]: AuthPreviewOperateScope,
  [WS_METHODS.previewList]: AuthOrchestrationReadScope,
  [WS_METHODS.previewClearProfile]: AuthPreviewOperateScope,
  [WS_METHODS.previewReportStatus]: AuthPreviewOperateScope,
  [WS_METHODS.subscribePreviewEvents]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeDiscoveredLocalServers]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceConfigure]: AuthSettingsWriteScope,
  [WS_METHODS.deviceTestHost]: AuthSettingsWriteScope,
  [WS_METHODS.deviceList]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceOpen]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceClose]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceShutdown]: AuthOrchestrationOperateScope,
  [WS_METHODS.deviceDetail]: AuthOrchestrationReadScope,
  [WS_METHODS.deviceAction]: AuthOrchestrationOperateScope,
  [WS_METHODS.subscribeDeviceState]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeServerConfig]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeServerLifecycle]: AuthOrchestrationReadScope,
  [WS_METHODS.subscribeAuthAccess]: AuthAccessReadScope,
  [WS_METHODS.subscribeBackgroundPolicy]: AuthOrchestrationReadScope,
} as const satisfies Readonly<Record<WsRpcMethod, AuthEnvironmentScope>>;

/** V1 extra-tool denial is independent of action scopes and the issuance gate. */
export const RPC_EXTRA_TOOLS = {
  [WS_METHODS.pullRequestsRunAction]: true,
  [WS_METHODS.pullRequestsUpdate]: true,
  [WS_METHODS.pullRequestsComment]: true,
  [WS_METHODS.pullRequestsUpdateComment]: true,
  [WS_METHODS.pullRequestsSubmitReview]: true,
  [WS_METHODS.pullRequestsReplyToThread]: true,
  [WS_METHODS.pullRequestsSetThreadResolution]: true,
  [WS_METHODS.pullRequestsSetReaction]: true,
  [WS_METHODS.pullRequestsSetFilesViewed]: true,
  [WS_METHODS.pullRequestsRequestReviewers]: true,
  [WS_METHODS.pullRequestsSetLabels]: true,
  [WS_METHODS.sourceControlCloneRepository]: true,
  [WS_METHODS.sourceControlPublishRepository]: true,
  [WS_METHODS.projectCloneStart]: true,
  [WS_METHODS.projectCloneCancel]: true,
  [WS_METHODS.projectCloneRetry]: true,
  [WS_METHODS.vcsPull]: true,
  [WS_METHODS.gitRunStackedAction]: true,
  [WS_METHODS.gitPreparePullRequestThread]: true,
  [WS_METHODS.vcsCreateWorktree]: true,
  [WS_METHODS.vcsRemoveWorktree]: true,
  [WS_METHODS.vcsCreateRef]: true,
  [WS_METHODS.vcsSwitchRef]: true,
  [WS_METHODS.vcsInit]: true,
  [WS_METHODS.scheduledTasksUpsert]: true,
  [WS_METHODS.scheduledTasksSetEnabled]: true,
  [WS_METHODS.scheduledTasksDelete]: true,
  [WS_METHODS.scheduledTasksRunNow]: true,
  [WS_METHODS.scheduledTasksRotateWebhookToken]: true,
  [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: false,
  [ORCHESTRATION_V2_WS_METHODS.getWorkflowScript]: false,
  [ORCHESTRATION_V2_WS_METHODS.getTurnDiff]: false,
  [ORCHESTRATION_V2_WS_METHODS.getFullThreadDiff]: false,
  [ORCHESTRATION_V2_WS_METHODS.searchThreads]: false,
  [ORCHESTRATION_V2_WS_METHODS.searchThread]: false,
  [ORCHESTRATION_V2_WS_METHODS.searchThreadStream]: false,
  [ORCHESTRATION_V2_WS_METHODS.getArchivedShellSnapshot]: false,
  [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: false,
  [ORCHESTRATION_V2_WS_METHODS.getTurnItem]: false,
  [ORCHESTRATION_V2_WS_METHODS.launchThread]: false,
  [ORCHESTRATION_V2_WS_METHODS.subscribeArchivedShell]: false,
  [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: false,
  [ORCHESTRATION_V2_WS_METHODS.subscribeThread]: false,
  [WS_METHODS.projectsMutate]: false,
  [WS_METHODS.serverProbe]: false,
  [WS_METHODS.serverGetConfig]: false,
  [WS_METHODS.serverRefreshProviders]: false,
  [WS_METHODS.serverUpdateProvider]: true,
  [WS_METHODS.providerAuthStart]: true,
  [WS_METHODS.providerConsumeResetCredit]: true,
  [WS_METHODS.providerAuthComplete]: true,
  [WS_METHODS.chatGptReconnectProfile]: true,
  [WS_METHODS.chatGptImportProfile]: true,
  [WS_METHODS.chatGptHandoffSubscribe]: true,
  [WS_METHODS.codexAuthCallbackSubscribe]: true,
  [WS_METHODS.providerAuthRespond]: true,
  [WS_METHODS.providerAuthCancel]: true,
  [WS_METHODS.providerAuthLogout]: true,
  [WS_METHODS.providerAuthSubscribe]: true,
  [WS_METHODS.providerInstallStart]: true,
  [WS_METHODS.providerInstallCancel]: true,
  [WS_METHODS.providerInstallSubscribe]: true,
  [WS_METHODS.providerInstallRemove]: true,
  [WS_METHODS.serverUpdateServer]: true,
  [WS_METHODS.serverUpdateServerWithProgress]: true,
  [WS_METHODS.serverCommitDesktopUpdate]: true,
  [WS_METHODS.serverUpsertKeybinding]: true,
  [WS_METHODS.serverRemoveKeybinding]: true,
  [WS_METHODS.serverGetSettings]: true,
  [WS_METHODS.serverUpdateSettings]: true,
  [WS_METHODS.serverSearchAcpRegistry]: true,
  [WS_METHODS.serverPrepareAcpRegistryAgent]: true,
  [WS_METHODS.serverUninstallAcpRegistryManagedBinary]: true,
  [WS_METHODS.serverAcceptAcpRegistryUrlAuth]: true,
  [WS_METHODS.serverListAcpRegistrySessions]: true,
  [WS_METHODS.serverImportAcpRegistrySession]: true,
  [WS_METHODS.serverDeleteAcpRegistrySession]: true,
  [WS_METHODS.serverListAcpRegistryProviders]: true,
  [WS_METHODS.serverSetAcpRegistryProvider]: true,
  [WS_METHODS.serverDisableAcpRegistryProvider]: true,
  [WS_METHODS.serverLogoutAcpRegistry]: true,
  [WS_METHODS.serverDiscoverSourceControl]: true,
  [WS_METHODS.serverGetTraceDiagnostics]: true,
  [WS_METHODS.serverGetProcessDiagnostics]: true,
  [WS_METHODS.serverGetHostResources]: true,
  [WS_METHODS.serverGetProcessResourceHistory]: true,
  [WS_METHODS.serverGetResourceTelemetryHistory]: true,
  [WS_METHODS.serverRetryResourceTelemetry]: true,
  [WS_METHODS.serverGetUsageSummary]: true,
  [WS_METHODS.serverRefreshUsageRates]: true,
  [WS_METHODS.serverSignalProcess]: true,
  [WS_METHODS.serverReportClientActivity]: false,
  [WS_METHODS.serverReportHostPowerState]: true,
  [WS_METHODS.serverGetBackgroundPolicy]: true,
  [WS_METHODS.scheduledTasksList]: true,
  [WS_METHODS.scheduledTasksSubscribe]: true,
  [WS_METHODS.secretsAnswerRequest]: true,
  [WS_METHODS.scheduledTasksListWebhookDeliveries]: true,
  [WS_METHODS.scheduledTasksGetWebhookDelivery]: true,
  [WS_METHODS.cloudGetRelayClientStatus]: true,
  [WS_METHODS.cloudInstallRelayClient]: true,
  [WS_METHODS.pullRequestsList]: true,
  [WS_METHODS.pullRequestsListStats]: true,
  [WS_METHODS.pullRequestsSummary]: true,
  [WS_METHODS.pullRequestsRouting]: true,
  [WS_METHODS.pullRequestsRoutingIdentity]: true,
  [WS_METHODS.pullRequestsStack]: true,
  [WS_METHODS.pullRequestsLinkedThreads]: true,
  [WS_METHODS.pullRequestsDetail]: true,
  [WS_METHODS.pullRequestsPreview]: true,
  [WS_METHODS.pullRequestsChecks]: true,
  [WS_METHODS.pullRequestsActivity]: true,
  [WS_METHODS.pullRequestsThreadComments]: true,
  [WS_METHODS.pullRequestsDiffFileContents]: true,
  [WS_METHODS.pullRequestsFilesViewed]: true,
  [WS_METHODS.pullRequestsInvalidate]: true,
  [WS_METHODS.pullRequestsSubscribeRefreshes]: true,
  [WS_METHODS.pullRequestsReviewerCandidates]: true,
  [WS_METHODS.pullRequestsLabelCandidates]: true,
  [WS_METHODS.sourceControlLookupRepository]: true,
  [WS_METHODS.subscribeProjectClones]: true,
  [WS_METHODS.projectsListEntries]: false,
  [WS_METHODS.projectsReadFile]: false,
  [WS_METHODS.projectsSearchContents]: false,
  [WS_METHODS.projectsSearchEntries]: false,
  [WS_METHODS.projectsWriteFile]: false,
  [WS_METHODS.projectsEnsureScratch]: true,
  [WS_METHODS.projectsCreateNew]: true,
  [WS_METHODS.shellOpenInEditor]: true,
  [WS_METHODS.filesystemBrowse]: true,
  [WS_METHODS.agentSessionsScan]: true,
  [WS_METHODS.agentSessionsImport]: true,
  [WS_METHODS.assetsCreateUrl]: false,
  [WS_METHODS.assetsPersistChatAttachments]: false,
  [WS_METHODS.attachmentsCreateUploadUrl]: false,
  [WS_METHODS.attachmentsDelete]: false,
  [WS_METHODS.providerUploadFeedback]: true,
  [WS_METHODS.mcpAppsCallTool]: true,
  [WS_METHODS.mcpAppsToolInfo]: true,
  [WS_METHODS.mcpAppsReadResource]: true,
  [WS_METHODS.mcpAppsUpdateModelContext]: true,
  [WS_METHODS.subscribeVcsStatus]: true,
  [WS_METHODS.subscribeWorktreeSetup]: false,
  [WS_METHODS.worktreeSetupCancel]: false,
  [WS_METHODS.subscribeResourceTelemetry]: true,
  [WS_METHODS.vcsRefreshStatus]: true,
  [WS_METHODS.gitResolvePullRequest]: true,
  [WS_METHODS.vcsListRefs]: true,
  [WS_METHODS.reviewGetDiffPreview]: true,
  [WS_METHODS.reviewGetDiffFileContents]: true,
  [WS_METHODS.terminalOpen]: true,
  [WS_METHODS.terminalAttach]: true,
  [WS_METHODS.terminalObserve]: true,
  [WS_METHODS.terminalWrite]: true,
  [WS_METHODS.terminalResize]: true,
  [WS_METHODS.terminalClear]: true,
  [WS_METHODS.terminalRestart]: true,
  [WS_METHODS.terminalClose]: true,
  [WS_METHODS.subscribeTerminalEvents]: true,
  [WS_METHODS.subscribeTerminalMetadata]: true,
  [WS_METHODS.previewOpen]: true,
  [WS_METHODS.previewNavigate]: true,
  [WS_METHODS.previewResize]: true,
  [WS_METHODS.previewAdjust]: true,
  [WS_METHODS.previewRefresh]: true,
  [WS_METHODS.previewClose]: true,
  [WS_METHODS.previewList]: true,
  [WS_METHODS.previewClearProfile]: true,
  [WS_METHODS.previewReportStatus]: true,
  [WS_METHODS.subscribePreviewEvents]: true,
  [WS_METHODS.subscribeDiscoveredLocalServers]: true,
  [WS_METHODS.deviceConfigure]: true,
  [WS_METHODS.deviceTestHost]: true,
  [WS_METHODS.deviceList]: true,
  [WS_METHODS.deviceOpen]: true,
  [WS_METHODS.deviceClose]: true,
  [WS_METHODS.deviceShutdown]: true,
  [WS_METHODS.deviceDetail]: true,
  [WS_METHODS.deviceAction]: true,
  [WS_METHODS.subscribeDeviceState]: true,
  [WS_METHODS.subscribeServerConfig]: false,
  [WS_METHODS.subscribeServerLifecycle]: true,
  [WS_METHODS.subscribeAuthAccess]: true,
  [WS_METHODS.subscribeBackgroundPolicy]: true,
} as const satisfies Readonly<Record<WsRpcMethod, boolean>>;

export function requiredScopeForRpcMethod(method: string): AuthEnvironmentScope {
  if (!Object.hasOwn(RPC_REQUIRED_SCOPES, method)) {
    throw new Error(`RPC method ${method} has no declared authorization scope.`);
  }
  const requiredScope = RPC_REQUIRED_SCOPES[method as WsRpcMethod];
  if (requiredScope === undefined) {
    throw new Error(`RPC method ${method} has no declared authorization scope.`);
  }
  return requiredScope;
}

export const rpcAuthorizationError = (requiredScope: AuthEnvironmentScope) =>
  new EnvironmentAuthorizationError({
    message: `The authenticated token is missing required scope: ${requiredScope}.`,
    ...authScopeRequiredResponse(requiredScope),
  });

const SettingsUpdate = Schema.Struct({
  patch: ServerSettingsPatch,
  providerInstanceMutation: Schema.optionalKey(ProviderInstanceMutation),
});

const requiredScopesForSettingsUpdate = (payload: unknown) => {
  const input = Schema.decodeUnknownSync(SettingsUpdate)(payload);
  const scopes = requiredScopesForServerSettingsPatch(input.patch);
  if (input.providerInstanceMutation === undefined) return scopes;
  // An atomic provider mutation carries an empty patch unless it also changes settings.
  return Object.values(input.patch).every((value) => value === undefined)
    ? [AuthProvidersManageScope]
    : [...new Set([...scopes, AuthProvidersManageScope])];
};

const requiredScopesForRpcCall = (
  method: string,
  payload: unknown,
): ReadonlyArray<AuthEnvironmentScope> => {
  if (method === WS_METHODS.serverRetryResourceTelemetry) {
    return [AuthEnvironmentMaintainScope, AuthDiagnosticsReadScope];
  }
  if (method === WS_METHODS.assetsCreateUrl) {
    const { resource } = Schema.decodeUnknownSync(AssetCreateUrlInput)(payload);
    return [
      resource._tag === "workspace-file" ||
      resource._tag === "media-file" ||
      resource._tag === "draft-workspace-file"
        ? AuthFilesystemReadScope
        : AuthOrchestrationReadScope,
    ];
  }
  if (method === WS_METHODS.serverUpdateSettings) return requiredScopesForSettingsUpdate(payload);
  const guarded = clientRpcRequiredScopes(method, payload);
  if (guarded.length > 0) return guarded;
  return [requiredScopeForRpcMethod(method)];
};

/** Authorizes every RPC on one connection against that connection's session scopes. */
export const layer = (
  scopes: ReadonlyArray<AuthEnvironmentScope>,
  authority: ThreadReadAuthority,
  currentScopes: Effect.Effect<
    ReadonlyArray<AuthEnvironmentScope>,
    EnvironmentAuthorizationError
  > = Effect.succeed(scopes),
) =>
  Layer.succeed(RpcScopeAuthorization)((effect, { rpc, payload }) => {
    const toolCheck =
      Object.hasOwn(RPC_EXTRA_TOOLS, rpc._tag) && RPC_EXTRA_TOOLS[rpc._tag as WsRpcMethod]
        ? authorizeExtraTool(authority, requiredScopeForRpcMethod(rpc._tag))
        : Effect.void;
    return toolCheck.pipe(Effect.andThen(currentScopes)).pipe(
      Effect.flatMap((current) => {
        const requiredScopes = requiredScopesForRpcCall(rpc._tag, payload);
        const requiredScope = requiredScopes.find((scope) => !current.includes(scope));
        return requiredScope === undefined
          ? effect
          : Effect.fail(rpcAuthorizationError(requiredScope));
      }),
    );
  });

/** Retrying can install or restart tools even though ordinary listing is readable. */
export const requiredScopeForDeviceList = (input: DeviceListInput): AuthEnvironmentScope =>
  input.retryHostId || input.updateTool
    ? AuthOrchestrationOperateScope
    : AuthOrchestrationReadScope;
