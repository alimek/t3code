// @effect-diagnostics nodeBuiltinImport:off -- bound user regex CPU without reading unadmitted files.
import * as NodeVM from "node:vm";
import { isWholeWordRange } from "./WorkspaceSearchIndex.ts";
import {
  AuthFilesystemReadScope,
  AuthFilesystemWriteScope,
  type AuthEnvironmentScope,
  type ProjectReadFileInput,
  type ProjectWriteFileInput,
  type ProjectListEntriesInput,
  type ProjectSearchEntriesInput,
  type ProjectSearchContentsInput,
  type ProjectSearchContentsResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import { threadReadDenied, type ThreadReadAuthority } from "../auth/ResourceAuthorization.ts";
import { resourceRestricted } from "../auth/ToolAuthorization.ts";
import { current } from "../assets/AssetAuthority.ts";
import * as Access from "../auth/workspaceReadAccess.ts";
import * as Files from "./WorkspaceFileSystem.ts";
import * as Entries from "./WorkspaceEntries.ts";

const matchContents = new NodeVM.Script(`
  for (const [index, lineContent] of lines.entries()) {
    const matchRanges = [...lineContent.matchAll(regex)].flatMap(match => {
      const start = match.index, end = start + match[0].length;
      return end === start || (wholeWord && !isWholeWordRange(lineContent, {start, end}))
        ? [] : [{start, end}];
    });
    if (matchRanges.length > 0) matches.push({path, lineNumber: index + 1, lineContent, matchRanges});
    if (matches.length >= limit) break;
  }
`);

/** Public file operations retain existing scopes and registered-root admission. */
export const make = Effect.gen(function* () {
  const access = yield* Access.make;
  const files = yield* Files.WorkspaceFileSystem;
  const entries = yield* Entries.WorkspaceEntries;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const restricted = (
    authority: ThreadReadAuthority,
    scope: AuthEnvironmentScope = AuthFilesystemReadScope,
  ) =>
    current(authority, scope).pipe(
      Effect.map((value) => value !== undefined && resourceRestricted(value.resourceGrant)),
    );
  const target = Effect.fnUntraced(
    function* (authority: ThreadReadAuthority, cwd: string, relativePath: string, write = false) {
      const scope = write ? AuthFilesystemWriteScope : AuthFilesystemReadScope;
      yield* access.admit(authority, { cwd }, scope);
      if (!(yield* restricted(authority, scope))) return;
      if (relativePath.includes("\0")) return yield* threadReadDenied();
      const requested = path.resolve(cwd, relativePath);
      const relative = path.relative(cwd, requested);
      if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative))
        return yield* threadReadDenied();
      let existing = requested;
      if (write) {
        while (!(yield* fs.exists(existing))) existing = path.dirname(existing);
      }
      const [root, canonical] = yield* Effect.all([fs.realPath(cwd), fs.realPath(existing)]);
      const resolved = path.relative(root, canonical);
      if (resolved === ".." || resolved.startsWith(`..${path.sep}`) || path.isAbsolute(resolved))
        return yield* threadReadDenied();
      const info = yield* fs.stat(existing);
      yield* access.admit(
        authority,
        { cwd: info.type === "Directory" ? existing : path.dirname(existing) },
        scope,
      );
    },
    Effect.mapError(() => threadReadDenied()),
  );
  const runFile = <A, E, R>(
    authority: ThreadReadAuthority,
    input: ProjectReadFileInput,
    operation: Effect.Effect<A, E, R>,
    write = false,
  ) =>
    access.run(
      authority,
      input,
      Effect.gen(function* () {
        const admission = yield* Access.workspaceEffectAdmission;
        if (admission === undefined) return yield* threadReadDenied();
        yield* target(authority, input.cwd, input.relativePath, write);
        return yield* operation.pipe(
          Effect.provideService(Access.workspaceEffectAdmission, {
            ...admission,
            beforeEffect: (cwd, scope) =>
              admission
                .beforeEffect(cwd, scope)
                .pipe(Effect.andThen(target(authority, input.cwd, input.relativePath, write))),
          }),
        );
      }),
      write ? AuthFilesystemWriteScope : AuthFilesystemReadScope,
    );
  const readFile = (authority: ThreadReadAuthority, input: ProjectReadFileInput) =>
    runFile(authority, input, files.readFile(input));
  const writeFile = (authority: ThreadReadAuthority, input: ProjectWriteFileInput) =>
    runFile(authority, input, files.writeFile(input), true);
  const filter = Effect.fnUntraced(
    function* <A extends { readonly path: string }>(
      authority: ThreadReadAuthority,
      cwd: string,
      values: ReadonlyArray<A>,
    ) {
      if (!(yield* restricted(authority))) return values;
      const root = yield* fs.realPath(cwd);
      const directories = new Map<string, boolean>();
      const allowed = yield* Effect.forEach(
        values,
        (entry) =>
          Effect.gen(function* () {
            const requested = path.resolve(cwd, entry.path);
            const canonical = yield* fs.realPath(requested);
            const relative = path.relative(root, canonical);
            if (
              relative === ".." ||
              relative.startsWith(`..${path.sep}`) ||
              path.isAbsolute(relative)
            )
              return undefined;
            const info = yield* fs.stat(canonical);
            const directory = info.type === "Directory" ? requested : path.dirname(requested);
            const canonicalDirectory =
              info.type === "Directory" ? canonical : path.dirname(canonical);
            for (const directoryPath of new Set([directory, canonicalDirectory])) {
              if (!directories.has(directoryPath)) {
                const result = yield* access
                  .admit(authority, { cwd: directoryPath }, AuthFilesystemReadScope)
                  .pipe(Effect.result);
                directories.set(directoryPath, Result.isSuccess(result));
              }
              if (!directories.get(directoryPath)) return undefined;
            }
            return entry;
          }).pipe(Effect.orElseSucceed(() => undefined)),
        { concurrency: 1 },
      );
      // Recheck each distinct owner once after the last filesystem await.
      for (const [directory, admitted] of directories)
        if (admitted) yield* access.admit(authority, { cwd: directory }, AuthFilesystemReadScope);
      return allowed.filter((entry) => entry !== undefined);
    },
    Effect.mapError(() => threadReadDenied()),
  );
  const list = (authority: ThreadReadAuthority, input: ProjectListEntriesInput) =>
    access.run(
      authority,
      input,
      Effect.gen(function* () {
        if (input.directoryPath !== undefined)
          yield* target(authority, input.cwd, input.directoryPath);
        const result = yield* entries.list(input);
        return { ...result, entries: yield* filter(authority, input.cwd, result.entries) };
      }),
      AuthFilesystemReadScope,
    );
  const search = (authority: ThreadReadAuthority, input: ProjectSearchEntriesInput) =>
    access.run(
      authority,
      input,
      entries
        .search(input)
        .pipe(
          Effect.flatMap((result) =>
            filter(authority, input.cwd, result.entries).pipe(
              Effect.map((values) => ({ ...result, entries: values })),
            ),
          ),
        ),
      AuthFilesystemReadScope,
    );
  const searchContents = (authority: ThreadReadAuthority, input: ProjectSearchContentsInput) =>
    access.run(
      authority,
      input,
      Effect.gen(function* () {
        if (!(yield* restricted(authority))) return yield* entries.searchContents(input);
        // Native grep scans the entire root. Restricted reads use admitted files only.
        const listing = yield* list(authority, { cwd: input.cwd });
        const literal = input.query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        let regex: RegExp;
        let regexFallbackError: string | undefined;
        try {
          regex = new RegExp(
            input.useRegex ? input.query : literal,
            input.caseSensitive ? "g" : "gi",
          );
        } catch (error) {
          regexFallbackError = String(error);
          regex = new RegExp(literal, input.caseSensitive ? "g" : "gi");
        }
        const matches: ProjectSearchContentsResult["matches"][number][] = [];
        // ponytail: same 250ms partial-result ceiling as native grep; use an admitted native file allowlist if it becomes available.
        const deadline = performance.now() + 250;
        let truncated = listing.truncated;
        for (const entry of listing.entries) {
          if (entry.kind !== "file") continue;
          if (performance.now() >= deadline || matches.length >= input.limit) {
            truncated = true;
            break;
          }
          const result = yield* readFile(authority, {
            cwd: input.cwd,
            relativePath: entry.path,
          }).pipe(Effect.result);
          if (Result.isFailure(result)) continue;
          truncated ||= result.success.truncated;
          const remaining = Math.ceil(deadline - performance.now());
          if (remaining <= 0) {
            truncated = true;
            break;
          }
          const matched = yield* Effect.try(() =>
            matchContents.runInNewContext(
              {
                lines: result.success.contents.split("\n"),
                regex,
                matches,
                path: entry.path,
                limit: input.limit,
                wholeWord: input.wholeWord,
                isWholeWordRange,
              },
              { timeout: remaining },
            ),
          ).pipe(Effect.result);
          if (Result.isFailure(matched) || matches.length >= input.limit) {
            truncated = true;
            break;
          }
        }
        return {
          matches,
          truncated,
          ...(regexFallbackError === undefined ? {} : { regexFallbackError }),
        };
      }),
      AuthFilesystemReadScope,
    );
  return { readFile, writeFile, list, search, searchContents };
});
