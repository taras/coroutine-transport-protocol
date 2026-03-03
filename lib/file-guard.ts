/**
 * useFileContentGuard — replay guard for file-backed effects.
 *
 * Detects when a file's content has changed since the journal entry was
 * recorded. Effects store a file path and content hash in their `meta`;
 * this guard recomputes the hash during the check phase and compares
 * during the decide phase.
 *
 * This is the primary use case for the executable document runtime:
 * if a source file has changed, the system should detect it and error
 * rather than silently replaying stale content.
 *
 * See replay-guard-spec.md §6.
 */

import { call, useScope } from "@effection/effection";
import type { Operation } from "@effection/effection";
import { StaleInputError } from "./errors.ts";
import { ReplayGuard, type ReplayOutcome } from "./replay-guard.ts";

/**
 * Compute a SHA-256 hash of file content.
 *
 * Uses Web Crypto API (available in Deno and modern browsers).
 */
async function computeFileHash(filePath: string): Promise<string> {
  const content = await Deno.readFile(filePath);
  const hashBuffer = await crypto.subtle.digest("SHA-256", content);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Install a file content replay guard on the current scope.
 *
 * Effects that read files should store metadata with:
 * - `filePath`: the path to the file that was read
 * - `fileSHA`: the SHA-256 hash of the file's content at read time
 *
 * During replay, if the file's current content hash differs from the
 * stored hash, the guard returns an error outcome and replay halts
 * with `StaleInputError`.
 *
 * ## Lifecycle Notes
 *
 * - **Install timing**: Must be called before `durableRun()` so the guard
 *   observes all journal events during the check phase.
 * - **Cache lifetime**: The internal file hash cache lives for the scope's
 *   lifetime. Each `durableRun()` creates a fresh scope chain, so caches
 *   don't persist across workflow runs.
 * - **Check/Decide separation**: The check phase runs before replay starts
 *   (I/O allowed), and the decide phase runs during replay (synchronous).
 *   The cache is populated in check and only read in decide — no concurrent
 *   mutation.
 * - **Cancellation**: File hashing in the check phase uses `yield* call()`,
 *   making it cancellable if the workflow is aborted during startup.
 *
 * ## Usage
 *
 * ```ts
 * function* workflow(): Operation<void> {
 *   // Install the guard — children inherit it
 *   yield* useFileContentGuard();
 *
 *   // Effects that store file metadata will be validated on replay
 *   const content = yield* durableCall("readFile", async () => {
 *     const data = await Deno.readTextFile("./input.txt");
 *     return data;
 *   }, {
 *     meta: (content) => ({
 *       filePath: "./input.txt",
 *       fileSHA: computeHash(content),
 *     }),
 *   });
 *
 *   yield* durableRun(innerWorkflow, { stream });
 * }
 * ```
 *
 * Note: The guard only validates events that have `filePath` and `fileSHA`
 * in their metadata. Events without this metadata pass through unchanged
 * (preserving "logs are authoritative" for effects that don't opt in).
 */
export function* useFileContentGuard(): Operation<void> {
  const scope = yield* useScope();

  // Cache: filePath → current SHA, populated during check phase
  const cache = new Map<string, string>();

  scope.around(ReplayGuard, {
    /**
     * Phase 1: Check — hash files mentioned in metadata.
     *
     * Runs in generator context before replay begins. I/O is allowed.
     * Results are cached for the decide phase.
     */
    *check([event], next): Operation<void> {
      const meta = event.meta;
      if (
        meta &&
        typeof meta.filePath === "string" &&
        typeof meta.fileSHA === "string"
      ) {
        const filePath = meta.filePath;
        if (!cache.has(filePath)) {
          try {
            const currentSHA = yield* call(() => computeFileHash(filePath));
            cache.set(filePath, currentSHA);
          } catch {
            // File doesn't exist or is unreadable — will be detected as
            // stale in decide phase since cached SHA will be undefined
          }
        }
      }
      // Always call next — other middleware may need to check this event too
      return yield* next(event);
    },

    /**
     * Phase 2: Decide — compare stored hash to current hash.
     *
     * Must be pure and synchronous — no I/O, no side effects.
     * Reads from the cache populated during check phase.
     */
    decide([event], next): ReplayOutcome {
      const meta = event.meta;
      if (
        meta &&
        typeof meta.filePath === "string" &&
        typeof meta.fileSHA === "string"
      ) {
        const filePath = meta.filePath;
        const storedSHA = meta.fileSHA;
        const currentSHA = cache.get(filePath);

        if (currentSHA === undefined) {
          // File was unreadable during check (probably deleted)
          return {
            outcome: "error",
            error: new StaleInputError(
              `File not found or unreadable: ${filePath}`,
              {
                coroutineId: event.coroutineId,
                description: event.description,
              },
            ),
          };
        }

        if (currentSHA !== storedSHA) {
          return {
            outcome: "error",
            error: new StaleInputError(
              `File changed: ${filePath} ` +
                `(recorded: ${storedSHA.slice(0, 8)}..., ` +
                `current: ${currentSHA.slice(0, 8)}...)`,
              {
                coroutineId: event.coroutineId,
                description: event.description,
              },
            ),
          };
        }
      }

      // No opinion — delegate to next middleware or default
      return next(event);
    },
  });
}
