/**
 * Test helpers for running Effection v4 Operations in Deno tests.
 *
 * Provides a `test()` wrapper that accepts generator function bodies,
 * eliminating the `await run(() => ...)` boilerplate from every test.
 */

import { run } from "@effection/effection";
import type { Operation } from "@effection/effection";

/**
 * Register a Deno test whose body is an Effection Operation (generator).
 *
 * Usage:
 * ```ts
 * test("my test", function* () {
 *   const result = yield* someOperation();
 *   assertEquals(result, expected);
 * });
 * ```
 *
 * Equivalent to:
 * ```ts
 * Deno.test("my test", async () => {
 *   await run(function* () { ... });
 * });
 * ```
 */
export function test(name: string, body: () => Operation<void>): void {
  Deno.test(name, async () => {
    await run(body);
  });
}
