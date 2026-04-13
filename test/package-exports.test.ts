/**
 * Package exports smoke test.
 *
 * Verifies that `npm pack` produces a tarball whose subpath exports
 * resolve correctly when installed as a dependency. Imports go through
 * Node's package resolution (bare specifiers), not direct dist/ paths.
 */

import { execSync } from "node:child_process";
import { mkdtempSync, rmSync, existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, it, expect, beforeAll, afterAll } from "vitest";

const PKG_ROOT = resolve(import.meta.dirname, "..");

/**
 * Run an ESM script in the temp dir where delaykit is installed.
 * Bare imports like "delaykit" resolve through Node's exports map.
 */
function runInTmpDir(tmpDir: string, script: string): string {
  const scriptPath = join(tmpDir, "_test.mjs");
  writeFileSync(scriptPath, script);
  return execSync(`node ${scriptPath}`, {
    cwd: tmpDir,
    encoding: "utf8",
    timeout: 10_000,
  });
}

describe("package exports", () => {
  let tmpDir: string;

  beforeAll(() => {
    execSync("npm run build", { cwd: PKG_ROOT, stdio: "pipe" });

    // --ignore-scripts skips the `prepack` hook — we've already built
    // above and don't want the build banner mixed into stdout, where
    // it would garble the tarball filename this test captures.
    const tarball = execSync("npm pack --ignore-scripts --pack-destination /tmp", {
      cwd: PKG_ROOT,
      encoding: "utf8",
    }).trim();

    tmpDir = mkdtempSync(join(tmpdir(), "delaykit-exports-"));
    execSync('npm init -y && npm pkg set type="module"', { cwd: tmpDir, stdio: "pipe" });
    // Install delaykit + optional peer deps so all subpath exports resolve
    execSync(`npm install /tmp/${tarball} postgres @posthook/node`, { cwd: tmpDir, stdio: "pipe" });
  });

  afterAll(() => {
    if (tmpDir && existsSync(tmpDir)) {
      rmSync(tmpDir, { recursive: true });
    }
  });

  it('import "delaykit" (main entry)', () => {
    const output = runInTmpDir(tmpDir, `
      import { DelayKit, ACTIVE_STATUSES, parseDuration, executeJob } from "delaykit";
      const checks = {
        DelayKit: typeof DelayKit === "function",
        ACTIVE_STATUSES: ACTIVE_STATUSES instanceof Set,
        parseDuration: typeof parseDuration === "function",
        executeJob: typeof executeJob === "function",
      };
      console.log(JSON.stringify(checks));
    `);
    const checks = JSON.parse(output.trim());
    expect(checks).toEqual({
      DelayKit: true,
      ACTIVE_STATUSES: true,
      parseDuration: true,
      executeJob: true,
    });
  });

  it('import "delaykit/memory"', () => {
    const output = runInTmpDir(tmpDir, `
      import { MemoryStore } from "delaykit/memory";
      console.log(JSON.stringify({ MemoryStore: typeof MemoryStore === "function" }));
    `);
    expect(JSON.parse(output.trim())).toEqual({ MemoryStore: true });
  });

  it('import "delaykit/polling"', () => {
    const output = runInTmpDir(tmpDir, `
      import { PollingScheduler } from "delaykit/polling";
      console.log(JSON.stringify({ PollingScheduler: typeof PollingScheduler === "function" }));
    `);
    expect(JSON.parse(output.trim())).toEqual({ PollingScheduler: true });
  });

  it('import "delaykit/postgres"', () => {
    const output = runInTmpDir(tmpDir, `
      import { PostgresStore } from "delaykit/postgres";
      console.log(JSON.stringify({ PostgresStore: typeof PostgresStore === "function" }));
    `);
    expect(JSON.parse(output.trim())).toEqual({ PostgresStore: true });
  });

  it('import "delaykit/posthook"', () => {
    const output = runInTmpDir(tmpDir, `
      import { PosthookScheduler } from "delaykit/posthook";
      console.log(JSON.stringify({ PosthookScheduler: typeof PosthookScheduler === "function" }));
    `);
    expect(JSON.parse(output.trim())).toEqual({ PosthookScheduler: true });
  });

  it("DelayKit can be instantiated with MemoryStore + PollingScheduler", () => {
    const output = runInTmpDir(tmpDir, `
      import { DelayKit } from "delaykit";
      import { MemoryStore } from "delaykit/memory";
      import { PollingScheduler } from "delaykit/polling";

      const store = new MemoryStore();
      const dk = new DelayKit({ store, scheduler: new PollingScheduler() });

      dk.handle("test", async () => {});
      const { job, created } = await dk.schedule("test", { key: "smoke:1", delay: "5s" });
      console.log(JSON.stringify({ created, key: job.key }));
      await store.close();
    `);
    expect(JSON.parse(output.trim())).toEqual({ created: true, key: "smoke:1" });
  });
});
