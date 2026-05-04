/**
 * Package exports smoke test.
 *
 * Verifies that `npm pack` produces a tarball whose subpath exports
 * resolve correctly when installed as a dependency. Imports go through
 * Node's package resolution (bare specifiers), not direct dist/ paths.
 */

import { execSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, existsSync, writeFileSync } from "node:fs";
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

let tarballPath: string;

beforeAll(() => {
  execSync("npm run build", { cwd: PKG_ROOT, stdio: "pipe" });

  // --ignore-scripts skips the `prepack` hook — we've already built
  // above and don't want the build banner mixed into stdout, where
  // it would garble the tarball filename this test captures.
  const tarball = execSync("npm pack --ignore-scripts --pack-destination /tmp", {
    cwd: PKG_ROOT,
    encoding: "utf8",
  }).trim();
  tarballPath = `/tmp/${tarball}`;
});

describe("package exports", () => {
  let tmpDir: string;

  beforeAll(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "delaykit-exports-"));
    execSync('npm init -y && npm pkg set type="module"', { cwd: tmpDir, stdio: "pipe" });
    // Install delaykit + optional peer deps so all subpath exports resolve
    execSync(`npm install ${tarballPath} postgres @posthook/node better-sqlite3`, { cwd: tmpDir, stdio: "pipe" });
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
      import { PostgresStore, runPostgresMigrations, LATEST_POSTGRES_MIGRATION_VERSION } from "delaykit/postgres";
      console.log(JSON.stringify({
        PostgresStore: typeof PostgresStore === "function",
        runPostgresMigrations: typeof runPostgresMigrations === "function",
        LATEST_POSTGRES_MIGRATION_VERSION: typeof LATEST_POSTGRES_MIGRATION_VERSION === "number",
      }));
    `);
    expect(JSON.parse(output.trim())).toEqual({
      PostgresStore: true,
      runPostgresMigrations: true,
      LATEST_POSTGRES_MIGRATION_VERSION: true,
    });
  });

  it('import "delaykit/sqlite"', () => {
    const output = runInTmpDir(tmpDir, `
      import { SQLiteStore, runSQLiteMigrations, LATEST_SQLITE_MIGRATION_VERSION } from "delaykit/sqlite";
      console.log(JSON.stringify({
        SQLiteStore: typeof SQLiteStore === "function",
        runSQLiteMigrations: typeof runSQLiteMigrations === "function",
        LATEST_SQLITE_MIGRATION_VERSION: typeof LATEST_SQLITE_MIGRATION_VERSION === "number",
      }));
    `);
    expect(JSON.parse(output.trim())).toEqual({
      SQLiteStore: true,
      runSQLiteMigrations: true,
      LATEST_SQLITE_MIGRATION_VERSION: true,
    });
  });

  it('import "delaykit/posthook"', () => {
    const output = runInTmpDir(tmpDir, `
      import { PosthookScheduler } from "delaykit/posthook";
      console.log(JSON.stringify({ PosthookScheduler: typeof PosthookScheduler === "function" }));
    `);
    expect(JSON.parse(output.trim())).toEqual({ PosthookScheduler: true });
  });

  it("emitted dist/schedulers/posthook.d.ts has no '@posthook/node' import — peer must not leak into public types", () => {
    const dts = readFileSync(
      join(PKG_ROOT, "dist", "schedulers", "posthook.d.ts"),
      "utf8",
    );
    // Strip docstring/comment lines so prose mentions of @posthook/node
    // (which are fine) don't trip the regex. Only real import statements
    // would break consumers without skipLibCheck.
    const code = dts.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
    expect(code).not.toMatch(/from ["']@posthook\/node["']/);
    expect(code).not.toMatch(/import\(["']@posthook\/node["']\)/);
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

describe("package exports without optional peers", () => {
  let tmpDir: string;

  beforeAll(() => {
    tmpDir = mkdtempSync(join(tmpdir(), "delaykit-no-peers-"));
    execSync('npm init -y && npm pkg set type="module"', { cwd: tmpDir, stdio: "pipe" });
    // Install delaykit alone — no postgres/@posthook/node/better-sqlite3.
    // The core, memory, and polling subpaths must resolve and run without
    // the optional peers being present.
    execSync(`npm install ${tarballPath}`, { cwd: tmpDir, stdio: "pipe" });
  });

  afterAll(() => {
    if (tmpDir && existsSync(tmpDir)) {
      rmSync(tmpDir, { recursive: true });
    }
  });

  it("core, memory, and polling subpaths import without peers", () => {
    const output = runInTmpDir(tmpDir, `
      import { DelayKit } from "delaykit";
      import { MemoryStore } from "delaykit/memory";
      import { PollingScheduler } from "delaykit/polling";
      console.log(JSON.stringify({
        DelayKit: typeof DelayKit === "function",
        MemoryStore: typeof MemoryStore === "function",
        PollingScheduler: typeof PollingScheduler === "function",
      }));
    `);
    expect(JSON.parse(output.trim())).toEqual({
      DelayKit: true,
      MemoryStore: true,
      PollingScheduler: true,
    });
  });

  it("MemoryStore + PollingScheduler runs end-to-end without peers", () => {
    const output = runInTmpDir(tmpDir, `
      import { DelayKit } from "delaykit";
      import { MemoryStore } from "delaykit/memory";
      import { PollingScheduler } from "delaykit/polling";

      const store = new MemoryStore();
      const dk = new DelayKit({ store, scheduler: new PollingScheduler() });
      dk.handle("test", async () => {});
      const { job, created } = await dk.schedule("test", { key: "no-peers:1", delay: "5s" });
      console.log(JSON.stringify({ created, key: job.key }));
      await store.close();
    `);
    expect(JSON.parse(output.trim())).toEqual({ created: true, key: "no-peers:1" });
  });
});
