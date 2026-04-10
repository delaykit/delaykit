import { describe, it, expect } from "vitest";
import { parseDuration, delayToDate } from "../src/duration.js";

describe("parseDuration", () => {
  it("parses seconds", () => {
    expect(parseDuration("5s")).toBe(5_000);
    expect(parseDuration("30s")).toBe(30_000);
  });

  it("parses minutes", () => {
    expect(parseDuration("5m")).toBe(300_000);
    expect(parseDuration("30m")).toBe(1_800_000);
  });

  it("parses hours", () => {
    expect(parseDuration("1h")).toBe(3_600_000);
    expect(parseDuration("24h")).toBe(86_400_000);
  });

  it("parses days", () => {
    expect(parseDuration("1d")).toBe(86_400_000);
    expect(parseDuration("7d")).toBe(604_800_000);
  });

  it("parses compound durations", () => {
    expect(parseDuration("1h30m")).toBe(5_400_000);
    expect(parseDuration("1d12h")).toBe(129_600_000);
    expect(parseDuration("2h30m15s")).toBe(9_015_000);
  });

  it("parses milliseconds", () => {
    expect(parseDuration("500ms")).toBe(500);
    expect(parseDuration("100ms")).toBe(100);
  });

  it("throws on invalid input", () => {
    expect(() => parseDuration("")).toThrow("Invalid duration");
    expect(() => parseDuration("abc")).toThrow("Invalid duration");
    expect(() => parseDuration("5")).toThrow("Invalid duration");
    expect(() => parseDuration("5x")).toThrow("Invalid duration");
  });

  it("rejects trailing garbage", () => {
    expect(() => parseDuration("1hgarbage")).toThrow("Unexpected text");
    expect(() => parseDuration("abc5m")).toThrow("Unexpected text");
    expect(() => parseDuration("5s ")).toThrow("Unexpected text");
    expect(() => parseDuration("5s5")).toThrow("Unexpected text");
  });
});

describe("delayToDate", () => {
  it("returns a future Date", () => {
    const before = Date.now();
    const result = delayToDate("5s");
    const after = Date.now();

    expect(result.getTime()).toBeGreaterThanOrEqual(before + 5_000);
    expect(result.getTime()).toBeLessThanOrEqual(after + 5_000);
  });
});
