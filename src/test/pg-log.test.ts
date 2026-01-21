import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import postgres from "postgres";
import { z } from "zod";
import { Graph } from "derivation";
import { PgLog } from "../pg-log.js";

const sql = postgres({
  host: "/var/run/postgresql",
});

const TestDataSchema = z.object({
  value: z.number(),
});

describe("PgLog", () => {
  let graph: Graph;

  beforeAll(async () => {
    await sql`
      CREATE TABLE IF NOT EXISTS test_log (
        seq BIGSERIAL PRIMARY KEY,
        data JSONB NOT NULL
      )
    `;
  });

  beforeEach(async () => {
    await sql`TRUNCATE test_log RESTART IDENTITY`;
    graph = new Graph();
  });

  afterAll(async () => {
    await sql`DROP TABLE IF EXISTS test_log`;
    await sql.end();
  });

  it("loads empty table", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    expect(log.asLog.snapshot.length).toBe(0);
    expect(log.asLog.length.value).toBe(0);
  });

  it("loads existing rows on create", async () => {
    await sql`INSERT INTO test_log (data) VALUES ('{"value": 42}'::jsonb)`;

    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    expect(log.asLog.snapshot.length).toBe(1);
    expect(log.asLog.snapshot.get(0)?.data).toEqual({ value: 42 });
  });

  it("appends and persists", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    await log.append({ value: 42 });

    await log.poll();
    graph.step();

    expect(log.asLog.snapshot.length).toBe(1);
    expect(log.asLog.snapshot.get(0)?.data).toEqual({ value: 42 });
  });

  it("appendAll inserts multiple rows", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    await log.appendAll([{ value: 1 }, { value: 2 }, { value: 3 }]);

    await log.poll();
    graph.step();

    expect(log.asLog.snapshot.length).toBe(3);
    expect(log.asLog.length.value).toBe(3);
  });

  it("fold works over persisted data", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    await log.appendAll([{ value: 10 }, { value: 20 }, { value: 30 }]);

    await log.poll();
    graph.step();

    const sum = log.asLog.fold(0, (acc, row) => acc + row.data.value);
    expect(sum.value).toBe(60);

    await log.append({ value: 5 });
    await log.poll();
    graph.step();

    expect(sum.value).toBe(65);
  });

  it("poll syncs new rows from database", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    await sql`INSERT INTO test_log (data) VALUES ('{"value": 100}'::jsonb)`;
    await sql`INSERT INTO test_log (data) VALUES ('{"value": 200}'::jsonb)`;

    expect(log.asLog.snapshot.length).toBe(0);

    await log.poll();
    graph.step();

    expect(log.asLog.snapshot.length).toBe(2);
    expect(log.asLog.snapshot.get(0)?.data).toEqual({ value: 100 });
    expect(log.asLog.snapshot.get(1)?.data).toEqual({ value: 200 });
  });

  it("rejects invalid data on append", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    // @ts-expect-error - intentionally passing invalid data
    await expect(log.append({ value: "not a number" })).rejects.toThrow();
  });

  it("rejects invalid data on load", async () => {
    await sql`INSERT INTO test_log (data) VALUES ('{"value": "invalid"}'::jsonb)`;

    await expect(
      PgLog.create(sql, "test_log", graph, TestDataSchema),
    ).rejects.toThrow();
  });

  it("rejects invalid data on poll", async () => {
    const log = await PgLog.create(sql, "test_log", graph, TestDataSchema);

    await sql`INSERT INTO test_log (data) VALUES ('{"value": "invalid"}'::jsonb)`;

    await expect(log.poll()).rejects.toThrow();
  });
});
