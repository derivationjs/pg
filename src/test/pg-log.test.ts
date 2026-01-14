import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import postgres from "postgres";
import { z } from "zod";
import { Graph } from "derivation";
import { PgLog } from "../pg-log.js";
import { PgNotifier } from "../pg-notifier.js";

const sql = postgres({
  host: "/var/run/postgresql",
});

const TestDataSchema = z.object({
  value: z.number(),
});

describe("PgLog", () => {
  let graph: Graph;
  let notifier: PgNotifier;

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
    notifier = new PgNotifier(sql, graph);
  });

  afterAll(async () => {
    await sql`DROP TABLE IF EXISTS test_log`;
    await sql.end();
  });

  it("loads empty table", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    expect(log.snapshot.size).toBe(0);
    expect(log.length.value).toBe(0);
  });

  it("appends and persists", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    const row = await log.append({ value: 42 });

    expect(row.seq).toBe(1);
    expect(row.data).toEqual({ value: 42 });

    graph.step();

    expect(log.snapshot.size).toBe(1);
    expect(log.snapshot.get(0)?.data).toEqual({ value: 42 });
  });

  it("loads existing rows on create", async () => {
    // Insert data first
    await sql`INSERT INTO test_log (data) VALUES ('{"value": 42}'::jsonb)`;

    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    expect(log.snapshot.size).toBe(1);
    expect(log.snapshot.get(0)?.data).toEqual({ value: 42 });
  });

  it("appendAll inserts multiple rows", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    const rows = await log.appendAll([{ value: 1 }, { value: 2 }, { value: 3 }]);
    graph.step();

    expect(rows.length).toBe(3);
    expect(rows[0]?.seq).toBe(1);
    expect(rows[1]?.seq).toBe(2);
    expect(rows[2]?.seq).toBe(3);

    expect(log.snapshot.size).toBe(3);
    expect(log.length.value).toBe(3);
  });

  it("fold works over persisted data", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    await log.appendAll([{ value: 10 }, { value: 20 }, { value: 30 }]);
    graph.step();

    const sum = log.fold(0, (acc, row) => acc + row.data.value);
    expect(sum.value).toBe(60);

    await log.append({ value: 5 });
    graph.step();

    expect(sum.value).toBe(65);
  });

  it("poll syncs new rows from database", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    // Insert directly into database (simulating another process)
    await sql`INSERT INTO test_log (data) VALUES ('{"value": 100}'::jsonb)`;
    await sql`INSERT INTO test_log (data) VALUES ('{"value": 200}'::jsonb)`;

    expect(log.snapshot.size).toBe(0);

    await log.poll();
    graph.step();

    expect(log.snapshot.size).toBe(2);
    expect(log.snapshot.get(0)?.data).toEqual({ value: 100 });
    expect(log.snapshot.get(1)?.data).toEqual({ value: 200 });
  });

  it("rejects invalid data on append", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    // @ts-expect-error - intentionally passing invalid data
    await expect(log.append({ value: "not a number" })).rejects.toThrow();
  });

  it("rejects invalid data on load", async () => {
    // Insert invalid data directly
    await sql`INSERT INTO test_log (data) VALUES ('{"value": "bad"}'::jsonb)`;

    await expect(
      PgLog.create(sql, "test_log", graph, notifier, TestDataSchema)
    ).rejects.toThrow();
  });

  it("rejects invalid data on poll", async () => {
    const log = await PgLog.create(sql, "test_log", graph, notifier, TestDataSchema);

    // Insert invalid data directly
    await sql`INSERT INTO test_log (data) VALUES ('{"value": "bad"}'::jsonb)`;

    await expect(log.poll()).rejects.toThrow();
  });
});
