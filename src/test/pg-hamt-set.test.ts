import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import postgres from "postgres";
import { z } from "zod";
import { Graph } from "derivation";
import { PgHamtMap, type JsonValue } from "../pg-hamt-set.js";

const sql = postgres({
  host: "/var/run/postgresql",
});

const ITEMS_TABLE = "test_hamt_items";
const NODES_TABLE = "test_hamt_nodes";

const NumberValueSchema = z.object({
  value: z.number(),
});

async function createMapWith<T extends Exclude<JsonValue, null>>(
  schema: z.ZodType<T>,
  options?: { graph?: Graph; maxDepth?: number },
) {
  const graph = options?.graph ?? new Graph();
  const map = await PgHamtMap.create(
    sql,
    ITEMS_TABLE,
    NODES_TABLE,
    graph,
    schema,
    {
      maxDepth: options?.maxDepth ?? 4,
    },
  );
  return { map, graph };
}

async function createMap(graph = new Graph()) {
  return createMapWith(NumberValueSchema, { graph });
}

describe("PgHamtMap", () => {
  beforeAll(async () => {
    await sql`
      CREATE TABLE IF NOT EXISTS ${sql(ITEMS_TABLE)} (
        key TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        item_mac TEXT NOT NULL,
        leaf_path TEXT NOT NULL
      )
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS ${sql(NODES_TABLE)} (
        path TEXT PRIMARY KEY,
        level INTEGER NOT NULL,
        bitmap TEXT NOT NULL,
        count INTEGER NOT NULL,
        mac TEXT NOT NULL
      )
    `;
  });

  beforeEach(async () => {
    await sql`TRUNCATE ${sql(ITEMS_TABLE)}`;
    await sql`TRUNCATE ${sql(NODES_TABLE)}`;
  });

  afterAll(async () => {
    await sql`DROP TABLE IF EXISTS ${sql(ITEMS_TABLE)}`;
    await sql`DROP TABLE IF EXISTS ${sql(NODES_TABLE)}`;
    await sql.end();
  });

  it("rejects invalid maxDepth bounds before touching the database", async () => {
    const graph = new Graph();

    await expect(
      PgHamtMap.create(
        null as any,
        ITEMS_TABLE,
        NODES_TABLE,
        graph,
        NumberValueSchema,
        { maxDepth: 0 },
      ),
    ).rejects.toThrow("maxDepth must be >= 1");

    await expect(
      PgHamtMap.create(
        null as any,
        ITEMS_TABLE,
        NODES_TABLE,
        graph,
        NumberValueSchema,
        { maxDepth: 52 },
      ),
    ).rejects.toThrow("maxDepth must be <=");
  });

  it("creates the root node and loads an empty map", async () => {
    const { map } = await createMap();

    expect(map.reactive.snapshot.size).toBe(0);

    const rows = await sql<{ path: string; level: number; count: number }[]>`
      SELECT path, level, count
      FROM ${sql(NODES_TABLE)}
      ORDER BY path ASC
    `;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ path: "", level: 0, count: 0 });
  });

  it("set/poll/remove persists values and updates reactive state", async () => {
    const { map, graph } = await createMap();

    const key1 = await map.set({ value: 1 });
    const key2 = await map.set({ value: 2 });

    expect(typeof key1).toBe("string");
    expect(typeof key2).toBe("string");
    expect(key1).not.toBe(key2);

    await map.poll();
    graph.step();

    expect(map.reactive.snapshot.size).toBe(2);
    expect(map.reactive.snapshot.get(key1)?.value).toEqual({ value: 1 });
    expect(map.reactive.snapshot.get(key2)?.value).toEqual({ value: 2 });

    expect(await map.removeByHash("missing")).toBe(false);
    expect(await map.removeByHash(key1)).toBe(true);

    await map.poll();
    graph.step();

    expect(map.reactive.snapshot.size).toBe(1);
    expect(map.reactive.snapshot.get(key1)).toBeUndefined();
    expect(map.reactive.snapshot.get(key2)?.value).toEqual({ value: 2 });
  });

  it("setAll with duplicate values upserts one row and returns repeated hashes", async () => {
    const { map, graph } = await createMap();

    const keys = await map.setAll([{ value: 7 }, { value: 7 }]);

    expect(keys).toHaveLength(2);
    expect(keys[0]).toBe(keys[1]);

    await map.poll();
    graph.step();

    expect(map.reactive.snapshot.size).toBe(1);

    const rows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(ITEMS_TABLE)}
    `;
    expect(rows[0]?.count).toBe(1);
  });

  it("loads persisted rows when recreated", async () => {
    const first = await createMap(new Graph());
    const key = await first.map.set({ value: 42 });

    const second = await createMap(new Graph());

    expect(second.map.reactive.snapshot.size).toBe(1);
    expect(second.map.reactive.snapshot.get(key)?.value).toEqual({ value: 42 });
  });

  it("syncs changes across instances via poll", async () => {
    const a = await createMap(new Graph());
    const b = await createMap(new Graph());

    const key = await a.map.set({ value: 100 });

    expect(b.map.reactive.snapshot.size).toBe(0);
    await b.map.poll();
    b.graph.step();
    expect(b.map.reactive.snapshot.get(key)?.value).toEqual({ value: 100 });

    await a.map.removeByHash(key);
    await b.map.poll();
    b.graph.step();
    expect(b.map.reactive.snapshot.get(key)).toBeUndefined();
  });

  it("rejects invalid data on set", async () => {
    const { map } = await createMap();

    // intentional runtime validation test
    await expect(map.set({ value: "bad" } as any)).rejects.toThrow();
  });

  it("rejects corrupted stored keys on create", async () => {
    await sql`
      INSERT INTO ${sql(ITEMS_TABLE)} (key, data, item_mac, leaf_path)
      VALUES ('not-a-real-hash', '{"value":1}'::jsonb, 'bad-mac', 'xxxx')
    `;

    await expect(createMap()).rejects.toThrow("Stored key mismatch");
  });

  it("returns an empty array for setAll([]) and leaves storage empty", async () => {
    const { map } = await createMap();

    await expect(map.setAll([])).resolves.toEqual([]);

    const itemRows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(ITEMS_TABLE)}
    `;
    const nodeRows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(NODES_TABLE)}
    `;

    expect(itemRows[0]?.count).toBe(0);
    expect(nodeRows[0]?.count).toBe(1);
  });

  it("validates all setAll inputs before writing any rows", async () => {
    const { map } = await createMap();

    // intentional mixed runtime validation test
    await expect(map.setAll([{ value: 1 }, { value: "bad" }] as any)).rejects.toThrow();

    const rows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(ITEMS_TABLE)}
    `;
    expect(rows[0]?.count).toBe(0);
  });

  it("accepts a generic iterable in setAll", async () => {
    const { map, graph } = await createMap();

    function* values() {
      yield { value: 11 };
      yield { value: 12 };
    }

    const keys = await map.setAll(values());
    expect(keys).toHaveLength(2);

    await map.poll();
    graph.step();
    expect(map.reactive.snapshot.size).toBe(2);
  });

  it("returns the same hash when setting the same value repeatedly", async () => {
    const { map } = await createMap();

    const first = await map.set({ value: 9 });
    const second = await map.set({ value: 9 });

    expect(second).toBe(first);

    const rows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(ITEMS_TABLE)}
    `;
    expect(rows[0]?.count).toBe(1);
  });

  it("queues poll changes until graph.step()", async () => {
    const a = await createMap(new Graph());
    const b = await createMap(new Graph());

    const key = await a.map.set({ value: 300 });
    await b.map.poll();

    expect(b.map.reactive.snapshot.get(key)).toBeUndefined();

    b.graph.step();
    expect(b.map.reactive.snapshot.get(key)?.value).toEqual({ value: 300 });
  });

  it("syncs multiple externally added values in one poll", async () => {
    const a = await createMap(new Graph());
    const b = await createMap(new Graph());

    const keys = await a.map.setAll([{ value: 21 }, { value: 22 }, { value: 23 }]);

    await b.map.poll();
    b.graph.step();

    expect(b.map.reactive.snapshot.size).toBe(3);
    expect(b.map.reactive.snapshot.get(keys[0]!)?.value).toEqual({ value: 21 });
    expect(b.map.reactive.snapshot.get(keys[1]!)?.value).toEqual({ value: 22 });
    expect(b.map.reactive.snapshot.get(keys[2]!)?.value).toEqual({ value: 23 });
  });

  it("uses default maxDepth=8 when not provided", async () => {
    const graph = new Graph();
    const map = await PgHamtMap.create(
      sql,
      ITEMS_TABLE,
      NODES_TABLE,
      graph,
      NumberValueSchema,
      {},
    );

    const key = await map.set({ value: 55 });
    const rows = await sql<{ leaf_path: string }[]>`
      SELECT leaf_path FROM ${sql(ITEMS_TABLE)} WHERE key = ${key}
    `;

    expect(rows[0]?.leaf_path).toHaveLength(8);
  });

  it("removes persisted row and leaves only the root node after deleting the last item", async () => {
    const { map } = await createMap();

    const key = await map.set({ value: 500 });
    expect(await map.removeByHash(key)).toBe(true);

    const itemRows = await sql<{ count: number }[]>`
      SELECT COUNT(*)::int AS count FROM ${sql(ITEMS_TABLE)}
    `;
    const nodeRows = await sql<{ path: string; level: number; count: number }[]>`
      SELECT path, level, count
      FROM ${sql(NODES_TABLE)}
      ORDER BY path ASC
    `;

    expect(itemRows[0]?.count).toBe(0);
    expect(nodeRows).toHaveLength(1);
    expect(nodeRows[0]).toMatchObject({ path: "", level: 0, count: 0 });
  });

  it("rejects invalid stored schema data on create", async () => {
    const { map } = await createMap();
    const key = await map.set({ value: 123 });

    await sql`
      UPDATE ${sql(ITEMS_TABLE)}
      SET data = '{"value":"bad"}'::jsonb
      WHERE key = ${key}
    `;

    await expect(createMap()).rejects.toThrow();
  });

  it("rejects corrupted stored leaf_path on create", async () => {
    const { map } = await createMap();
    const key = await map.set({ value: 124 });

    await sql`
      UPDATE ${sql(ITEMS_TABLE)}
      SET leaf_path = 'zzzz'
      WHERE key = ${key}
    `;

    await expect(createMap()).rejects.toThrow("Stored leaf_path mismatch");
  });

  it("rejects corrupted stored item_mac on create", async () => {
    const { map } = await createMap();
    const key = await map.set({ value: 125 });

    await sql`
      UPDATE ${sql(ITEMS_TABLE)}
      SET item_mac = 'not-the-right-mac'
      WHERE key = ${key}
    `;

    await expect(createMap()).rejects.toThrow("Stored item_mac mismatch");
  });

  it("canonicalizes object key order for hashing", async () => {
    const RecordSchema = z.record(z.string(), z.number());
    const { map, graph } = await createMapWith(RecordSchema as any, {
      graph: new Graph(),
    });

    const keyA = await map.set({ b: 2, a: 1 } as any);
    const keyB = await map.set({ a: 1, b: 2 } as any);

    expect(keyA).toBe(keyB);

    await map.poll();
    graph.step();
    expect(map.reactive.snapshot.size).toBe(1);
  });

  it("throws on poll if schema-invalid data is fetched from a changed leaf", async () => {
    const a = await createMapWith(NumberValueSchema, {
      graph: new Graph(),
      maxDepth: 1,
    });
    const b = await createMapWith(NumberValueSchema, {
      graph: new Graph(),
      maxDepth: 1,
    });

    const key = await a.map.set({ value: 700 });
    await b.map.poll();
    b.graph.step();

    const itemRows = await sql<{ leaf_path: string }[]>`
      SELECT leaf_path FROM ${sql(ITEMS_TABLE)} WHERE key = ${key}
    `;
    const leafPath = itemRows[0]!.leaf_path;

    await sql`
      UPDATE ${sql(ITEMS_TABLE)}
      SET data = '{"value":"bad"}'::jsonb
      WHERE key = ${key}
    `;
    await sql`
      UPDATE ${sql(NODES_TABLE)}
      SET mac = mac || '_tampered'
      WHERE path IN ('', ${leafPath})
    `;

    await expect(b.map.poll()).rejects.toThrow();
  });

});
