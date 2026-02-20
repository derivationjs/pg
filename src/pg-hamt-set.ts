import { createHash } from "node:crypto";
import { Map as IMap } from "immutable";
import type { Sql } from "postgres";
import type { Graph } from "derivation";
import {
  Cell,
  CellOperations,
  ChangeInput,
  MapOperations,
  Reactive,
  type MapCommand,
} from "@derivation/composable";
import type { z } from "zod";

const FANOUT = 32;
const ROOT_PATH = "";
const PATH_DIGITS = "0123456789abcdefghijklmnopqrstuv";
const MAX_DEPTH_SUPPORTED_BY_SHA256 = 51;
const SERIALIZABLE_WRITE_MAX_RETRIES = 5;

export interface HamtMapOptions {
  maxDepth?: number;
}

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

function createJsonMapOps<T extends NonNullable<unknown>>() {
  return new MapOperations<string, Cell<T>>(new CellOperations<T>() as any);
}

interface NodeSummary {
  path: string;
  level: number;
  count: number;
  bitmap: bigint;
  mac: string;
}

interface RawNodeRow {
  path: string;
  level: number;
  count: number;
  bitmap: number | string;
  mac: string;
}

interface RawItemRow {
  key: string;
  data: unknown;
  item_mac: string;
  leaf_path: string;
}

interface ParsedItemRow<T> {
  key: string;
  data: T;
  itemMac: string;
  leafPath: string;
}

function parseNodeRow(row: RawNodeRow): NodeSummary {
  return {
    path: row.path,
    level: Number(row.level),
    count: Number(row.count),
    bitmap: BigInt(row.bitmap),
    mac: row.mac,
  };
}

function canonicalStringify(value: unknown): string {
  if (value === null) return "null";
  const t = typeof value;
  if (t === "string") return JSON.stringify(value);
  if (t === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Cannot canonicalize non-finite number");
    }
    return JSON.stringify(value);
  }
  if (t === "boolean") return value ? "true" : "false";
  if (Array.isArray(value)) {
    return `[${value.map((x) => canonicalStringify(x)).join(",")}]`;
  }
  if (t === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return `{${keys
      .map((k) => `${JSON.stringify(k)}:${canonicalStringify(obj[k])}`)
      .join(",")}}`;
  }
  throw new Error(`Unsupported value in canonicalStringify: ${t}`);
}

function frameHashParts(parts: Array<string | Buffer>): Buffer {
  const chunks: Buffer[] = [];
  const count = Buffer.allocUnsafe(8);
  count.writeBigUInt64BE(BigInt(parts.length), 0);
  chunks.push(count);
  for (const part of parts) {
    const bytes = typeof part === "string" ? Buffer.from(part, "utf8") : part;
    const len = Buffer.allocUnsafe(8);
    len.writeBigUInt64BE(BigInt(bytes.length), 0);
    chunks.push(len, bytes);
  }
  return Buffer.concat(chunks);
}

function hashPartsHex(parts: Array<string | Buffer>): string {
  return createHash("sha256").update(frameHashParts(parts)).digest("hex");
}

function hashCanonicalHex(domain: string, payload: unknown): string {
  return hashPartsHex([domain, canonicalStringify(payload)]);
}

function pathDigestBytes(itemKey: string): Buffer {
  return createHash("sha256").update(frameHashParts(["path:v1", itemKey])).digest();
}

function bitsToPath(bytes: Buffer, depth: number): string {
  let path = "";
  for (let i = 0; i < depth; i++) {
    let value = 0;
    const startBit = i * 5;
    for (let bit = 0; bit < 5; bit++) {
      const bitIndex = startBit + bit;
      const byteIndex = bitIndex >> 3;
      const bitInByte = 7 - (bitIndex & 7);
      const bitValue = (bytes[byteIndex]! >> bitInByte) & 1;
      value = (value << 1) | bitValue;
    }
    path += PATH_DIGITS[value]!;
  }
  return path;
}

function childPath(parentPath: string, childIndex: number): string {
  return `${parentPath}${PATH_DIGITS[childIndex]}`;
}

function childIndexFromPath(path: string): number {
  const ch = path[path.length - 1];
  if (ch === undefined) {
    throw new Error("Root path does not have a child index");
  }
  const idx = PATH_DIGITS.indexOf(ch);
  if (idx < 0) {
    throw new Error(`Invalid path digit: ${ch}`);
  }
  return idx;
}

function sameSummary(a: NodeSummary | null | undefined, b: NodeSummary | null | undefined): boolean {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return a.count === b.count && a.mac === b.mac && a.bitmap === b.bitmap;
}

function parseItemRow<T>(
  row: RawItemRow,
  schema: z.ZodType<T>,
): ParsedItemRow<T> {
  const raw = typeof row.data === "string" ? JSON.parse(row.data) : row.data;
  return {
    key: row.key,
    data: schema.parse(raw),
    itemMac: row.item_mac,
    leafPath: row.leaf_path,
  };
}

function replaceMapContents<K, V>(target: Map<K, V>, source: Map<K, V>): void {
  target.clear();
  for (const [key, value] of source) {
    target.set(key, value);
  }
}

function isSerializationFailure(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && (error as any).code === "40001";
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type LeafItems<T extends NonNullable<unknown>> = Map<string, Cell<T>>;

export class PgHamtMap<T extends Exclude<JsonValue, null> = Exclude<JsonValue, null>> {
  private readonly changeInput: ChangeInput<IMap<string, Cell<T>>>;
  private readonly reactiveMap: Reactive<IMap<string, Cell<T>>>;
  private readonly localNodeCache: Map<string, NodeSummary>;
  private readonly localLeafItems: Map<string, LeafItems<T>>;

  private constructor(
    private readonly sql: Sql,
    private readonly itemsTable: string,
    private readonly nodesTable: string,
    private readonly schema: z.ZodType<T>,
    private readonly maxDepth: number,
    changeInput: ChangeInput<IMap<string, Cell<T>>>,
    reactiveMap: Reactive<IMap<string, Cell<T>>>,
    localNodeCache: Map<string, NodeSummary>,
    localLeafItems: Map<string, LeafItems<T>>,
  ) {
    this.changeInput = changeInput;
    this.reactiveMap = reactiveMap;
    this.localNodeCache = localNodeCache;
    this.localLeafItems = localLeafItems;
  }

  static async create<T extends Exclude<JsonValue, null>>(
    sql: Sql,
    itemsTable: string,
    nodesTable: string,
    graph: Graph,
    schema: z.ZodType<T>,
    options: HamtMapOptions,
  ): Promise<PgHamtMap<T>> {
    const maxDepth = options.maxDepth ?? 8;
    if (maxDepth < 1) {
      throw new Error("maxDepth must be >= 1");
    }
    if (maxDepth > MAX_DEPTH_SUPPORTED_BY_SHA256) {
      throw new Error(
        `maxDepth must be <= ${MAX_DEPTH_SUPPORTED_BY_SHA256} for HMAC-SHA-256 path hashing`,
      );
    }

    await PgHamtMap.ensureRootNode(sql, nodesTable);

    const rawItems = await sql<RawItemRow[]>`
      SELECT key, data, item_mac, leaf_path
      FROM ${sql(itemsTable)}
      ORDER BY key ASC
    `;
    let initialMap = IMap<string, Cell<T>>();
    const localLeafItems = new Map<string, LeafItems<T>>();
    for (const rawItem of rawItems) {
      const item = parseItemRow(rawItem, schema);
      const expectedKey = PgHamtMap.hashKeyForValue(item.data);
      if (item.key !== expectedKey) {
        throw new Error(`Stored key mismatch for value: ${item.key} !== ${expectedKey}`);
      }
      const expectedLeaf = PgHamtMap.leafPathForKey(
        maxDepth,
        item.key,
      );
      if (item.leafPath !== expectedLeaf) {
        throw new Error(
          `Stored leaf_path mismatch for key ${item.key}: ${item.leafPath} !== ${expectedLeaf}`,
        );
      }
      const expectedMac = PgHamtMap.itemMac(item.key, item.data);
      if (item.itemMac !== expectedMac) {
        throw new Error(`Stored item_mac mismatch for key ${item.key}`);
      }
      initialMap = initialMap.set(item.key, new Cell(item.data));
      const leaf = localLeafItems.get(item.leafPath) ?? new Map<string, Cell<T>>();
      leaf.set(item.key, new Cell(item.data));
      localLeafItems.set(item.leafPath, leaf);
    }

    const rawNodes = await sql<RawNodeRow[]>`
      SELECT path, level, count, bitmap, mac
      FROM ${sql(nodesTable)}
    `;
    const localNodeCache = new Map<string, NodeSummary>();
    for (const rawNode of rawNodes) {
      const node = parseNodeRow(rawNode);
      localNodeCache.set(node.path, node);
    }

    const changeInput = new ChangeInput<IMap<string, Cell<T>>>(
      graph,
      createJsonMapOps<T>(),
    );
    const reactiveMap = Reactive.create<IMap<string, Cell<T>>>(
      graph,
      createJsonMapOps<T>(),
      changeInput,
      initialMap,
    );

    return new PgHamtMap<T>(
      sql,
      itemsTable,
      nodesTable,
      schema,
      maxDepth,
      changeInput,
      reactiveMap,
      localNodeCache,
      localLeafItems,
    );
  }

  async set(value: T): Promise<string> {
    const [key] = await this.setAll([value]);
    return key!;
  }

  async setAll(values: Iterable<T>): Promise<string[]> {
    const validated: Array<{ key: string; data: T; itemMac: string; leafPath: string }> = [];
    for (const input of values) {
      const data = this.schema.parse(input);
      const key = this.hashKeyForValue(data);
      const leafPath = this.leafPathForKey(key);
      validated.push({
        key,
        data,
        itemMac: this.itemMac(key, data),
        leafPath,
      });
    }
    if (validated.length === 0) return [];

    const byLeaf = new Map<string, Array<{ key: string; data: T; itemMac: string; leafPath: string }>>();
    for (const item of validated) {
      const group = byLeaf.get(item.leafPath) ?? [];
      group.push(item);
      byLeaf.set(item.leafPath, group);
    }

    await this.withSerializableWriteRetry(async (tx: any) => {
      for (const item of validated) {
        await tx`
          INSERT INTO ${this.sql(this.itemsTable)} (key, data, item_mac, leaf_path)
          VALUES (${item.key}, ${JSON.stringify(item.data)}::jsonb, ${item.itemMac}, ${item.leafPath})
          ON CONFLICT (key) DO UPDATE SET
            data = EXCLUDED.data,
            item_mac = EXCLUDED.item_mac,
            leaf_path = EXCLUDED.leaf_path
        `;
      }
      for (const leafPath of byLeaf.keys()) {
        await this.recomputePath(tx, leafPath);
      }
    });

    return validated.map((item) => item.key);
  }

  async removeByHash(key: string): Promise<boolean> {
    const leafPath = this.leafPathForKey(key);
    const deleted = await this.withSerializableWriteRetry(async (tx: any) => {
      const rows = await tx<{ key: string }[]>`
        DELETE FROM ${this.sql(this.itemsTable)}
        WHERE key = ${key}
        RETURNING key
      `;
      if (rows.length === 0) {
        return false;
      }
      await this.recomputePath(tx, leafPath);
      return true;
    });
    return deleted;
  }

  async poll(): Promise<void> {
    const result = await this.sql.begin(
      "isolation level serializable read only deferrable",
      async (tx: any) => {
        const dbRoot = await this.fetchNode(tx, ROOT_PATH, 0);
        const localRoot =
          this.localNodeCache.get(ROOT_PATH) ?? this.emptyBranchSummary(ROOT_PATH, 0);
        if (sameSummary(dbRoot, localRoot)) {
          return null;
        }

        const nextNodeCache = new Map(this.localNodeCache);
        const nextLeafItems = new Map(this.localLeafItems);
        const commands: MapCommand<string, Cell<T>>[] = [];
        await this.syncNode(
          tx,
          ROOT_PATH,
          0,
          dbRoot,
          commands,
          nextNodeCache,
          nextLeafItems,
        );
        return { commands, nextNodeCache, nextLeafItems };
      },
    );
    if (!result) {
      return;
    }
    if (result.commands.length > 0) {
      this.changeInput.push(result.commands);
    }
    replaceMapContents(this.localNodeCache, result.nextNodeCache);
    replaceMapContents(this.localLeafItems, result.nextLeafItems);
  }

  get reactive() {
    return this.reactiveMap;
  }

  get graph() {
    return this.changeInput.graph;
  }

  private leafPathForKey(key: string): string {
    return PgHamtMap.leafPathForKey(this.maxDepth, key);
  }

  private hashKeyForValue(value: T): string {
    return PgHamtMap.hashKeyForValue(value);
  }

  private itemMac(key: string, data: T): string {
    return PgHamtMap.itemMac(key, data);
  }

  private emptyLeafSummary(path: string, level: number): NodeSummary {
    return {
      path,
      level,
      count: 0,
      bitmap: 0n,
      mac: hashCanonicalHex("leaf:v1", { path, items: [] as string[] }),
    };
  }

  private emptyBranchSummary(path: string, level: number): NodeSummary {
    return {
      path,
      level,
      count: 0,
      bitmap: 0n,
      mac: hashCanonicalHex("node:v1", { path, children: [] as unknown[] }),
    };
  }

  private static async ensureRootNode(
    sql: Sql,
    nodesTable: string,
  ): Promise<void> {
    const root = {
      path: ROOT_PATH,
      level: 0,
      bitmap: 0n,
      count: 0,
      mac: hashCanonicalHex("node:v1", { path: ROOT_PATH, children: [] as unknown[] }),
    };
    await sql`
      INSERT INTO ${sql(nodesTable)} (path, level, bitmap, count, mac)
      VALUES (${ROOT_PATH}, 0, ${root.bitmap.toString()}, ${root.count}, ${root.mac})
      ON CONFLICT (path) DO NOTHING
    `;
  }

  private static hashKeyForValue(value: unknown): string {
    return hashCanonicalHex("key:v1", value);
  }

  private static itemMac(key: string, data: unknown): string {
    return hashCanonicalHex("item:v1", { key, data });
  }

  private static leafPathForKey(
    maxDepth: number,
    key: string,
  ): string {
    const bytes = pathDigestBytes(key);
    return bitsToPath(bytes, maxDepth);
  }

  private async recomputePath(tx: any, leafPath: string): Promise<void> {
    for (let level = this.maxDepth; level >= 0; level--) {
      const path = leafPath.slice(0, level);
      if (level === this.maxDepth) {
        await this.recomputeLeafNode(tx, path, level);
      } else {
        await this.recomputeInternalNode(tx, path, level);
      }
    }
  }

  private async recomputeLeafNode(tx: any, path: string, level: number): Promise<void> {
    const rows = await tx<RawItemRow[]>`
      SELECT key, data, item_mac, leaf_path
      FROM ${this.sql(this.itemsTable)}
      WHERE leaf_path = ${path}
      ORDER BY item_mac ASC, key ASC
    `;

    if (rows.length === 0) {
      if (path === ROOT_PATH) {
        await this.upsertNode(tx, this.emptyLeafSummary(path, level));
      } else {
        await this.deleteNode(tx, path);
      }
      return;
    }

    const itemMacs = rows.map((row: RawItemRow) => row.item_mac);
    const summary: NodeSummary = {
      path,
      level,
      count: rows.length,
      bitmap: 0n,
      mac: hashCanonicalHex("leaf:v1", {
        path,
        items: itemMacs,
      }),
    };
    await this.upsertNode(tx, summary);
  }

  private async recomputeInternalNode(tx: any, path: string, level: number): Promise<void> {
    const rows = await this.fetchChildren(tx, path, level);
    if (rows.length === 0) {
      if (path === ROOT_PATH) {
        await this.upsertNode(tx, this.emptyBranchSummary(path, level));
      } else {
        await this.deleteNode(tx, path);
      }
      return;
    }

    let count = 0;
    let bitmap = 0n;
    const children = rows
      .map((row: RawNodeRow) => {
        const child = parseNodeRow(row);
        const slot = childIndexFromPath(child.path);
        count += child.count;
        bitmap |= 1n << BigInt(slot);
        return {
          slot,
          count: child.count,
          mac: child.mac,
        };
      })
      .sort((a, b) => a.slot - b.slot);

    const summary: NodeSummary = {
      path,
      level,
      count,
      bitmap,
      mac: hashCanonicalHex("node:v1", {
        path,
        children,
      }),
    };
    await this.upsertNode(tx, summary);
  }

  private async upsertNode(tx: any, summary: NodeSummary): Promise<void> {
    await tx`
      INSERT INTO ${this.sql(this.nodesTable)} (path, level, bitmap, count, mac)
      VALUES (
        ${summary.path},
        ${summary.level},
        ${summary.bitmap.toString()},
        ${summary.count},
        ${summary.mac}
      )
      ON CONFLICT (path) DO UPDATE SET
        level = EXCLUDED.level,
        bitmap = EXCLUDED.bitmap,
        count = EXCLUDED.count,
        mac = EXCLUDED.mac
    `;
  }

  private async deleteNode(tx: any, path: string): Promise<void> {
    await tx`
      DELETE FROM ${this.sql(this.nodesTable)}
      WHERE path = ${path}
    `;
  }

  private async fetchNode(sql: Sql, path: string, level: number): Promise<NodeSummary | null> {
    const rows = await sql<RawNodeRow[]>`
      SELECT path, level, bitmap, count, mac
      FROM ${sql(this.nodesTable)}
      WHERE path = ${path} AND level = ${level}
      LIMIT 1
    `;
    return rows[0] ? parseNodeRow(rows[0]) : null;
  }

  private async fetchChildren(sql: Sql, path: string, level: number): Promise<RawNodeRow[]> {
    const nextLevel = level + 1;
    const prefix = `${path}%`;
    const nextPathLength = path.length + 1;
    return sql<RawNodeRow[]>`
      SELECT path, level, bitmap, count, mac
      FROM ${sql(this.nodesTable)}
      WHERE level = ${nextLevel}
        AND path LIKE ${prefix}
        AND char_length(path) = ${nextPathLength}
      ORDER BY path ASC
    `;
  }

  private async fetchLeafItems(sql: Sql, leafPath: string): Promise<ParsedItemRow<T>[]> {
    const rows = await sql<RawItemRow[]>`
      SELECT key, data, item_mac, leaf_path
      FROM ${sql(this.itemsTable)}
      WHERE leaf_path = ${leafPath}
      ORDER BY key ASC
    `;
    return rows.map((row) => parseItemRow(row, this.schema));
  }

  private async syncNode(
    sql: Sql,
    path: string,
    level: number,
    dbNode: NodeSummary | null,
    commands: MapCommand<string, Cell<T>>[],
    nodeCache: Map<string, NodeSummary>,
    leafItems: Map<string, LeafItems<T>>,
  ): Promise<void> {
    const localNode =
      nodeCache.get(path) ??
      (level === this.maxDepth
        ? this.emptyLeafSummary(path, level)
        : this.emptyBranchSummary(path, level));

    if (sameSummary(dbNode, localNode)) {
      if (dbNode) {
        nodeCache.set(path, dbNode);
      } else {
        nodeCache.delete(path);
      }
      return;
    }

    if (level === this.maxDepth) {
      await this.reconcileLeaf(sql, path, dbNode, commands, leafItems);
      if (dbNode) {
        nodeCache.set(path, dbNode);
      } else {
        nodeCache.delete(path);
      }
      return;
    }

    const dbChildrenRows = dbNode ? await this.fetchChildren(sql, path, level) : [];
    const dbChildren = new Map<string, NodeSummary>();
    for (const row of dbChildrenRows) {
      const child = parseNodeRow(row);
      dbChildren.set(child.path, child);
    }

    for (let childIndex = 0; childIndex < FANOUT; childIndex++) {
      const child = childPath(path, childIndex);
      const dbChild = dbChildren.get(child) ?? null;
      const localChild = nodeCache.get(child) ?? null;
      if (sameSummary(dbChild, localChild)) {
        if (dbChild) nodeCache.set(child, dbChild);
        continue;
      }
      await this.syncNode(sql, child, level + 1, dbChild, commands, nodeCache, leafItems);
    }

    if (dbNode) {
      nodeCache.set(path, dbNode);
    } else {
      nodeCache.delete(path);
    }
  }

  private async reconcileLeaf(
    sql: Sql,
    leafPath: string,
    _dbNode: NodeSummary | null,
    commands: MapCommand<string, Cell<T>>[],
    leafItems: Map<string, LeafItems<T>>,
  ): Promise<void> {
    const dbItems = await this.fetchLeafItems(sql, leafPath);
    const dbLeafMap = new Map<string, Cell<T>>();
    for (const item of dbItems) {
      dbLeafMap.set(item.key, new Cell(item.data));
    }

    const localLeafMap = leafItems.get(leafPath) ?? new Map<string, Cell<T>>();

    for (const [key, localValue] of localLeafMap) {
      if (!dbLeafMap.has(key)) {
        commands.push({ type: "delete", key });
      } else {
        const dbValue = dbLeafMap.get(key)!;
        if (canonicalStringify(localValue.value) !== canonicalStringify(dbValue.value)) {
          commands.push({ type: "add", key, value: dbValue });
        }
      }
    }

    for (const [key, dbValue] of dbLeafMap) {
      if (!localLeafMap.has(key)) {
        commands.push({ type: "add", key, value: dbValue });
      }
    }

    if (dbLeafMap.size === 0) {
      leafItems.delete(leafPath);
    } else {
      leafItems.set(leafPath, dbLeafMap);
    }
  }

  private async withSerializableWriteRetry<R>(fn: (tx: any) => Promise<R>): Promise<R> {
    for (let attempt = 0; attempt < SERIALIZABLE_WRITE_MAX_RETRIES; attempt++) {
      try {
        return (await this.sql.begin("isolation level serializable", fn as any)) as R;
      } catch (error) {
        if (!isSerializationFailure(error) || attempt === SERIALIZABLE_WRITE_MAX_RETRIES - 1) {
          throw error;
        }
        // Small jittered backoff to reduce immediate retry collisions under contention.
        const backoffMs = 5 * (attempt + 1) + Math.floor(Math.random() * 10);
        await sleep(backoffMs);
      }
    }
    throw new Error("unreachable");
  }
}
