import { List } from "immutable";
import type { Sql } from "postgres";
import type { Graph, ReactiveLogSource, ReactiveValue } from "derivation";
import type { PgNotifier } from "./pg-notifier.js";
import type { z } from "zod";

export interface LogRow<T> {
  seq: number;
  data: T;
}

interface RawRow {
  seq: string;
  data: unknown;
}

function parseRow<T>(raw: RawRow, schema: z.ZodType<T>): LogRow<T> {
  const seq = Number(raw.seq);
  const jsonData = typeof raw.data === "string" ? JSON.parse(raw.data) : raw.data;
  const data = schema.parse(jsonData);
  return { seq, data };
}

export class PgLog<T> {
  private readonly log: ReactiveLogSource<LogRow<T>>;

  private constructor(
    private readonly sql: Sql,
    private readonly table: string,
    private readonly notifier: PgNotifier,
    private readonly schema: z.ZodType<T>,
    log: ReactiveLogSource<LogRow<T>>,
  ) {
    this.log = log;
  }

  static async create<T>(
    sql: Sql,
    table: string,
    graph: Graph,
    notifier: PgNotifier,
    schema: z.ZodType<T>,
  ): Promise<PgLog<T>> {
    const rawRows = await sql<RawRow[]>`
      SELECT seq, data FROM ${sql(table)} ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, schema));
    const snapshot = List(rows);
    const log = graph.inputLog<LogRow<T>>(snapshot);
    return new PgLog<T>(sql, table, notifier, schema, log);
  }

  async append(data: T): Promise<LogRow<T>> {
    // Validate input before inserting
    const validated = this.schema.parse(data);

    const [rawRow] = await this.sql<RawRow[]>`
      INSERT INTO ${this.sql(this.table)} (data)
      VALUES (${JSON.stringify(validated)}::jsonb)
      RETURNING seq, data
    `;
    if (!rawRow) {
      throw new Error("Insert failed");
    }
    const row = parseRow(rawRow, this.schema);
    await this.notifier.notify();
    return row;
  }

  async appendAll(items: T[]): Promise<LogRow<T>[]> {
    if (items.length === 0) return [];

    // Validate all inputs before inserting
    const validated = items.map((item) => this.schema.parse(item));

    const rawRows = await this.sql<RawRow[]>`
      INSERT INTO ${this.sql(this.table)} (data)
      VALUES ${this.sql(validated.map(data => [JSON.stringify(data)]))}
      RETURNING seq, data
    `;
    const rows = rawRows.map((raw) => parseRow(raw, this.schema));
    await this.notifier.notify();
    return rows;
  }

  async poll(): Promise<void> {
    const localCount = this.log.length.value;
    const [result] = await this.sql<[{ count: string }]>`
      SELECT COUNT(*)::text as count FROM ${this.sql(this.table)}
    `;
    const tableCount = Number(result?.count ?? 0);

    if (tableCount <= localCount) return;

    const diff = tableCount - localCount;
    const rawRows = await this.sql<RawRow[]>`
      SELECT seq, data FROM ${this.sql(this.table)}
      ORDER BY seq DESC
      LIMIT ${diff}
    `;
    // Reverse to get ascending order, validate each row
    const rows = rawRows.reverse().map((raw) => parseRow(raw, this.schema));
    this.log.pushAll(List(rows));
  }

  get snapshot(): List<LogRow<T>> {
    return this.log.snapshot;
  }

  get changes(): ReactiveValue<List<LogRow<T>>> {
    return this.log.changes;
  }

  get materialized(): ReactiveValue<List<LogRow<T>>> {
    return this.log.materialized;
  }

  get length(): ReactiveValue<number> {
    return this.log.length;
  }

  fold<S>(initial: S, reducer: (acc: S, item: LogRow<T>) => S): ReactiveValue<S> {
    return this.log.fold(initial, reducer);
  }
}
