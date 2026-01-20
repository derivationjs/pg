import { List } from "immutable";
import type { Sql } from "postgres";
import type { Graph, ReactiveLogSource, ReactiveLog } from "derivation";
import type { z } from "zod";

export interface LogRow<T> {
  seq: number;
  data: T;
}

interface RawRow {
  seq: number;
  data: unknown;
}

function parseRow<T>(raw: RawRow, schema: z.ZodType<T>): LogRow<T> {
  const seq = Number(raw.seq);
  const jsonData =
    typeof raw.data === "string" ? JSON.parse(raw.data) : raw.data;
  const data = schema.parse(jsonData);
  return { seq, data };
}

export class PgLog<T> {
  private readonly log: ReactiveLogSource<LogRow<T>>;

  private constructor(
    private readonly sql: Sql,
    private readonly table: string,
    private readonly schema: z.ZodType<T>,
    log: ReactiveLogSource<LogRow<T>>,
  ) {
    this.log = log;
  }

  static async create<T>(
    sql: Sql,
    table: string,
    graph: Graph,
    schema: z.ZodType<T>,
  ): Promise<PgLog<T>> {
    const rawRows = await sql<RawRow[]>`
      SELECT seq, data FROM ${sql(table)} ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, schema));
    const snapshot = List(rows);
    const log = graph.inputLog<LogRow<T>>(snapshot);
    return new PgLog<T>(sql, table, schema, log);
  }

  async append(data: T): Promise<void> {
    await this.appendAll([data]);
  }

  async appendAll(items: T[]): Promise<void> {
    if (items.length === 0) return;

    const validated = items.map((item) => this.schema.parse(item));

    await this.sql<RawRow[]>`
      INSERT INTO ${this.sql(this.table)} (data)
      VALUES ${this.sql(validated.map((data) => [JSON.stringify(data)]))}
    `;
  }

  async poll(): Promise<void> {
    const lastSeq = this.log.snapshot.last()?.seq ?? 0;
    const rawRows = await this.sql<RawRow[]>`
      SELECT seq, data FROM ${this.sql(this.table)}
      WHERE seq > ${lastSeq}
      ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, this.schema));
    console.log(`📊 Poll: fetched ${rows.length} new rows`);
    this.log.pushAll(List(rows));
  }

  get asLog(): ReactiveLog<LogRow<T>> {
    return this.log;
  }
}
