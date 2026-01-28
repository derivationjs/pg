import { List } from "immutable";
import type { Sql } from "postgres";
import type { Graph, ReactiveValue } from "derivation";
import {
  Reactive,
  Log,
  LogOperations,
  LogChangeInput,
  foldLog,
  lengthLog,
} from "@derivation/composable";
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
  private readonly logInput: LogChangeInput<LogRow<T>>;
  private readonly reactiveLog: Reactive<Log<LogRow<T>>>;

  private constructor(
    private readonly sql: Sql,
    private readonly table: string,
    private readonly schema: z.ZodType<T>,
    logInput: LogChangeInput<LogRow<T>>,
    reactiveLog: Reactive<Log<LogRow<T>>>,
  ) {
    this.logInput = logInput;
    this.reactiveLog = reactiveLog;
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
    const immutableList = List(rows);
    const initialLog = new Log(immutableList);

    const logInput = new LogChangeInput<LogRow<T>>(graph);
    const reactiveLog = Reactive.create(
      graph,
      new LogOperations<LogRow<T>>(),
      logInput,
      initialLog,
    );

    return new PgLog<T>(sql, table, schema, logInput, reactiveLog);
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
    const snapshot = this.reactiveLog.snapshot.toList();
    const lastSeq = snapshot.last()?.seq ?? 0;
    const rawRows = await this.sql<RawRow[]>`
      SELECT seq, data FROM ${this.sql(this.table)}
      WHERE seq > ${lastSeq}
      ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, this.schema));
    console.log(`📊 Poll: fetched ${rows.length} new rows`);
    this.logInput.pushAll(rows);
  }

  get asLog() {
    const reactiveLog = this.reactiveLog;
    const graph = this.logInput.graph;
    return {
      get snapshot() {
        return reactiveLog.snapshot.toList();
      },
      get length() {
        return lengthLog(graph, reactiveLog);
      },
      fold<S>(initial: S, reducer: (acc: S, item: LogRow<T>) => S): ReactiveValue<S> {
        return foldLog(graph, reactiveLog, initial, reducer);
      },
    };
  }
}
