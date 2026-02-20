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

const SERIALIZABLE_WRITE_MAX_RETRIES = 5;

export interface LogRow<T> {
  seq: bigint;
  data: T;
}

interface RawRow {
  seq: number | string | bigint;
  data: unknown;
}

function parseRow<T>(raw: RawRow, schema: z.ZodType<T>): LogRow<T> {
  const seq = BigInt(raw.seq);
  const jsonData =
    typeof raw.data === "string" ? JSON.parse(raw.data) : raw.data;
  const data = schema.parse(jsonData);
  return { seq, data };
}

function isSerializationFailure(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as any).code === "40001"
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class PgLog<T> {
  private readonly logInput: LogChangeInput<LogRow<T>>;
  private readonly reactiveLog: Reactive<Log<LogRow<T>>>;
  private polledThroughSeq: bigint;

  private constructor(
    private readonly sql: Sql,
    private readonly table: string,
    private readonly stateTable: string,
    private readonly schema: z.ZodType<T>,
    polledThroughSeq: bigint,
    logInput: LogChangeInput<LogRow<T>>,
    reactiveLog: Reactive<Log<LogRow<T>>>,
  ) {
    this.logInput = logInput;
    this.reactiveLog = reactiveLog;
    this.polledThroughSeq = polledThroughSeq;
  }

  static async create<T>(
    sql: Sql,
    table: string,
    graph: Graph,
    schema: z.ZodType<T>,
  ): Promise<PgLog<T>> {
    const stateTable = PgLog.stateTableName(table);
    await PgLog.ensureStateTable(sql, table, stateTable);

    const rawRows = await sql<RawRow[]>`
      SELECT seq, data FROM ${sql(table)} ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, schema));
    const immutableList = List(rows);
    const initialLog = new Log(immutableList);

    const logInput = new LogChangeInput<LogRow<T>>(graph);
    const reactiveLog = Reactive.create<Log<LogRow<T>>>(
      graph,
      new LogOperations<LogRow<T>>(),
      logInput,
      initialLog,
    );

    const polledThroughSeq = rows.at(-1)?.seq ?? 0n;
    return new PgLog<T>(
      sql,
      table,
      stateTable,
      schema,
      polledThroughSeq,
      logInput,
      reactiveLog,
    );
  }

  async append(data: T): Promise<void> {
    await this.appendAll([data]);
  }

  async appendAll(items: T[]): Promise<void> {
    if (items.length === 0) return;

    const validated = items.map((item) => this.schema.parse(item));
    const rowCount = BigInt(validated.length);

    await this.withSerializableWriteRetry(async (tx: any) => {
      const stateRows = await tx<{ start_seq: string | number | bigint }[]>`
        UPDATE ${this.sql(this.stateTable)}
        SET next_seq = next_seq + ${rowCount}
        WHERE singleton = true
        RETURNING next_seq - ${rowCount} AS start_seq
      `;
      const startSeq = BigInt(stateRows[0]!.start_seq);
      await tx`
        INSERT INTO ${this.sql(this.table)} (seq, data)
        VALUES ${this.sql(
          validated.map((data, index) => [
            (startSeq + BigInt(index)).toString(),
            JSON.stringify(data),
          ]),
        )}
      `;
    });
  }

  async poll(): Promise<void> {
    const lastSeq = this.polledThroughSeq;
    const lastSeqParam = lastSeq.toString();
    const rawRows = await this.sql<RawRow[]>`
      SELECT seq, data FROM ${this.sql(this.table)}
      WHERE seq > ${lastSeqParam}
      ORDER BY seq ASC
    `;
    const rows = rawRows.map((raw) => parseRow(raw, this.schema));
    console.log(`📊 Poll: fetched ${rows.length} new rows`);
    if (rows.length > 0) {
      this.polledThroughSeq = rows[rows.length - 1]!.seq;
    }
    this.logInput.addAll(rows);
  }

  get reactive() {
    return this.reactiveLog;
  }

  get graph() {
    return this.logInput.graph;
  }

  private static stateTableName(table: string): string {
    return `${table}__state`;
  }

  private static async ensureStateTable(
    sql: Sql,
    logTable: string,
    stateTable: string,
  ): Promise<void> {
    await sql`
      CREATE TABLE IF NOT EXISTS ${sql(stateTable)} (
        singleton BOOLEAN PRIMARY KEY,
        next_seq BIGINT NOT NULL
      )
    `;
    await sql`
      INSERT INTO ${sql(stateTable)} (singleton, next_seq)
      VALUES (true, 1)
      ON CONFLICT (singleton) DO NOTHING
    `;
    const maxRows = await sql<{ max_seq: string | number | bigint | null }[]>`
      SELECT MAX(seq) AS max_seq
      FROM ${sql(logTable)}
    `;
    const nextSeq = (maxRows[0]?.max_seq === null ? 0n : BigInt(maxRows[0]!.max_seq!)) + 1n;
    const nextSeqParam = nextSeq.toString();
    await sql`
      UPDATE ${sql(stateTable)}
      SET next_seq = GREATEST(next_seq, ${nextSeqParam})
      WHERE singleton = true
    `;
  }

  private async withSerializableWriteRetry<R>(fn: (tx: any) => Promise<R>): Promise<R> {
    for (let attempt = 0; attempt < SERIALIZABLE_WRITE_MAX_RETRIES; attempt++) {
      try {
        return (await this.sql.begin("isolation level serializable", fn as any)) as R;
      } catch (error) {
        if (!isSerializationFailure(error) || attempt === SERIALIZABLE_WRITE_MAX_RETRIES - 1) {
          throw error;
        }
        await sleep(5 * (attempt + 1) + Math.floor(Math.random() * 10));
      }
    }
    throw new Error("unreachable");
  }
}
