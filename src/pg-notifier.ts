import type { Sql } from "postgres";
import type { Graph } from "derivation";

export class PgNotifier {
  private unlisten: (() => Promise<void>) | null = null;

  constructor(
    private readonly sql: Sql,
    private readonly graph: Graph,
    private readonly channel: string = "derivation",
  ) {}

  async start(): Promise<void> {
    if (this.unlisten) return;

    const result = await this.sql.listen(this.channel, () => {
      this.graph.step();
    });
    this.unlisten = result.unlisten;
  }

  async notify(): Promise<void> {
    await this.sql.notify(this.channel, "");
  }

  async stop(): Promise<void> {
    if (!this.unlisten) return;
    await this.unlisten();
    this.unlisten = null;
  }
}
