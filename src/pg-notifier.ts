import type { Sql } from "postgres";
import type { Graph } from "derivation";

export interface Pollable {
  poll(): Promise<void>;
}

export class PgNotifier {
  private unlisten: (() => Promise<void>) | null = null;
  private pollables: Pollable[] = [];

  constructor(
    private readonly sql: Sql,
    private readonly graph: Graph,
    private readonly channel: string = "derivation",
  ) {}

  register(pollable: Pollable): void {
    this.pollables.push(pollable);
  }

  async start(): Promise<void> {
    if (this.unlisten) return;

    const result = await this.sql.listen(this.channel, async () => {
      for (const pollable of this.pollables) {
        await pollable.poll();
      }
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
