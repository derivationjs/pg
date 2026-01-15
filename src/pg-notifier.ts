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

    console.log(`👂 Starting listener on channel "${this.channel}"`);
    const result = await this.sql.listen(this.channel, async () => {
      console.log("📬 Received notification, polling...");
      for (const pollable of this.pollables) {
        await pollable.poll();
      }
      console.log("📬 Polling complete, stepping graph...");
      this.graph.step();
      console.log("📬 Graph stepped");
    });
    this.unlisten = result.unlisten;
    console.log(`👂 Listener started`);
  }

  async notify(): Promise<void> {
    console.log(`📤 Sending notification on channel "${this.channel}"`);
    await this.sql.notify(this.channel, "");
    console.log(`📤 Notification sent`);
  }

  async stop(): Promise<void> {
    if (!this.unlisten) return;
    await this.unlisten();
    this.unlisten = null;
  }
}
