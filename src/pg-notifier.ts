import type { Sql } from "postgres";

export class PgNotifier {
  private listen: Promise<void> | null = null;

  constructor(
    private readonly sql: Sql,
    private readonly channel: string = "step",
  ) {}

  wait(): Promise<void> {
    if (!this.listen) {
      this.listen = new Promise((resolve) =>
        this.sql.listen(this.channel, () => resolve()),
      );

      this.listen.then(() => {
        this.listen = null;
      });
    }
    return this.listen;
  }

  async notify(): Promise<void> {
    await this.sql.notify(this.channel, "");
  }
}
