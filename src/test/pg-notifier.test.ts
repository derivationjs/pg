import { describe, expect, it, vi } from "vitest";
import { PgNotifier } from "../pg-notifier.js";

describe("PgNotifier", () => {
  it("coalesces concurrent waits onto one listener and resets after notification", async () => {
    let callback: (() => void) | undefined;
    const sql = {
      listen: vi.fn((channel: string, cb: () => void) => {
        callback = cb;
        return Promise.resolve();
      }),
      notify: vi.fn(),
    } as any;

    const notifier = new PgNotifier(sql);

    const p1 = notifier.wait();
    const p2 = notifier.wait();

    expect(p1).toBe(p2);
    expect(sql.listen).toHaveBeenCalledTimes(1);
    expect(sql.listen).toHaveBeenCalledWith("step", expect.any(Function));

    callback!();
    await p1;
    await Promise.resolve();

    const p3 = notifier.wait();
    expect(p3).not.toBe(p1);
    expect(sql.listen).toHaveBeenCalledTimes(2);
  });

  it("uses custom channel for wait and notify", async () => {
    const sql = {
      listen: vi.fn((_channel: string, _cb: () => void) => Promise.resolve()),
      notify: vi.fn(() => Promise.resolve()),
    } as any;

    const notifier = new PgNotifier(sql, "custom_channel");

    void notifier.wait();
    expect(sql.listen).toHaveBeenCalledWith("custom_channel", expect.any(Function));

    await notifier.notify();
    expect(sql.notify).toHaveBeenCalledWith("custom_channel", "");
  });
});
