export type LoginRateLimitScope = "ip" | "account";

export interface LoginRateLimitDecision {
  limited: boolean;
  retryAfterSeconds: number;
  scope?: LoginRateLimitScope;
}

export interface LoginRateLimiterOptions {
  windowMs: number;
  maxPerIp: number;
  maxPerAccount: number;
  maxTrackedIps: number;
  maxTrackedAccounts: number;
  now?: () => number;
}

interface WindowEntry {
  count: number;
  resetAt: number;
}

class BoundedFixedWindow {
  private readonly entries = new Map<string, WindowEntry>();

  constructor(
    private readonly windowMs: number,
    private readonly maximum: number,
    private readonly maxEntries: number,
    private readonly now: () => number,
  ) {}

  consume(key: string): LoginRateLimitDecision {
    const now = this.now();
    const existing = this.entries.get(key);
    if (existing && existing.resetAt > now) {
      if (existing.count >= this.maximum) {
        return this.limited(existing.resetAt, now);
      }
      existing.count += 1;
      return { limited: false, retryAfterSeconds: 0 };
    }
    if (existing) this.entries.delete(key);

    if (this.entries.size >= this.maxEntries) {
      this.removeExpired(now);
      if (this.entries.size >= this.maxEntries) {
        return this.limited(now + this.windowMs, now);
      }
    }

    this.entries.set(key, { count: 1, resetAt: now + this.windowMs });
    return { limited: false, retryAfterSeconds: 0 };
  }

  reset(key: string): void {
    this.entries.delete(key);
  }

  get size(): number {
    return this.entries.size;
  }

  private removeExpired(now: number): void {
    for (const [key, entry] of this.entries) {
      if (entry.resetAt <= now) this.entries.delete(key);
    }
  }

  private limited(resetAt: number, now: number): LoginRateLimitDecision {
    return {
      limited: true,
      retryAfterSeconds: Math.max(1, Math.ceil((resetAt - now) / 1_000)),
    };
  }
}

export class LoginRateLimiter {
  private readonly ips: BoundedFixedWindow;
  private readonly accounts: BoundedFixedWindow;

  constructor(options: LoginRateLimiterOptions) {
    const now = options.now ?? Date.now;
    this.ips = new BoundedFixedWindow(
      options.windowMs,
      options.maxPerIp,
      options.maxTrackedIps,
      now,
    );
    this.accounts = new BoundedFixedWindow(
      options.windowMs,
      options.maxPerAccount,
      options.maxTrackedAccounts,
      now,
    );
  }

  consume(sourceIp: string, account: string): LoginRateLimitDecision {
    const ipDecision = this.ips.consume(sourceIp);
    if (ipDecision.limited) return { ...ipDecision, scope: "ip" };

    const accountDecision = this.accounts.consume(account);
    if (accountDecision.limited) {
      return { ...accountDecision, scope: "account" };
    }
    return { limited: false, retryAfterSeconds: 0 };
  }

  resetAccount(account: string): void {
    this.accounts.reset(account);
  }

  get tracked(): { ips: number; accounts: number } {
    return { ips: this.ips.size, accounts: this.accounts.size };
  }
}

export function createDefaultLoginRateLimiter(): LoginRateLimiter {
  return new LoginRateLimiter({
    windowMs: 15 * 60 * 1_000,
    maxPerIp: 30,
    maxPerAccount: 5,
    maxTrackedIps: 10_000,
    maxTrackedAccounts: 10_000,
  });
}
