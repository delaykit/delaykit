const UNITS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const TOKEN_PATTERN = /(\d+)\s*(ms|s|m|h|d)/g;

/**
 * Parse a duration string like '5s', '30m', '24h', '7d', '500ms', or compound '1h30m'.
 * Returns milliseconds. Rejects malformed input (trailing text, unknown units).
 */
export function parseDuration(input: string): number {
  let total = 0;
  let lastIndex = 0;

  let match: RegExpExecArray | null;
  TOKEN_PATTERN.lastIndex = 0;

  while ((match = TOKEN_PATTERN.exec(input)) !== null) {
    if (match.index !== lastIndex) {
      throw new Error(
        `Invalid duration: "${input}". Unexpected text at position ${lastIndex}.`
      );
    }
    const value = parseInt(match[1], 10);
    const unit = match[2];
    total += value * UNITS[unit];
    lastIndex = TOKEN_PATTERN.lastIndex;
  }

  if (lastIndex === 0) {
    throw new Error(
      `Invalid duration: "${input}". Use a duration like "5s", "30m", "24h", "7d", or compound "1h30m".`
    );
  }

  if (lastIndex !== input.length) {
    throw new Error(
      `Invalid duration: "${input}". Unexpected text at position ${lastIndex}.`
    );
  }

  return total;
}

export function delayToDate(delay: string): Date {
  return new Date(Date.now() + parseDuration(delay));
}
