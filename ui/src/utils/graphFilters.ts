// parseFilterList splits a user-entered filter string into trimmed entries.
// Entries are comma, whitespace, or newline separated; empty entries are
// dropped so a trailing separator never changes the request.
export function parseFilterList(text: string): string[] {
  return text
    .split(/[\s,]+/)
    .map((entry) => entry.trim())
    .filter((entry) => entry !== "");
}
