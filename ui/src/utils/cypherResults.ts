import { identityRecord } from "./identityRecords";

// Result-column names are decoded data. Grid bookkeeping lives separately.
export function cypherResultRecord<T extends Record<string, unknown> = Record<string, unknown>>(
  columns: readonly string[],
  values: readonly unknown[],
): T {
  return identityRecord(columns.map((column, index) => [column, values[index]] as const)) as T;
}
