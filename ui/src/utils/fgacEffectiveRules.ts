import type { FgacDefinition } from "./fgacCatalog";

/** Effective rules returned for the signed-in account. */
export interface FgacEffectiveRules {
  enforced: boolean;
  rules: FgacDefinition[] | null;
  homeDatabaseId?: string;
}

/** Check a concrete operation for a UI control. Backend decisions are authoritative. */
export function permitsFgacAction(effective: FgacEffectiveRules, action: string, databaseId?: string, property?: string): boolean {
  let granted = false;
  for (const rule of effective.rules ?? []) {
    const scope = rule.scope;
    if (rule.action !== action || scope.kind === "node" || scope.kind === "relationship") continue;
    if (scope.kind === "database" && !databaseId) continue;
    if (scope.databaseId && scope.databaseId !== databaseId) continue;
    if (scope.homeDatabase && effective.homeDatabaseId !== databaseId) continue;
    if (scope.properties?.length && (!property || !scope.properties.includes(property))) continue;
    if (scope.propertyPatterns?.length || scope.predicate || scope.entityIds?.length) continue;
    if (rule.deny) return false;
    granted = true;
  }
  return granted;
}

/** Navigation can advertise a view that contains at least one delegated database operation. */
export function hasFgacActionFamily(effective: FgacEffectiveRules, prefix: string): boolean {
  return (effective.rules ?? []).some(rule => !rule.deny && rule.action.startsWith(prefix));
}
