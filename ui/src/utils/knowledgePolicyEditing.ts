import type { DecayProfileBinding, PromotionPolicyDef } from "./api";
import { quoteName } from "./cypherNames.mjs";

export type PolicyScope = "NODE" | "EDGE";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function quotePolicyName(name: string): string {
  if (IDENTIFIER_PATTERN.test(name)) return name;
  return optionLiteral(name);
}

export function targetValue(
  labels: string[] | undefined,
  edgeType: string | undefined,
  isWildcard: boolean,
): string {
  if (isWildcard) return "*";
  if (edgeType) return quoteName(edgeType, "type");
  return (labels ?? []).map((label) => quoteName(label, "label")).join(":");
}

// The editor contains written target source. Keep validated name tokens whole;
// decoded API metadata enters this source representation through quoteName.
function targetTokens(value: unknown, scope: PolicyScope): string[] | null {
  const target = String(value ?? "").trim();
  if (scope === "NODE" && target === "*") return [];
  const token = /`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*/y;
  const parts: string[] = [];
  let offset = 0;
  while (offset < target.length) {
    token.lastIndex = offset;
    const match = token.exec(target);
    if (!match) return null;
    parts.push(match[0]);
    offset = token.lastIndex;
    if (offset === target.length) break;
    const separator = /^[:,\s]+/.exec(target.slice(offset));
    if (!separator) return null;
    offset += separator[0].length;
    if (offset === target.length) return null;
  }
  if (parts.length === 0 || (scope === "EDGE" && parts.length !== 1)) return null;
  return parts;
}

export function validateTarget(value: unknown, scope: PolicyScope): boolean {
  return targetTokens(value, scope) !== null;
}

export function buildTarget(value: unknown, scope: PolicyScope): string {
  const parts = targetTokens(value, scope);
  if (parts === null) {
    throw new Error(
      scope === "EDGE"
        ? "Enter one valid edge type."
        : "Enter labels separated by colons, or * for all nodes.",
    );
  }
  if (scope === "EDGE") return `()-[r:${parts[0]}]-()`;
  if (parts.length === 0) return "()";
  return `(n:${parts.join(":")})`;
}

export function buildDecayBindingAlter(
  binding: DecayProfileBinding & { Target: string },
): string {
  const scope: PolicyScope = binding.IsEdge ? "EDGE" : "NODE";
  const apply = binding.Apply.trim();
  if (!apply) throw new Error("The decay APPLY block cannot be empty.");
  return `ALTER DECAY PROFILE ${quotePolicyName(binding.Name)} FOR ${buildTarget(binding.Target, scope)} APPLY {\n${apply}\n}`;
}

export function buildPromotionPolicyAlter(
  policy: PromotionPolicyDef & { Target: string },
): string {
  const scope: PolicyScope = policy.IsEdge ? "EDGE" : "NODE";
  const apply = policy.Apply.trim();
  const alterTarget = `ALTER PROMOTION POLICY ${quotePolicyName(policy.Name)} FOR ${buildTarget(policy.Target, scope)}`;
  return apply ? `${alterTarget} APPLY {\n${apply}\n}` : alterTarget;
}

export function optionLiteral(value: unknown): string {
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return JSON.stringify(String(value));
}

export function buildProfileOptionAlter(
  kind: "DECAY" | "PROMOTION",
  name: string,
  field: string,
  value: unknown,
): string {
  return `ALTER ${kind} PROFILE ${quotePolicyName(name)} SET OPTIONS { ${field}: ${optionLiteral(value)} }`;
}

export function isFiniteNumber(value: unknown): boolean {
  return value !== "" && Number.isFinite(Number(value));
}

export function isUnitInterval(value: unknown): boolean {
  return isFiniteNumber(value) && Number(value) >= 0 && Number(value) <= 1;
}

export function isNonNegative(value: unknown): boolean {
  return isFiniteNumber(value) && Number(value) >= 0;
}
