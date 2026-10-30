import type { DecayProfileBinding, PromotionPolicyDef } from "./api";

export type PolicyScope = "NODE" | "EDGE";

const IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function quotePolicyName(name: string): string {
  if (IDENTIFIER_PATTERN.test(name)) return name;
  if (!name.includes("'")) return `'${name}'`;
  if (!name.includes('"')) return `"${name}"`;
  throw new Error(
    "Policy names containing both quote styles cannot be edited.",
  );
}

export function targetValue(
  labels: string[] | undefined,
  edgeType: string | undefined,
  isWildcard: boolean,
): string {
  if (isWildcard) return "*";
  if (edgeType) return edgeType;
  return (labels ?? []).join(":");
}

export function validateTarget(value: unknown, scope: PolicyScope): boolean {
  const target = String(value ?? "").trim();
  if (scope === "EDGE") return IDENTIFIER_PATTERN.test(target);
  if (target === "*") return true;
  const labels = target.split(/[:,\s]+/).filter(Boolean);
  return (
    labels.length > 0 && labels.every((label) => IDENTIFIER_PATTERN.test(label))
  );
}

export function buildTarget(value: unknown, scope: PolicyScope): string {
  const target = String(value ?? "").trim();
  if (!validateTarget(target, scope)) {
    throw new Error(
      scope === "EDGE"
        ? "Enter one valid edge type."
        : "Enter labels separated by colons, or * for all nodes.",
    );
  }
  if (scope === "EDGE") return `()-[r:${target}]-()`;
  if (target === "*") return "()";
  return `(n:${target
    .split(/[:,\s]+/)
    .filter(Boolean)
    .join(":")})`;
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
  return `'${String(value).replace(/'/g, "\\'")}'`;
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
