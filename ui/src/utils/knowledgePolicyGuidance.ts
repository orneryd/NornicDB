import type { DecayProfileBundle } from "./api";
import {
  buildTarget,
  optionLiteral,
  quotePolicyName,
  type PolicyScope,
} from "./knowledgePolicyEditing.ts";
export function decayBaseline(
  profile: Pick<
    DecayProfileBundle,
    "HalfLifeSeconds" | "Function" | "ScoreFloor" | "DecayEnabled"
  >,
  ageSeconds: number,
) {
  if (!profile.DecayEnabled || profile.Function === "none") return 1;
  const halfLife = Math.abs(profile.HalfLifeSeconds),
    age = Math.max(0, ageSeconds);
  let score =
    halfLife === 0
      ? 1
      : profile.Function === "linear"
        ? Math.max(0, 1 - age / (2 * halfLife))
        : profile.Function === "step"
          ? age < halfLife
            ? 1
            : 0
          : 2 ** (-age / halfLife);
  if (profile.HalfLifeSeconds < 0) score = 1 - score;
  return Math.max(profile.ScoreFloor, score);
}
export function newDecayProfileStatement(profile: DecayProfileBundle) {
  if (!profile.Name.trim()) throw Error("Enter a profile name.");
  if (!["exponential", "linear", "step", "none"].includes(profile.Function))
    throw Error("Choose a supported curve.");
  if (
    !Number.isFinite(profile.HalfLifeSeconds) ||
    (profile.Function !== "none" && profile.HalfLifeSeconds === 0)
  )
    throw Error("Half-life must be greater than zero.");
  if (
    ![profile.ScoreFloor, profile.VisibilityThreshold].every(
      (value) => Number.isFinite(value) && value >= 0 && value <= 1,
    )
  )
    throw Error("Floor and visibility threshold must be between 0 and 1.");
  const options = {
    halfLifeSeconds: profile.HalfLifeSeconds,
    function: profile.Function,
    scope: profile.Scope,
    scoreFrom: profile.ScoreFrom,
    visibilityThreshold: profile.VisibilityThreshold,
    scoreFloor: profile.ScoreFloor,
    decayEnabled: profile.DecayEnabled,
  };
  return `CREATE DECAY PROFILE ${quotePolicyName(profile.Name)} OPTIONS { ${Object.entries(
    options,
  )
    .map(([key, value]) => `${key}: ${optionLiteral(value)}`)
    .join(", ")} }`;
}
export function newDecayBindingStatement(
  name: string,
  scope: PolicyScope,
  target: string,
  profile: string,
) {
  if (!name.trim()) throw Error("Enter a binding name.");
  if (!profile.trim()) throw Error("Choose a profile.");
  return `CREATE DECAY PROFILE ${quotePolicyName(name)} FOR ${buildTarget(target, scope)} APPLY { DECAY PROFILE ${quotePolicyName(profile)} }`;
}
