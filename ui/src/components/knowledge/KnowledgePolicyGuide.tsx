import { useEffect, useId, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  api,
  type DecayProfileBundle,
  type KPProfilesResponse,
} from "../../utils/api";
import {
  buildTarget,
  validateTarget,
  type PolicyScope,
} from "../../utils/knowledgePolicyEditing";
import {
  decayBaseline,
  newDecayBindingStatement,
  newDecayProfileStatement,
} from "../../utils/knowledgePolicyGuidance";
import { resultRecords } from "../../utils/schemaOperations";
import { useUrlState, useViewState } from "../../utils/useUrlState";
import { Button } from "../common/Button";
import { FormInput } from "../common/FormInput";
import { Alert } from "../common/Alert";
import { Disclosure } from "../common/Disclosure";
const selectClass =
  "block w-full mt-1 p-2 rounded border border-norse-rune bg-norse-stone text-white";
export function KnowledgePolicyGuide({
  database,
  profiles,
  saving,
  onApply,
}: {
  database: string;
  profiles: KPProfilesResponse | null;
  saving: boolean;
  onApply: (statement: string, message: string) => Promise<boolean>;
}) {
  const { patch } = useUrlState();
  const [step, setStep] = useViewState("kp.setup", "");
  const [scope, setScope] = useViewState<PolicyScope>("kp.scope", "NODE"),
    [target, setTarget] = useViewState("kp.target", ""),
    [selectedProfile, setSelectedProfile] = useViewState("kp.profile", "");
  const [mode, setMode] = useState<"existing" | "new">("existing"),
    [profileName, setProfileName] = useState(""),
    [bindingName, setBindingName] = useState("");
  const [curve, setCurve] = useState("exponential"),
    [direction, setDirection] = useState("forward"),
    [days, setDays] = useState("7"),
    [threshold, setThreshold] = useState("0.1"),
    [floor, setFloor] = useState("0"),
    [anchor, setAnchor] = useState("VERSION");
  const [choices, setChoices] = useState<{ labels: string[]; types: string[] }>(
      { labels: [], types: [] },
    ),
    [preview, setPreview] = useState<{
      signature: string;
      rows: Record<string, unknown>[];
    } | null>(null),
    [busy, setBusy] = useState(false),
    [applying, setApplying] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState("");
  const suggestionsId = useId(),
    signature = JSON.stringify([database, scope, target]);
  useEffect(() => {
    let current = true;
    Promise.all([
      api.executeCypherOverHttp(database, "CALL db.labels()"),
      api.executeCypherOverHttp(database, "CALL db.relationshipTypes()"),
    ])
      .then(([labels, types]) => {
        if (current)
          setChoices({
            labels: resultRecords(labels).map((row) =>
              String(Object.values(row)[0]),
            ),
            types: resultRecords(types).map((row) =>
              String(Object.values(row)[0]),
            ),
          });
      })
      .catch((e) => {
        if (current) setError(e.message);
      });
    return () => {
      current = false;
    };
  }, [database]);
  const existing =
    profiles?.bundles.filter(
      (profile) =>
        profile.Enabled &&
        (profile.Scope === scope || (!profile.Scope && scope === "NODE")),
    ) ?? [];
  const profile: DecayProfileBundle | undefined =
    mode === "existing"
      ? existing.find((profile) => profile.Name === selectedProfile)
      : {
          Name: profileName,
          Function: curve,
          HalfLifeSeconds:
            curve === "none"
              ? 0
              : Math.round(Number(days) * 86400) *
                (direction === "inverse" ? -1 : 1),
          VisibilityThreshold: Number(threshold),
          ScoreFloor: Number(floor),
          ScoreFrom: anchor,
          Scope: scope,
          DecayEnabled: curve !== "none",
          Enabled: true,
        };
  let profileStatement = "",
    bindingStatement = "",
    validation = "";
  try {
    if (!profile) throw Error("Choose a profile or create one.");
    if (mode === "new") profileStatement = newDecayProfileStatement(profile);
    bindingStatement = newDecayBindingStatement(
      bindingName,
      scope,
      target,
      profile.Name,
    );
  } catch (e) {
    validation = e instanceof Error ? e.message : "Complete the setup";
  }
  const matching = preview?.signature === signature ? preview : null;
  const inspect = async () => {
    setBusy(true);
    setError("");
    try {
      const pattern = buildTarget(target, scope),
        variable = scope === "EDGE" ? "r" : "n";
      const response = await api.executeCypherOverHttp(
        database,
        `MATCH ${pattern} RETURN elementId(${variable}) AS id LIMIT 10`,
      );
      setPreview({ signature, rows: resultRecords(response) });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Target preview failed");
    } finally {
      setBusy(false);
    }
  };
  const apply = async () => {
    if (!profile || !bindingStatement || applying) return;
    setApplying(true);
    setError("");
    setNotice("");
    try {
      if (mode === "new") {
        if (
          !(await onApply(profileStatement, `Created profile ${profile.Name}.`))
        )
          return;
        setMode("existing");
        setSelectedProfile(profile.Name);
        setNotice(
          `Profile ${profile.Name} was created. The binding has not been saved yet.`,
        );
      }
      if (
        await onApply(bindingStatement, `Applied ${profile.Name} to ${target}.`)
      ) {
        setNotice(
          `Binding ${bindingName} is saved in ${database}. Use Resolve to inspect effective scores.`,
        );
        setStep("");
      } else
        setError(
          "The binding was not saved. Correct the reported error, then retry. Any created profile is retained.",
        );
    } finally {
      setApplying(false);
    }
  };
  if (!step)
    return (
      <section className="rounded-xl border border-norse-rune bg-norse-shadow p-5 space-y-3">
        <h2 className="text-lg font-semibold text-white">
          Start with a target, then choose its behavior
        </h2>
        {profiles?.decay_enabled === false && (
          <Alert
            type="info"
            message="Decay scoring is disabled. You can configure profiles and bindings now, but enable decay in server configuration before expecting score changes."
          />
        )}
        <p className="text-sm text-norse-silver">
          A profile defines a scoring curve. A binding applies it to node
          labels, a relationship type, or all nodes. Promotion policies can then
          boost or dampen the score.
        </p>
        {notice && <Alert type="success" message={notice} />}
        <Button onClick={() => setStep("target")}>Guided policy setup</Button>
        <Disclosure
          compact
          id="kp-concepts"
          title="What changes when a policy is applied?"
        >
          <p className="text-sm text-norse-silver">
            Decay changes relevance and visibility, not the underlying facts.
            When a score falls below the visibility threshold, entities can be
            suppressed and deindexed; this does not delete them. Resolve shows
            the effective policy for an entity. Deindex Status shows pending
            index work.
          </p>
          <p className="text-sm text-norse-silver mt-2">
            Bindings target labels and relationship types, not arbitrary IDs or
            WHERE clauses. Use entity IDs in Resolve to inspect individual
            effects. Advanced property rules and promotion conditions remain
            available in the editors below.
          </p>
        </Disclosure>
      </section>
    );
  return (
    <section
      aria-label="Guided policy setup"
      className="rounded-xl border border-nornic-primary/40 bg-norse-shadow p-5 space-y-5"
    >
      <div className="flex justify-between gap-4">
        <h2 className="text-lg font-semibold text-white">
          Guided policy setup · {database}
        </h2>
        <Button
          variant="secondary"
          disabled={applying}
          onClick={() => setStep("")}
        >
          Close setup
        </Button>
      </div>
      <ol
        className="flex flex-wrap gap-5 text-sm text-norse-silver"
        aria-label="Setup progress"
      >
        {["target", "profile", "review"].map((name, index) => (
          <li
            key={name}
            aria-current={step === name ? "step" : undefined}
            className={step === name ? "text-white font-semibold" : ""}
          >
            {index + 1}. {name}
          </li>
        ))}
      </ol>
      {error && <Alert type="error" message={error} />}{" "}
      {notice && <Alert type="info" message={notice} />}
      {step === "target" && (
        <div className="space-y-4">
          <p className="text-sm text-norse-silver">
            Choose a label or relationship type already in this database, or
            enter a target for future data. Multiple node labels match entities
            that have every listed label.
          </p>
          <label className="block text-sm text-norse-silver">
            Entity kind
            <select
              className={selectClass}
              value={scope}
              onChange={(e) => {
                setScope(e.target.value as PolicyScope);
                setTarget("");
                setSelectedProfile("");
              }}
            >
              <option value="NODE">Nodes</option>
              <option value="EDGE">Relationships</option>
            </select>
          </label>
          <label className="block text-sm text-norse-silver">
            {scope === "NODE"
              ? "Node labels (colon-separated), or * for all nodes"
              : "Relationship type"}
            <input
              list={suggestionsId}
              className={selectClass}
              value={target}
              onChange={(e) => setTarget(e.target.value)}
              placeholder={scope === "NODE" ? "MemoryEpisode" : "SUPPORTS"}
            />
          </label>
          <datalist id={suggestionsId}>
            {(scope === "NODE" ? choices.labels : choices.types)
              .filter((name) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(name))
              .map((name) => (
                <option key={name} value={name} />
              ))}
          </datalist>
          {target === "*" && (
            <Alert
              type="warning"
              message="This target applies to every node in the selected database, including future nodes."
            />
          )}
          <p className="text-xs text-norse-silver">
            Suggestions come from this database. For names requiring quotes, use
            the Cypher workspace to define the binding.
          </p>
          <Button
            variant="secondary"
            disabled={busy || !validateTarget(target, scope)}
            onClick={() => void inspect()}
          >
            {busy ? "Checking…" : "Preview matching entities"}
          </Button>
          {matching && (
            <div className="space-y-2">
              <p role="status" className="text-sm text-norse-silver">
                {matching.rows.length
                  ? `Showing ${matching.rows.length} matching entities (up to 10). This is a sample, not the total count.`
                  : "No current matches. The binding would still apply to future matching entities."}
              </p>
              <ul className="text-xs text-norse-silver space-y-2">
                {matching.rows.map((row, index) => (
                  <li key={`${row.id}-${index}`}>
                    <code className="break-all">{String(row.id)}</code>{" "}
                    <button
                      type="button"
                      className="underline text-nornic-primary"
                      onClick={() =>
                        patch({
                          tab: "resolve",
                          "resolve.id": String(row.id),
                          "kp.setup": null,
                        })
                      }
                    >
                      Inspect effective policy
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}
          <Button disabled={!matching} onClick={() => setStep("profile")}>
            Next: choose a profile
          </Button>
        </div>
      )}
      {step === "profile" && (
        <div className="space-y-4">
          <label className="block text-sm text-norse-silver">
            Profile source
            <select
              className={selectClass}
              value={mode}
              onChange={(e) => setMode(e.target.value as typeof mode)}
            >
              <option value="existing">Use an existing profile</option>
              <option value="new">Create a profile</option>
            </select>
          </label>
          <div className="grid lg:grid-cols-2 gap-6 items-start">
            <div className="space-y-4">
              {mode === "existing" ? (
                <>
                  <label className="block text-sm text-norse-silver">
                    Scoring profile
                    <select
                      className={selectClass}
                      value={selectedProfile}
                      onChange={(e) => setSelectedProfile(e.target.value)}
                    >
                      <option value="">
                        Choose an enabled {scope.toLowerCase()} profile
                      </option>
                      {existing.map((profile) => (
                        <option key={profile.Name}>{profile.Name}</option>
                      ))}
                    </select>
                  </label>
                  {!existing.length && (
                    <p className="text-sm text-norse-silver">
                      No enabled profiles for this entity kind. Choose Create a
                      profile, or enable one in Decay Profiles.
                    </p>
                  )}
                </>
              ) : (
                <>
                  <FormInput
                    label="Profile name"
                    value={profileName}
                    onChange={setProfileName}
                    required
                  />
                  <label className="block text-sm text-norse-silver">
                    Curve
                    <select
                      className={selectClass}
                      value={curve}
                      onChange={(e) => setCurve(e.target.value)}
                    >
                      <option value="exponential">
                        Exponential (Ebbinghaus-style)
                      </option>
                      <option value="linear">Linear</option>
                      <option value="step">Step</option>
                      <option value="none">No decay</option>
                    </select>
                  </label>
                  <label className="block text-sm text-norse-silver">
                    Direction
                    <select
                      disabled={curve === "none"}
                      className={selectClass}
                      value={direction}
                      onChange={(e) => {
                        setDirection(e.target.value);
                        if (e.target.value === "inverse") {
                          setAnchor("LAST_ACCESSED");
                          setFloor(threshold);
                        }
                      }}
                    >
                      <option value="forward">
                        Start strong, fade over time
                      </option>
                      <option value="inverse">
                        Start low, strengthen over time (inverse)
                      </option>
                    </select>
                  </label>
                  <FormInput
                    label="Half-life (days)"
                    type="number"
                    min={1 / 86400}
                    step="any"
                    disabled={curve === "none"}
                    value={days}
                    onChange={setDays}
                    description={direction === "inverse" ? "Exponential closes half the gap to 1 after this interval. Linear reaches 1 after twice this interval. Step changes at this interval." : "Exponential reaches half its starting score after this interval. Linear reaches zero after twice this interval. Step changes at this interval."}
                  />
                  <label className="block text-sm text-norse-silver">
                    Measure age from
                    <select
                      disabled={curve === "none"}
                      className={selectClass}
                      value={anchor}
                      onChange={(e) => setAnchor(e.target.value)}
                    >
                      <option value="VERSION">Last version time</option>
                      <option value="CREATED">Creation time</option>
                      <option value="LAST_ACCESSED">Last access time</option>
                    </select>
                  </label>
                  <div className="grid sm:grid-cols-2 gap-4">
                    <FormInput
                      label="Visibility threshold"
                      type="number"
                      min={0}
                      max={1}
                      step={0.01}
                      value={threshold}
                      onChange={setThreshold}
                      description="A lower score can hide an entity from search. This does not delete it."
                    />
                    <FormInput
                      label="Minimum score (floor)"
                      type="number"
                      min={0}
                      max={1}
                      step={0.01}
                      value={floor}
                      onChange={setFloor}
                      description="A floor at or above the threshold keeps the entity visible when no other rule changes eligibility."
                    />
                  </div>
                </>
              )}
            </div>
            <aside className="lg:sticky lg:top-6">
              {profile && <DecayCurvePreview profile={profile} />}
            </aside>
          </div>
          {mode === "new" && !profileStatement && (
            <p role="status" className="text-sm text-norse-silver">
              {validation}
            </p>
          )}
          <div className="flex gap-3">
            <Button variant="secondary" onClick={() => setStep("target")}>
              Back to target
            </Button>
            <Button
              disabled={!profile || (mode === "new" && !profileStatement)}
              onClick={() => setStep("review")}
            >
              Next: review binding
            </Button>
          </div>
        </div>
      )}
      {step === "review" && (
        <div className="space-y-4">
          <FormInput
            label="Binding name"
            value={bindingName}
            onChange={setBindingName}
            required
            description="A unique name for the rule that connects this target to the profile."
          />
          <p className="text-sm text-norse-silver">
            Database: <strong>{database}</strong> ·{" "}
            {scope === "NODE" ? "Node labels" : "Relationship type"}:{" "}
            <strong>{target}</strong> · Profile:{" "}
            <strong>{profile?.Name || "Choose a profile"}</strong>
          </p>
          <p className="text-sm text-norse-silver">
            Applying changes scoring for all matching entities, including future
            matches. Other bindings, property rules and promotion policies can
            affect the final result. Use Resolve afterward to inspect a specific
            entity.
          </p>
          {profile && profile.HalfLifeSeconds < 0 && (
            <Alert
              type="info"
              message={
                profile.ScoreFrom === "LAST_ACCESSED"
                  ? "Inverse scoring rises while idle and resets after access. The floor determines its minimum score."
                  : "Inverse scoring rises with age from the chosen anchor; it resets only when that anchor changes."
              }
            />
          )}
          <Disclosure id="kp-statements" title="Review generated Cypher">
            <pre className="text-xs text-norse-silver overflow-auto">
              {[profileStatement, bindingStatement]
                .filter(Boolean)
                .join(";\n\n")}
            </pre>
          </Disclosure>
          {!matching && (
            <p className="text-sm text-norse-silver">
              Return to Target and preview matching entities before applying.
            </p>
          )}
          {validation && (
            <p className="text-sm text-norse-silver">{validation}</p>
          )}
          <div className="flex gap-3">
            <Button
              variant="secondary"
              disabled={applying}
              onClick={() => setStep("profile")}
            >
              Back to profile
            </Button>
            <Button
              disabled={Boolean(validation) || saving || applying || !matching}
              onClick={() => void apply()}
            >
              {applying ? "Applying…" : "Apply binding"}
            </Button>
          </div>
        </div>
      )}
      <p className="text-xs text-norse-silver">
        Need property-level rules or promotion conditions? Use the existing
        editors or{" "}
        <Link
          className="underline text-nornic-primary"
          to={`/workspace/${encodeURIComponent(database)}?query=${encodeURIComponent("CALL kp.profiles()")}`}
        >
          Cypher workspace
        </Link>
        .
      </p>
    </section>
  );
}
export function DecayCurvePreview({
  profile,
}: {
  profile: DecayProfileBundle;
}) {
  const id = useId(),
    halfLife = Math.abs(profile.HalfLifeSeconds) || 86400;
  const points = useMemo(
    () =>
      Array.from({ length: 65 }, (_, index) => ({
        age: (halfLife * 4 * index) / 64,
        score: decayBaseline(profile, (halfLife * 4 * index) / 64),
      })),
    [
      profile.HalfLifeSeconds,
      profile.Function,
      profile.ScoreFloor,
      profile.DecayEnabled,
    ],
  );
  const valid = points.every((point) => Number.isFinite(point.score));
  return (
    <section
      aria-label="Decay curve preview"
      className="rounded-lg border border-norse-rune bg-norse-night p-4 space-y-3"
    >
      <h3 className="font-semibold text-white">Baseline curve preview</h3>
      <p className="text-sm text-norse-silver">
        {!profile.DecayEnabled || profile.Function === "none"
          ? "No time-based decay: the baseline stays at 1."
          : profile.HalfLifeSeconds < 0
            ? "Inverse: the baseline rises over time."
            : "Forward: the baseline falls over time."}{" "}
        {profile.ScoreFrom === "LAST_ACCESSED"
          ? "Access resets the age to zero."
          : "Age starts at the chosen anchor."}{" "}
        Promotion and property overrides are not included in this illustration.
      </p>
      {valid && (
        <svg
          viewBox="0 0 360 150"
          role="img"
          aria-labelledby={id}
          className="w-full max-w-lg"
        >
          <title id={id}>Baseline score over four half-life intervals</title>
          <line x1="35" y1="120" x2="345" y2="120" stroke="#929bad" />
          <line x1="35" y1="15" x2="35" y2="120" stroke="#929bad" />
          <line
            x1="35"
            x2="345"
            y1={120 - profile.VisibilityThreshold * 100}
            y2={120 - profile.VisibilityThreshold * 100}
            stroke="#fbbf24"
            strokeDasharray="4 4"
          />
          <polyline
            fill="none"
            stroke="#34d399"
            strokeWidth="3"
            points={points
              .map(
                (point, index) =>
                  `${35 + (index / 64) * 310},${120 - point.score * 100}`,
              )
              .join(" ")}
          />
          <text x="8" y="23" fill="#cbd5e1" fontSize="11">
            1
          </text>
          <text x="8" y="123" fill="#cbd5e1" fontSize="11">
            0
          </text>
          <text x="35" y="142" fill="#cbd5e1" fontSize="11">
            0
          </text>
          <text x="280" y="142" fill="#cbd5e1" fontSize="11">
            4 intervals
          </text>
        </svg>
      )}
      <p className="text-xs text-norse-silver">
        One interval ={" "}
        {halfLife >= 86400
          ? `${(halfLife / 86400).toLocaleString(undefined, { maximumFractionDigits: 3 })} days`
          : `${halfLife.toLocaleString()} seconds`}
        . Emerald: baseline including floor. Dashed gold: visibility threshold.
        Scores below the threshold can be hidden.
      </p>
      <dl className="grid grid-cols-3 gap-2 text-xs text-norse-silver">
        {[0, 1, 4].map((interval) => (
          <div key={interval}>
            <dt>
              {interval === 0
                ? "At anchor"
                : `After ${interval} interval${interval === 1 ? "" : "s"}`}
            </dt>
            <dd className="text-white font-mono">
              {decayBaseline(profile, interval * halfLife).toFixed(3)}
            </dd>
          </div>
        ))}
      </dl>
      {profile.HalfLifeSeconds < 0 &&
        profile.ScoreFloor < profile.VisibilityThreshold && (
          <Alert
            type="warning"
            message="The inverse curve begins below the visibility threshold. With this floor, matching entities can be hidden after access until their score rises enough."
          />
        )}
    </section>
  );
}
