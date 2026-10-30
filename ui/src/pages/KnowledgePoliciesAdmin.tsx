import { useCallback, useEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import {
  Database,
  RefreshCw,
  Search,
  Shield,
  ShieldCheck,
  Sliders,
  Trash2,
} from "lucide-react";
import { Alert } from "../components/common/Alert";
import { Button } from "../components/common/Button";
import { FormInput } from "../components/common/FormInput";
import { PageHeader } from "../components/common/PageHeader";
import { PageLayout } from "../components/common/PageLayout";
import { KnowledgePolicyEditorGrids } from "../components/knowledge/KnowledgePolicyEditorGrids";
import {
  api,
  type DatabaseInfo,
  type DeindexWorkItem,
  type KPDeindexStatusResponse,
  type KPPoliciesResponse,
  type KPProfilesResponse,
  type ScoringResolution,
} from "../utils/api";
import { BASE_PATH, joinBasePath } from "../utils/basePath";

type TabId =
  | "overview"
  | "decay-profiles"
  | "promotion-policies"
  | "resolve"
  | "deindex-status";

const TABS: Array<{ id: TabId; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "decay-profiles", label: "Decay Profiles" },
  { id: "promotion-policies", label: "Promotion Policies" },
  { id: "resolve", label: "Resolve" },
  { id: "deindex-status", label: "Deindex Status" },
];

function EnabledBadge({ enabled }: { enabled: boolean }) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${
        enabled
          ? "text-green-300 border-green-500/30 bg-green-500/10"
          : "text-norse-silver border-norse-rune bg-norse-stone/40"
      }`}
    >
      {enabled ? "Enabled" : "Disabled"}
    </span>
  );
}

function BoolBadge({
  value,
  trueLabel = "Yes",
  falseLabel = "No",
}: {
  value: boolean;
  trueLabel?: string;
  falseLabel?: string;
}) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${
        value
          ? "text-valhalla-gold border-valhalla-gold/30 bg-valhalla-gold/10"
          : "text-norse-silver border-norse-rune bg-norse-stone/40"
      }`}
    >
      {value ? trueLabel : falseLabel}
    </span>
  );
}

function TargetCell({
  labels,
  edgeType,
  isWildcard,
}: {
  labels?: string[];
  edgeType?: string;
  isWildcard: boolean;
}) {
  if (isWildcard) {
    return <span className="text-norse-fog italic">wildcard</span>;
  }
  if (edgeType) {
    return (
      <span className="text-nornic-accent font-mono text-xs">:{edgeType}</span>
    );
  }
  if (labels && labels.length > 0) {
    return (
      <span className="text-white font-mono text-xs">
        {labels.map((l) => `:${l}`).join(", ")}
      </span>
    );
  }
  return <span className="text-norse-fog">—</span>;
}

export function KnowledgePoliciesAdmin() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();

  const [checkingAuth, setCheckingAuth] = useState(true);
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [selectedDb, setSelectedDb] = useState("");
  const [activeTab, setActiveTab] = useState<TabId>(
    (searchParams.get("tab") as TabId) || "overview",
  );

  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [saving, setSaving] = useState(false);
  const mutationInFlight = useRef(false);
  const [error, setError] = useState("");
  const [notification, setNotification] = useState<{
    id: number;
    type: "error" | "success";
    message: string;
  } | null>(null);

  const [profiles, setProfiles] = useState<KPProfilesResponse | null>(null);
  const [policies, setPolicies] = useState<KPPoliciesResponse | null>(null);
  const [deindexStatus, setDeindexStatus] =
    useState<KPDeindexStatusResponse | null>(null);

  // Resolve tab state
  const [resolveEntityId, setResolveEntityId] = useState("");
  const [resolveLabels, setResolveLabels] = useState("");
  const [resolveEdgeType, setResolveEdgeType] = useState("");
  const [resolving, setResolving] = useState(false);
  const [resolveResult, setResolveResult] = useState<ScoringResolution | null>(
    null,
  );
  const [resolveError, setResolveError] = useState("");

  // Load databases and check admin auth
  useEffect(() => {
    fetch(joinBasePath(BASE_PATH, "/auth/me"), { credentials: "include" })
      .then((res) => res.json())
      .then(async (data) => {
        const roles = data.roles || [];
        if (!roles.includes("admin")) {
          navigate("/security");
          return;
        }
        const list = await api.listDatabases();
        setDatabases(
          list.filter((db) => db.name !== "system" && db.type !== "composite"),
        );
        const requestedDb = searchParams.get("db") || "";
        const available = list.filter(
          (db) => db.name !== "system" && db.type !== "composite",
        );
        const initialDb = available.some((db) => db.name === requestedDb)
          ? requestedDb
          : available[0]?.name || "";
        setSelectedDb(initialDb);
        setCheckingAuth(false);
      })
      .catch(() => {
        navigate("/security");
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadData = useCallback(async (dbName: string) => {
    const db = dbName || undefined;
    const [profilesData, policiesData, deindexData] = await Promise.all([
      api.getKnowledgePolicyProfiles(db),
      api.getKnowledgePolicyPolicies(db),
      api.getDeindexStatus(db),
    ]);
    setProfiles(profilesData);
    setPolicies(policiesData);
    setDeindexStatus(deindexData);
  }, []);

  useEffect(() => {
    if (!selectedDb) {
      setLoading(false);
      return;
    }
    const params: Record<string, string> = { db: selectedDb };
    if (activeTab !== "overview") params.tab = activeTab;
    setSearchParams(params, { replace: true });
    setLoading(true);
    setError("");
    loadData(selectedDb)
      .catch((err) => {
        setError(
          err instanceof Error
            ? err.message
            : "Failed to load knowledge policy data",
        );
      })
      .finally(() => setLoading(false));
  }, [selectedDb, loadData, setSearchParams, activeTab]);

  const handleTabChange = (tab: TabId) => {
    setActiveTab(tab);
    const params: Record<string, string> = { db: selectedDb };
    if (tab !== "overview") params.tab = tab;
    setSearchParams(params, { replace: true });
  };

  const handleRefresh = async () => {
    if (!selectedDb) return;
    setRefreshing(true);
    setError("");
    try {
      await loadData(selectedDb);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh");
    } finally {
      setRefreshing(false);
    }
  };

  const showNotification = useCallback(
    (type: "error" | "success", message: string) => {
      setNotification({ id: Date.now(), type, message });
    },
    [],
  );

  const handleAlterPolicy = useCallback(
    async (statement: string, successMessage: string) => {
      if (!selectedDb) return false;
      if (mutationInFlight.current) {
        showNotification(
          "error",
          "Wait for the current policy update to finish before editing again.",
        );
        return false;
      }
      mutationInFlight.current = true;
      setSaving(true);
      try {
        await api.alterKnowledgePolicy(statement, selectedDb);
        await loadData(selectedDb);
        showNotification("success", successMessage);
        return true;
      } catch (err) {
        try {
          await loadData(selectedDb);
        } catch {
          // Preserve the mutation error; the next manual refresh can retry loading.
        }
        showNotification(
          "error",
          err instanceof Error
            ? err.message
            : "Failed to update knowledge policy",
        );
        return false;
      } finally {
        mutationInFlight.current = false;
        setSaving(false);
      }
    },
    [loadData, selectedDb, showNotification],
  );

  const handleResolve = async () => {
    setResolveError("");
    setResolveResult(null);
    setResolving(true);
    try {
      const labels = resolveLabels
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      const result = await api.resolveKnowledgePolicy({
        entityId: resolveEntityId || undefined,
        labels: labels.length > 0 ? labels : undefined,
        edgeType: resolveEdgeType || undefined,
        database: selectedDb || undefined,
      });
      setResolveResult(result);
    } catch (err) {
      setResolveError(err instanceof Error ? err.message : "Resolution failed");
    } finally {
      setResolving(false);
    }
  };

  if (checkingAuth) {
    return (
      <PageLayout>
        <div className="flex items-center justify-center flex-1">
          <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
        </div>
      </PageLayout>
    );
  }

  return (
    <PageLayout className="min-w-0">
      <PageHeader
        title="Knowledge Policies"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={handleRefresh}
              disabled={!selectedDb || refreshing}
              icon={RefreshCw}
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </Button>
          </div>
        }
      />

      <main className="w-full min-w-0 max-w-6xl mx-auto px-4 py-6 sm:p-6 space-y-6">
        {notification && (
          <div
            className="fixed right-4 top-4 z-50 w-[min(26rem,calc(100vw-2rem))]"
            role={notification.type === "error" ? "alert" : "status"}
            aria-live={notification.type === "error" ? "assertive" : "polite"}
            aria-atomic="true"
          >
            <Alert
              key={notification.id}
              type={notification.type}
              message={notification.message}
              dismissible
              onDismiss={() => setNotification(null)}
              className="shadow-xl"
            />
          </div>
        )}
        {error && (
          <Alert
            type="error"
            message={error}
            dismissible
            onDismiss={() => setError("")}
          />
        )}

        {/* Database selector */}
        <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div className="space-y-2">
              <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                <Shield className="w-5 h-5 text-nornic-accent" />
                Knowledge Policy Control Plane
              </h2>
              <p className="text-sm text-norse-silver max-w-3xl">
                Inspect decay profiles, promotion policies, score resolution,
                and deindex queue status for the knowledge-layer scoring
                subsystem.
              </p>
            </div>
            <div className="w-full lg:w-72">
              <label
                htmlFor="kp-db"
                className="block text-sm font-medium text-norse-silver mb-2"
              >
                Target database
              </label>
              <select
                id="kp-db"
                value={selectedDb}
                onChange={(e) => setSelectedDb(e.target.value)}
                className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
              >
                {databases.map((db) => (
                  <option key={db.name} value={db.name}>
                    {db.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </section>

        {/* Tab nav */}
        <div className="flex max-w-full gap-1 overflow-x-auto border-b border-norse-rune">
          {TABS.map((tab) => (
            <button
              type="button"
              key={tab.id}
              onClick={() => handleTabChange(tab.id)}
              className={`shrink-0 px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                activeTab === tab.id
                  ? "bg-norse-shadow border border-b-norse-shadow border-norse-rune text-white"
                  : "text-norse-silver hover:text-white hover:bg-norse-stone/40"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {!selectedDb ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-12 text-center">
            <Database className="w-12 h-12 text-norse-silver mx-auto mb-3" />
            <h2 className="text-lg font-semibold text-white mb-2">
              No database available
            </h2>
            <p className="text-norse-silver">
              Create or open a standard database to inspect knowledge policies.
            </p>
          </section>
        ) : loading ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-12 flex items-center justify-center">
            <div className="w-10 h-10 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
          </section>
        ) : (
          <>
            {/* ── OVERVIEW TAB ── */}
            {activeTab === "overview" && (
              <div className="space-y-6">
                <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
                  {/* Decay enabled card */}
                  <div
                    className={`rounded-lg border p-4 ${
                      profiles?.decay_enabled
                        ? "text-green-300 border-green-500/30 bg-green-500/10"
                        : "text-norse-silver border-norse-rune bg-norse-shadow"
                    }`}
                  >
                    <div className="text-xs uppercase tracking-wide mb-2 opacity-80">
                      Decay scoring
                    </div>
                    <div className="text-2xl font-semibold">
                      {profiles?.decay_enabled ? "Enabled" : "Disabled"}
                    </div>
                    <div className="text-sm opacity-80 mt-2">
                      {profiles?.decay_enabled
                        ? "Score-based decay is active"
                        : "Feature flag off (DecayEnabled=false)"}
                    </div>
                  </div>

                  {/* Decay profiles count */}
                  <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                    <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">
                      Decay profiles
                    </div>
                    <div className="text-2xl font-semibold text-white">
                      {profiles?.bundles?.length ?? 0}
                    </div>
                    <div className="text-sm text-norse-silver mt-2">
                      {profiles?.bindings?.length ?? 0} binding
                      {(profiles?.bindings?.length ?? 0) !== 1 ? "s" : ""}
                    </div>
                  </div>

                  {/* Promotion policies count */}
                  <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                    <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">
                      Promotion policies
                    </div>
                    <div className="text-2xl font-semibold text-white">
                      {policies?.promotion_policies?.length ?? 0}
                    </div>
                    <div className="text-sm text-norse-silver mt-2">
                      {policies?.promotion_profiles?.length ?? 0} profile
                      {(policies?.promotion_profiles?.length ?? 0) !== 1
                        ? "s"
                        : ""}
                    </div>
                  </div>

                  {/* Deindex queue */}
                  <div
                    className={`rounded-lg border p-4 ${
                      (deindexStatus?.pending_count ?? 0) > 0
                        ? "text-yellow-300 border-yellow-500/30 bg-yellow-500/10"
                        : "text-norse-silver border-norse-rune bg-norse-shadow"
                    }`}
                  >
                    <div className="text-xs uppercase tracking-wide mb-2 opacity-80">
                      Pending deindex
                    </div>
                    <div className="text-2xl font-semibold">
                      {deindexStatus?.supported === false
                        ? "N/A"
                        : (deindexStatus?.pending_count ?? 0).toLocaleString()}
                    </div>
                    <div className="text-sm opacity-80 mt-2">
                      {deindexStatus?.supported === false
                        ? deindexStatus.message || "Not supported"
                        : "items in queue"}
                    </div>
                  </div>
                </section>

                {/* Quick summary tables */}
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                  <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <Sliders className="w-5 h-5 text-nornic-accent" />
                      Decay profile bundles
                    </h2>
                    {(profiles?.bundles ?? []).length === 0 ? (
                      <p className="text-norse-silver text-sm">
                        No decay profiles defined.
                      </p>
                    ) : (
                      <div className="overflow-x-auto border border-norse-rune rounded-lg">
                        <table className="w-full text-sm">
                          <thead className="bg-norse-stone/60">
                            <tr>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Name
                              </th>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Scope
                              </th>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Status
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {(profiles?.bundles ?? []).map((b) => (
                              <tr
                                key={b.Name}
                                className="border-t border-norse-rune/50"
                              >
                                <td className="px-3 py-2 text-white font-medium">
                                  {b.Name}
                                </td>
                                <td className="px-3 py-2 text-norse-silver">
                                  {b.Scope || "—"}
                                </td>
                                <td className="px-3 py-2">
                                  <EnabledBadge enabled={b.Enabled} />
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>

                  <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <ShieldCheck className="w-5 h-5 text-nornic-accent" />
                      Promotion policies
                    </h2>
                    {(policies?.promotion_policies ?? []).length === 0 ? (
                      <p className="text-norse-silver text-sm">
                        No promotion policies defined.
                      </p>
                    ) : (
                      <div className="overflow-x-auto border border-norse-rune rounded-lg">
                        <table className="w-full text-sm">
                          <thead className="bg-norse-stone/60">
                            <tr>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Name
                              </th>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Target
                              </th>
                              <th className="px-3 py-2 text-left text-norse-silver font-medium">
                                Status
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {(policies?.promotion_policies ?? []).map((p) => (
                              <tr
                                key={p.Name}
                                className="border-t border-norse-rune/50"
                              >
                                <td className="px-3 py-2 text-white font-medium">
                                  {p.Name}
                                </td>
                                <td className="px-3 py-2">
                                  <TargetCell
                                    labels={p.TargetLabels}
                                    edgeType={p.TargetEdgeType}
                                    isWildcard={p.IsWildcard}
                                  />
                                </td>
                                <td className="px-3 py-2">
                                  <EnabledBadge enabled={p.Enabled} />
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* ── DECAY PROFILES TAB ── */}
            {activeTab === "decay-profiles" && (
              <KnowledgePolicyEditorGrids
                section="decay"
                profiles={profiles}
                policies={policies}
                saving={saving}
                onAlter={handleAlterPolicy}
                onValidationError={(message) =>
                  showNotification("error", message)
                }
              />
            )}

            {/* ── PROMOTION POLICIES TAB ── */}
            {activeTab === "promotion-policies" && (
              <KnowledgePolicyEditorGrids
                section="promotion"
                profiles={profiles}
                policies={policies}
                saving={saving}
                onAlter={handleAlterPolicy}
                onValidationError={(message) =>
                  showNotification("error", message)
                }
              />
            )}

            {/* ── RESOLVE TAB ── */}
            {activeTab === "resolve" && (
              <div className="space-y-6">
                <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <Search className="w-5 h-5 text-nornic-accent" />
                    Resolve scoring policy
                  </h2>
                  <p className="text-sm text-norse-silver">
                    Resolve the effective decay profile and promotion policy
                    that would be applied to an entity. Provide an entity ID, a
                    comma-separated list of labels, or an edge type.
                  </p>

                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <FormInput
                      id="resolve-entity-id"
                      label="Entity ID"
                      value={resolveEntityId}
                      onChange={setResolveEntityId}
                      placeholder="Value from id(n) or elementId(n)"
                    />
                    <FormInput
                      id="resolve-labels"
                      label="Labels (comma-separated)"
                      value={resolveLabels}
                      onChange={setResolveLabels}
                      placeholder="e.g., KnowledgeFact, MemoryEpisode"
                    />
                    <FormInput
                      id="resolve-edge-type"
                      label="Edge Type"
                      value={resolveEdgeType}
                      onChange={setResolveEdgeType}
                      placeholder="e.g., SUPERSEDES"
                    />
                  </div>

                  <div className="flex gap-2">
                    <Button
                      variant="primary"
                      onClick={handleResolve}
                      disabled={
                        resolving ||
                        (!resolveEntityId && !resolveLabels && !resolveEdgeType)
                      }
                      icon={Search}
                    >
                      {resolving ? "Resolving..." : "Resolve"}
                    </Button>
                    {(resolveResult || resolveError) && (
                      <Button
                        variant="secondary"
                        onClick={() => {
                          setResolveResult(null);
                          setResolveError("");
                        }}
                      >
                        Clear
                      </Button>
                    )}
                  </div>

                  {resolveError && (
                    <div role="alert" aria-live="assertive" aria-atomic="true">
                      <Alert
                        type="error"
                        message={resolveError}
                        dismissible
                        onDismiss={() => setResolveError("")}
                      />
                    </div>
                  )}
                </section>

                {resolveResult && (
                  <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-5">
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <ShieldCheck className="w-5 h-5 text-nornic-accent" />
                      Resolution result
                    </h2>

                    {/* Explanation — prominent */}
                    <div className="rounded-lg border border-nornic-accent/30 bg-nornic-accent/5 p-4">
                      <div className="text-xs uppercase tracking-wide text-nornic-accent mb-2">
                        Explanation
                      </div>
                      <p className="text-white text-sm leading-relaxed">
                        {resolveResult.Explanation ||
                          "No explanation provided."}
                      </p>
                    </div>

                    {/* Score summary */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                        <div className="text-xs text-norse-silver mb-1">
                          Base Score
                        </div>
                        <div className="text-2xl font-semibold text-white">
                          {resolveResult.BaseScore?.toFixed(3) ?? "—"}
                        </div>
                      </div>
                      <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                        <div className="text-xs text-norse-silver mb-1">
                          Final Score
                        </div>
                        <div className="text-2xl font-semibold text-white">
                          {resolveResult.FinalScore?.toFixed(3) ?? "—"}
                        </div>
                      </div>
                      <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                        <div className="text-xs text-norse-silver mb-1">
                          Eff. Rate
                        </div>
                        <div className="text-2xl font-semibold text-white">
                          {resolveResult.EffectiveRate?.toFixed(4) ?? "—"}
                        </div>
                      </div>
                      <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                        <div className="text-xs text-norse-silver mb-1">
                          Eff. Threshold
                        </div>
                        <div className="text-2xl font-semibold text-white">
                          {resolveResult.EffectiveThreshold?.toFixed(3) ?? "—"}
                        </div>
                      </div>
                    </div>

                    {/* Detail grid */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                      <div className="space-y-2">
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">Target ID</span>
                          <span className="text-white font-mono text-xs break-all text-right max-w-[60%]">
                            {resolveResult.TargetID || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Target Scope
                          </span>
                          <span className="text-white">
                            {resolveResult.TargetScope || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Decay Profile
                          </span>
                          <span className="text-white font-mono text-xs">
                            {resolveResult.ResolvedDecayProfileID || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">Score From</span>
                          <span className="text-white font-mono text-xs">
                            {resolveResult.ResolvedScoreFrom || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">No Decay</span>
                          <BoolBadge
                            value={resolveResult.NoDecay}
                            trueLabel="Yes"
                            falseLabel="No"
                          />
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Suppression Eligible
                          </span>
                          <BoolBadge
                            value={resolveResult.SuppressionEligible}
                            trueLabel="Yes"
                            falseLabel="No"
                          />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Promotion Policy
                          </span>
                          <span className="text-white">
                            {resolveResult.AppliedPromotionPolicyName || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Promotion Profile
                          </span>
                          <span className="text-white">
                            {resolveResult.AppliedPromotionProfileName || "—"}
                          </span>
                        </div>
                        <div className="flex justify-between border-b border-norse-rune/40 pb-1">
                          <span className="text-norse-silver">
                            Eff. Multiplier
                          </span>
                          <span className="text-white">
                            {resolveResult.EffectiveMultiplier?.toFixed(3) ??
                              "—"}
                          </span>
                        </div>
                        {(resolveResult.AppliedDecayProfileNames ?? []).length >
                          0 && (
                          <div className="border-b border-norse-rune/40 pb-1">
                            <div className="text-norse-silver mb-1">
                              Applied Decay Profiles
                            </div>
                            <div className="flex flex-wrap gap-1">
                              {resolveResult.AppliedDecayProfileNames.map(
                                (name) => (
                                  <span
                                    key={name}
                                    className="px-2 py-0.5 rounded bg-nornic-accent/10 text-nornic-accent text-xs font-mono border border-nornic-accent/20"
                                  >
                                    {name}
                                  </span>
                                ),
                              )}
                            </div>
                          </div>
                        )}
                        {(resolveResult.ResolutionSourceChain ?? []).length >
                          0 && (
                          <div>
                            <div className="text-norse-silver mb-1">
                              Resolution chain
                            </div>
                            <div className="space-y-1">
                              {resolveResult.ResolutionSourceChain.map(
                                (step, i) => (
                                  <div
                                    key={i}
                                    className="text-xs text-norse-fog font-mono flex items-center gap-1"
                                  >
                                    <span className="text-nornic-accent">
                                      {i + 1}.
                                    </span>
                                    {step}
                                  </div>
                                ),
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </section>
                )}
              </div>
            )}

            {/* ── DEINDEX STATUS TAB ── */}
            {activeTab === "deindex-status" && (
              <div className="space-y-6">
                <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <Trash2 className="w-5 h-5 text-nornic-accent" />
                    Deindex queue status
                  </h2>
                  <p className="text-sm text-norse-silver">
                    Items that have fallen below the visibility threshold are
                    queued for deindexing from the search index and secondary
                    structures.
                  </p>

                  {deindexStatus?.supported === false ? (
                    <Alert
                      type="warning"
                      title="Deindex queue not supported"
                      message={
                        deindexStatus.message ||
                        "This database does not support the deindex queue."
                      }
                    />
                  ) : (
                    <>
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div
                          className={`rounded-lg border p-4 ${
                            (deindexStatus?.pending_count ?? 0) > 0
                              ? "text-yellow-300 border-yellow-500/30 bg-yellow-500/10"
                              : "text-green-300 border-green-500/30 bg-green-500/10"
                          }`}
                        >
                          <div className="text-xs uppercase tracking-wide mb-2 opacity-80">
                            Pending items
                          </div>
                          <div className="text-3xl font-semibold">
                            {(
                              deindexStatus?.pending_count ?? 0
                            ).toLocaleString()}
                          </div>
                        </div>
                      </div>

                      <div className="overflow-x-auto border border-norse-rune rounded-lg">
                        <table className="w-full text-sm">
                          <thead className="bg-norse-stone/60">
                            <tr>
                              <th className="px-4 py-3 text-left text-norse-silver font-medium">
                                Work Item ID
                              </th>
                              <th className="px-4 py-3 text-left text-norse-silver font-medium">
                                Target ID
                              </th>
                              <th className="px-4 py-3 text-left text-norse-silver font-medium">
                                Scope
                              </th>
                              <th className="px-4 py-3 text-left text-norse-silver font-medium">
                                Status
                              </th>
                              <th className="px-4 py-3 text-right text-norse-silver font-medium">
                                Enqueued
                              </th>
                            </tr>
                          </thead>
                          <tbody>
                            {(deindexStatus?.items ?? []).length === 0 ? (
                              <tr>
                                <td
                                  colSpan={5}
                                  className="px-4 py-6 text-center text-norse-silver"
                                >
                                  {(deindexStatus?.pending_count ?? 0) === 0
                                    ? "No items pending deindex."
                                    : "Items are pending but details are not available."}
                                </td>
                              </tr>
                            ) : (
                              (deindexStatus?.items ?? []).map(
                                (item: DeindexWorkItem) => (
                                  <tr
                                    key={item.workItemId}
                                    className="border-t border-norse-rune/50"
                                  >
                                    <td className="px-4 py-3 text-white font-mono text-xs">
                                      {item.workItemId}
                                    </td>
                                    <td className="px-4 py-3 text-norse-silver font-mono text-xs">
                                      {item.targetId}
                                    </td>
                                    <td className="px-4 py-3 text-norse-silver">
                                      {item.targetScope || "—"}
                                    </td>
                                    <td className="px-4 py-3">
                                      <span className="px-2 py-0.5 rounded text-xs font-medium border border-norse-rune bg-norse-stone/40 text-norse-silver">
                                        {item.status}
                                      </span>
                                    </td>
                                    <td className="px-4 py-3 text-right text-norse-fog text-xs">
                                      {item.enqueuedAt
                                        ? new Date(
                                            item.enqueuedAt,
                                          ).toLocaleString()
                                        : "—"}
                                    </td>
                                  </tr>
                                ),
                              )
                            )}
                          </tbody>
                        </table>
                      </div>
                    </>
                  )}
                </section>
              </div>
            )}
          </>
        )}
      </main>
    </PageLayout>
  );
}
