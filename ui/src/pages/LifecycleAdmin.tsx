import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { UiGrid } from "@ornery/ui-grid-react";
import type { GridColumnDef, GridOptions, GridRecord } from "@ornery/ui-grid-core";
import {
  Activity,
  AlertTriangle,
  Clock3,
  Database,
  Gauge,
  PauseCircle,
  PlayCircle,
  RefreshCw,
  ShieldAlert,
  ShieldCheck,
  TimerReset,
  Trash2,
} from "lucide-react";
import { PageLayout } from "../components/common/PageLayout";
import { PageHeader } from "../components/common/PageHeader";
import { Alert } from "../components/common/Alert";
import { Button } from "../components/common/Button";
import { FormInput } from "../components/common/FormInput";
import { Modal } from "../components/common/Modal";
import {
  api,
  type DatabaseInfo,
  type MVCCLifecycleDebtKey,
  type MVCCLifecycleRollup,
  type MVCCLifecycleStatus,
} from "../utils/api";
import { BASE_PATH, joinBasePath } from "../utils/basePath";

type ActionKind = "pause" | "resume" | "prune" | "schedule";

interface PendingAction {
  kind: ActionKind;
  title: string;
  description: string;
  confirmLabel: string;
  variant: "primary" | "secondary" | "danger" | "success" | "ghost";
}

function formatBytes(bytes?: number): string {
  const value =
    typeof bytes === "number" && Number.isFinite(bytes)
      ? Math.max(0, bytes)
      : 0;
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let scaled = value;
  let index = -1;
  do {
    scaled /= 1024;
    index++;
  } while (scaled >= 1024 && index < units.length - 1);
  return `${scaled.toFixed(scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2)} ${units[index]}`;
}

function formatDurationSeconds(seconds?: number): string {
  if (
    typeof seconds !== "number" ||
    !Number.isFinite(seconds) ||
    seconds <= 0
  ) {
    return "0s";
  }
  if (seconds < 60) {
    return `${seconds.toFixed(seconds >= 10 ? 0 : 1)}s`;
  }
  const totalSeconds = Math.round(seconds);
  const mins = Math.floor(totalSeconds / 60);
  const secs = totalSeconds % 60;
  if (mins < 60) {
    return `${mins}m ${secs}s`;
  }
  const hours = Math.floor(mins / 60);
  const remMins = mins % 60;
  return `${hours}h ${remMins}m`;
}

function formatTimestamp(value?: string): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function pressureTone(band?: string): string {
  switch (band) {
    case "critical":
      return "text-red-300 border-red-500/30 bg-red-500/10";
    case "high":
      return "text-yellow-300 border-yellow-500/30 bg-yellow-500/10";
    default:
      return "text-green-300 border-green-500/30 bg-green-500/10";
  }
}

export function LifecycleAdmin() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [selectedDb, setSelectedDb] = useState("");
  const [status, setStatus] = useState<MVCCLifecycleStatus | null>(null);
  const [debtKeys, setDebtKeys] = useState<MVCCLifecycleDebtKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [scheduleInput, setScheduleInput] = useState("30s");
  const [debtLimit, setDebtLimit] = useState("25");
  const [pendingAction, setPendingAction] = useState<PendingAction | null>(
    null,
  );
  const [actionLoading, setActionLoading] = useState<ActionKind | null>(null);

  const supportedDatabases = useMemo(
    () =>
      databases.filter((db) => db.name !== "system" && db.type !== "composite"),
    [databases],
  );
  const unsupportedDatabases = useMemo(
    () =>
      databases.filter((db) => db.name === "system" || db.type === "composite"),
    [databases],
  );

  const numericDebtLimit = useMemo(() => {
    const parsed = Number.parseInt(debtLimit, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return 25;
    return Math.min(parsed, 100);
  }, [debtLimit]);

  const debtGridData = useMemo<GridRecord[]>(
    () => debtKeys.map((key) => ({ ...key, __gridId: key.logical_key })),
    [debtKeys],
  );

  const debtColumnDefs = useMemo<GridColumnDef[]>(
    () => [
      { name: "logical_key", displayName: "Logical key", field: "logical_key", width: "minmax(14rem, 1.6fr)" },
      { name: "namespace", displayName: "Namespace", field: "namespace", width: "minmax(10rem, 1fr)" },
      {
        name: "debt_bytes",
        displayName: "Debt",
        field: "debt_bytes",
        align: "end",
        width: "120px",
        formatter: (value) => formatBytes(Number(value ?? 0)),
      },
      {
        name: "tombstone_depth",
        displayName: "Tombstones",
        field: "tombstone_depth",
        align: "end",
        width: "120px",
      },
      {
        name: "versions_to_delete",
        displayName: "Deletes",
        field: "versions_to_delete",
        align: "end",
        width: "120px",
      },
    ],
    [],
  );

  const debtGridOptions = useMemo<GridOptions>(
    () => ({
      id: "lifecycle-debt-grid",
      data: debtGridData,
      columnDefs: debtColumnDefs,
      rowIdentity: (row) => String(row.__gridId),
      enableSorting: true,
      enableFiltering: true,
      viewportHeight: 360,
      emptyMessage: "No lifecycle debt keys reported for this database.",
    }),
    [debtColumnDefs, debtGridData],
  );

  const readerGridData = useMemo<GridRecord[]>(
    () =>
      (status?.readers ?? []).map((reader, index) => ({
        ...reader,
        ReaderID: reader.ReaderID || `reader-${index + 1}`,
        Namespace: reader.Namespace || "—",
        StartTime: reader.StartTime || "",
        __gridId: `${reader.ReaderID || "reader"}-${index}`,
      })),
    [status?.readers],
  );

  const readerColumnDefs = useMemo<GridColumnDef[]>(
    () => [
      { name: "ReaderID", displayName: "Reader", field: "ReaderID", width: "minmax(12rem, 1.1fr)" },
      { name: "Namespace", displayName: "Namespace", field: "Namespace", width: "minmax(10rem, 1fr)" },
      {
        name: "StartTime",
        displayName: "Started",
        field: "StartTime",
        width: "minmax(12rem, 1fr)",
        formatter: (value) => formatTimestamp(typeof value === "string" ? value : undefined),
      },
    ],
    [],
  );

  const readerGridOptions = useMemo<GridOptions>(
    () => ({
      id: "lifecycle-readers-grid",
      data: readerGridData,
      columnDefs: readerColumnDefs,
      rowIdentity: (row) => String(row.__gridId),
      enableSorting: true,
      enableFiltering: true,
      viewportHeight: 320,
      emptyMessage: "No active snapshot readers are currently registered.",
    }),
    [readerColumnDefs, readerGridData],
  );

  const loadDatabases = useCallback(async () => {
    const list = await api.listDatabases();
    setDatabases(list);
    return list;
  }, []);

  const loadLifecycleState = useCallback(
    async (dbName: string, preserveScheduleInput: boolean) => {
      const [nextStatus, debtResponse] = await Promise.all([
        api.getDatabaseLifecycleStatus(dbName),
        api.getDatabaseLifecycleDebt(dbName, numericDebtLimit),
      ]);
      setStatus(nextStatus);
      setDebtKeys(debtResponse.keys ?? []);
      if (!preserveScheduleInput) {
        setScheduleInput(nextStatus.cycle_interval || "30s");
      }
    },
    [numericDebtLimit],
  );

  useEffect(() => {
    fetch(joinBasePath(BASE_PATH, "/auth/me"), { credentials: "include" })
      .then((res) => res.json())
      .then(async (data) => {
        const roles = data.roles || [];
        if (!roles.includes("admin")) {
          navigate("/security");
          return;
        }
        const list = await loadDatabases();
        const available = list.filter(
          (db) => db.name !== "system" && db.type !== "composite",
        );
        const requestedDb = searchParams.get("db") || "";
        const initialDb = available.some((db) => db.name === requestedDb)
          ? requestedDb
          : available[0]?.name || "";
        setSelectedDb(initialDb);
        setCheckingAuth(false);
      })
      .catch(() => {
        navigate("/security");
      });
  }, [loadDatabases, navigate, searchParams]);

  useEffect(() => {
    if (!selectedDb) {
      setLoading(false);
      return;
    }
    setSearchParams({ db: selectedDb }, { replace: true });
    setLoading(true);
    setError("");
    loadLifecycleState(selectedDb, false)
      .catch((err) => {
        setError(
          err instanceof Error ? err.message : "Failed to load lifecycle state",
        );
      })
      .finally(() => {
        setLoading(false);
      });
  }, [loadLifecycleState, selectedDb, setSearchParams]);

  const refreshLifecycleState = useCallback(
    async (preserveScheduleInput: boolean) => {
      if (!selectedDb) return;
      setRefreshing(true);
      setError("");
      try {
        await loadLifecycleState(selectedDb, preserveScheduleInput);
      } catch (err) {
        setError(
          err instanceof Error
            ? err.message
            : "Failed to refresh lifecycle state",
        );
      } finally {
        setRefreshing(false);
      }
    },
    [loadLifecycleState, selectedDb],
  );

  const openAction = (kind: ActionKind) => {
    if (!selectedDb) return;
    setSuccess("");
    switch (kind) {
      case "pause":
        setPendingAction({
          kind,
          title: `Pause automatic lifecycle for ${selectedDb}?`,
          description:
            "Automatic background MVCC maintenance will stop running until you explicitly resume it. Manual prune runs will still be available.",
          confirmLabel: "Pause lifecycle",
          variant: "danger",
        });
        break;
      case "resume":
        setPendingAction({
          kind,
          title: `Resume automatic lifecycle for ${selectedDb}?`,
          description:
            "Automatic lifecycle scheduling will resume using the current runtime interval.",
          confirmLabel: "Resume lifecycle",
          variant: "success",
        });
        break;
      case "prune":
        setPendingAction({
          kind,
          title: `Run prune now on ${selectedDb}?`,
          description:
            "This starts an immediate MVCC prune cycle. Use it when the database is in a safe maintenance window.",
          confirmLabel: "Run prune now",
          variant: "danger",
        });
        break;
      case "schedule":
        setPendingAction({
          kind,
          title:
            scheduleInput.trim() === "0s"
              ? `Switch ${selectedDb} to manual-only mode?`
              : `Apply lifecycle interval ${scheduleInput.trim()} to ${selectedDb}?`,
          description:
            scheduleInput.trim() === "0s"
              ? "Automatic lifecycle runs will be disabled until you set a non-zero interval or explicitly resume scheduled maintenance."
              : "This updates the runtime lifecycle cadence immediately without restarting the server.",
          confirmLabel:
            scheduleInput.trim() === "0s"
              ? "Enable manual-only mode"
              : "Apply schedule",
          variant: scheduleInput.trim() === "0s" ? "danger" : "primary",
        });
        break;
    }
  };

  const executePendingAction = async () => {
    if (!pendingAction || !selectedDb) return;
    setActionLoading(pendingAction.kind);
    setError("");
    setSuccess("");
    try {
      switch (pendingAction.kind) {
        case "pause":
          await api.pauseDatabaseLifecycle(selectedDb);
          setSuccess(`Paused automatic lifecycle work for ${selectedDb}.`);
          break;
        case "resume":
          await api.resumeDatabaseLifecycle(selectedDb);
          setSuccess(`Resumed automatic lifecycle work for ${selectedDb}.`);
          break;
        case "prune":
          await api.triggerDatabaseLifecyclePrune(selectedDb);
          setSuccess(`Triggered an on-demand prune cycle for ${selectedDb}.`);
          break;
        case "schedule":
          if (!scheduleInput.trim()) {
            throw new Error(
              "Enter a lifecycle interval such as 30s, 2m, or 0s.",
            );
          }
          await api.setDatabaseLifecycleSchedule(
            selectedDb,
            scheduleInput.trim(),
          );
          setSuccess(
            scheduleInput.trim() === "0s"
              ? `${selectedDb} is now in manual-only mode.`
              : `Updated ${selectedDb} lifecycle interval to ${scheduleInput.trim()}.`,
          );
          break;
      }
      setPendingAction(null);
      await refreshLifecycleState(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Lifecycle action failed");
    } finally {
      setActionLoading(null);
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
    <PageLayout>
      <PageHeader
        title="MVCC Lifecycle"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={() => refreshLifecycleState(true)}
              disabled={!selectedDb || refreshing}
              icon={RefreshCw}
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </Button>
          </div>
        }
      />

      <main className="max-w-6xl mx-auto p-6 space-y-6">
        {error && (
          <Alert
            type="error"
            message={error}
            dismissible
            onDismiss={() => setError("")}
          />
        )}
        {success && (
          <Alert
            type="success"
            message={success}
            dismissible
            onDismiss={() => setSuccess("")}
          />
        )}

        <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
            <div className="space-y-2">
              <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                <Database className="w-5 h-5 text-nornic-accent" />
                Lifecycle Control Plane
              </h2>
              <p className="text-sm text-norse-silver max-w-3xl">
                This panel drives the admin lifecycle API end to end: status
                inspection, debt review, pause or resume, manual prune, and
                runtime schedule control.
              </p>
            </div>
            <div className="w-full lg:w-72">
              <label
                htmlFor="lifecycle-db"
                className="block text-sm font-medium text-norse-silver mb-2"
              >
                Target database
              </label>
              <select
                id="lifecycle-db"
                value={selectedDb}
                onChange={(event) => setSelectedDb(event.target.value)}
                className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
              >
                {supportedDatabases.map((db) => (
                  <option key={db.name} value={db.name}>
                    {db.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          {unsupportedDatabases.length > 0 && (
            <Alert
              type="warning"
              title="Unsupported databases hidden from the selector"
              message={`Lifecycle controls are only available for standard databases. Ignored here: ${unsupportedDatabases.map((db) => db.name).join(", ")}.`}
            />
          )}
        </section>

        {!selectedDb ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-12 text-center">
            <Database className="w-12 h-12 text-norse-silver mx-auto mb-3" />
            <h2 className="text-lg font-semibold text-white mb-2">
              No lifecycle-capable database available
            </h2>
            <p className="text-norse-silver">
              Create or open a standard database to manage MVCC lifecycle
              operations.
            </p>
          </section>
        ) : loading ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-12 flex items-center justify-center">
            <div className="w-10 h-10 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
          </section>
        ) : (
          <>
            <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
              <div
                className={`rounded-lg border p-4 ${pressureTone(status?.pressure_band)}`}
              >
                <div className="text-xs uppercase tracking-wide mb-2 opacity-80">
                  Pressure band
                </div>
                <div className="text-2xl font-semibold capitalize">
                  {status?.pressure_band || "unknown"}
                </div>
                <div className="text-sm opacity-80 mt-2">
                  Pinned bytes:{" "}
                  {formatBytes(status?.mvcc_bytes_pinned_by_oldest_reader)}
                </div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">
                  Runtime mode
                </div>
                <div className="text-2xl font-semibold text-white">
                  {status?.automatic ? "Automatic" : "Manual-only"}
                </div>
                <div className="text-sm text-norse-silver mt-2">
                  Interval: {status?.cycle_interval || "—"}
                </div>
                <div className="text-sm text-norse-silver">
                  Paused: {status?.paused ? "yes" : "no"}
                </div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">
                  Debt
                </div>
                <div className="text-2xl font-semibold text-white">
                  {formatBytes(status?.mvcc_compaction_debt_bytes)}
                </div>
                <div className="text-sm text-norse-silver mt-2">
                  Keys:{" "}
                  {(status?.mvcc_compaction_debt_keys ?? 0).toLocaleString()}
                </div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">
                  Readers
                </div>
                <div className="text-2xl font-semibold text-white">
                  {(status?.mvcc_active_snapshot_readers ?? 0).toLocaleString()}
                </div>
                <div className="text-sm text-norse-silver mt-2">
                  Oldest age:{" "}
                  {formatDurationSeconds(
                    status?.mvcc_oldest_reader_age_seconds,
                  )}
                </div>
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-[1.1fr_0.9fr] gap-6">
              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-5">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <TimerReset className="w-5 h-5 text-nornic-accent" />
                      Runtime controls
                    </h2>
                    <p className="text-sm text-norse-silver mt-1">
                      Tune the scheduler, switch to manual-only mode, or run
                      maintenance on demand.
                    </p>
                  </div>
                  <div
                    className={`px-3 py-1 rounded-full text-xs font-medium border ${status?.emergency_mode ? "text-red-300 border-red-500/30 bg-red-500/10" : "text-green-300 border-green-500/30 bg-green-500/10"}`}
                  >
                    {status?.emergency_mode
                      ? "Emergency mode active"
                      : "Emergency mode inactive"}
                  </div>
                </div>

                <div className="space-y-3">
                  <FormInput
                    id="lifecycle-interval"
                    label="Automatic lifecycle interval"
                    value={scheduleInput}
                    onChange={setScheduleInput}
                    placeholder="30s, 2m, 5m, 0s"
                  />
                  <div className="flex flex-wrap gap-2">
                    {["30s", "2m", "5m", "15m", "0s"].map((preset) => (
                      <Button
                        key={preset}
                        variant="secondary"
                        size="sm"
                        onClick={() => setScheduleInput(preset)}
                      >
                        {preset === "0s" ? "Manual-only" : preset}
                      </Button>
                    ))}
                  </div>
                  <div className="flex flex-wrap gap-2 pt-2">
                    <Button
                      variant="primary"
                      onClick={() => openAction("schedule")}
                      disabled={!scheduleInput.trim()}
                      icon={Clock3}
                    >
                      Apply schedule
                    </Button>
                    {status?.paused ? (
                      <Button
                        variant="success"
                        onClick={() => openAction("resume")}
                        icon={PlayCircle}
                      >
                        Resume automatic runs
                      </Button>
                    ) : (
                      <Button
                        variant="danger"
                        onClick={() => openAction("pause")}
                        icon={PauseCircle}
                      >
                        Pause automatic runs
                      </Button>
                    )}
                    <Button
                      variant="danger"
                      onClick={() => openAction("prune")}
                      icon={Trash2}
                    >
                      Run prune now
                    </Button>
                  </div>
                </div>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4 pt-2">
                  <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                    <div className="text-sm text-norse-silver mb-1">
                      Last run
                    </div>
                    <div className="text-white font-medium">
                      {formatDurationSeconds(
                        status?.mvcc_prune_run_duration_seconds,
                      )}
                    </div>
                    <div className="text-xs text-norse-fog mt-2">
                      Keys processed:{" "}
                      {(status?.last_run?.keys_processed ?? 0).toLocaleString()}
                    </div>
                    <div className="text-xs text-norse-fog">
                      Versions deleted:{" "}
                      {(
                        status?.last_run?.versions_deleted ?? 0
                      ).toLocaleString()}
                    </div>
                    <div className="text-xs text-norse-fog">
                      Bytes freed: {formatBytes(status?.last_run?.bytes_freed)}
                    </div>
                  </div>
                  <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
                    <div className="text-sm text-norse-silver mb-1">
                      Planner friction
                    </div>
                    <div className="text-white font-medium">
                      {(
                        status?.mvcc_prune_stale_plan_skips_total ?? 0
                      ).toLocaleString()}{" "}
                      stale-plan skips
                    </div>
                    <div className="text-xs text-norse-fog mt-2">
                      Hot contention keys:{" "}
                      {(
                        status?.last_run?.hot_contention_keys ?? 0
                      ).toLocaleString()}
                    </div>
                    <div className="text-xs text-norse-fog">
                      Floor lag versions:{" "}
                      {(status?.mvcc_floor_lag_versions ?? 0).toLocaleString()}
                    </div>
                    <div className="text-xs text-norse-fog">
                      Tombstone max depth:{" "}
                      {(
                        status?.mvcc_tombstone_chain_max_depth ?? 0
                      ).toLocaleString()}
                    </div>
                  </div>
                </div>
              </div>

              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-5">
                <div>
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <Gauge className="w-5 h-5 text-nornic-accent" />
                    Rollups and pressure
                  </h2>
                  <p className="text-sm text-norse-silver mt-1">
                    Short-window summaries help operators see whether debt is
                    improving or churn is winning.
                  </p>
                </div>
                <div className="grid grid-cols-1 gap-4">
                  {(["10s", "60s"] as const).map((windowKey) => {
                    const rollup = status?.rollups?.[windowKey] as
                      | MVCCLifecycleRollup
                      | undefined;
                    return (
                      <div
                        key={windowKey}
                        className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                      >
                        <div className="flex items-center justify-between mb-3">
                          <div className="text-sm font-medium text-white">
                            {windowKey} rollup
                          </div>
                          <div className="text-xs text-norse-fog">
                            Max debt{" "}
                            {formatBytes(rollup?.compaction_debt_bytes_max)}
                          </div>
                        </div>
                        <div className="grid grid-cols-2 gap-3 text-sm">
                          <div>
                            <div className="text-norse-silver">Prune runs</div>
                            <div className="text-white font-medium">
                              {(rollup?.prune_runs ?? 0).toLocaleString()}
                            </div>
                          </div>
                          <div>
                            <div className="text-norse-silver">Bytes freed</div>
                            <div className="text-white font-medium">
                              {formatBytes(rollup?.bytes_freed)}
                            </div>
                          </div>
                          <div>
                            <div className="text-norse-silver">
                              Versions deleted
                            </div>
                            <div className="text-white font-medium">
                              {(rollup?.versions_deleted ?? 0).toLocaleString()}
                            </div>
                          </div>
                          <div>
                            <div className="text-norse-silver">
                              Fence mismatches
                            </div>
                            <div className="text-white font-medium">
                              {(rollup?.fence_mismatches ?? 0).toLocaleString()}
                            </div>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
                <Alert
                  type={
                    status?.pressure_band === "critical"
                      ? "error"
                      : status?.pressure_band === "high"
                        ? "warning"
                        : "info"
                  }
                  title={
                    status?.pressure_band === "critical"
                      ? "Critical pressure"
                      : status?.pressure_band === "high"
                        ? "High pressure"
                        : "Healthy pressure"
                  }
                  message={
                    status?.pressure_band === "critical"
                      ? "New long-lived snapshots may be rejected and existing readers may be forcefully expired if pressure remains critical."
                      : status?.pressure_band === "high"
                        ? "Long-lived snapshots are under tighter admission rules. Consider pruning during a quieter window."
                        : "Lifecycle pressure is currently within the normal operating envelope."
                  }
                />
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-[1.2fr_0.8fr] gap-6">
              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <Activity className="w-5 h-5 text-nornic-accent" />
                      Top debt keys
                    </h2>
                    <p className="text-sm text-norse-silver mt-1">
                      Inspect the hottest logical keys before forcing a prune or
                      tightening cadence.
                    </p>
                  </div>
                  <div className="w-32">
                    <FormInput
                      id="debt-limit"
                      label="Rows"
                      value={debtLimit}
                      onChange={setDebtLimit}
                    />
                  </div>
                </div>
                <div className="nornic-grid border border-norse-rune rounded-lg p-4">
                  <UiGrid options={debtGridOptions} />
                </div>
              </div>

              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div>
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <ShieldCheck className="w-5 h-5 text-nornic-accent" />
                    Per-namespace summary
                  </h2>
                  <p className="text-sm text-norse-silver mt-1">
                    Use this view to confirm a single namespace is not
                    monopolizing compaction work.
                  </p>
                </div>
                <div className="space-y-3">
                  {status?.per_namespace &&
                  Object.keys(status.per_namespace).length > 0 ? (
                    Object.entries(status.per_namespace).map(
                      ([namespace, metrics]) => (
                        <div
                          key={namespace}
                          className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                        >
                          <div className="flex items-center justify-between mb-2">
                            <div className="font-medium text-white">
                              {namespace}
                            </div>
                            <div className="text-xs text-norse-fog">
                              {metrics.compaction_debt_keys.toLocaleString()}{" "}
                              debt keys
                            </div>
                          </div>
                          <div className="grid grid-cols-2 gap-3 text-sm">
                            <div>
                              <div className="text-norse-silver">
                                Debt bytes
                              </div>
                              <div className="text-white">
                                {formatBytes(metrics.compaction_debt_bytes)}
                              </div>
                            </div>
                            <div>
                              <div className="text-norse-silver">Prunable</div>
                              <div className="text-white">
                                {formatBytes(metrics.prunable_bytes_total)}
                              </div>
                            </div>
                            <div>
                              <div className="text-norse-silver">
                                Pruned total
                              </div>
                              <div className="text-white">
                                {formatBytes(metrics.pruned_bytes_total)}
                              </div>
                            </div>
                          </div>
                        </div>
                      ),
                    )
                  ) : (
                    <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4 text-sm text-norse-silver">
                      No per-namespace metrics are currently populated for this
                      database.
                    </div>
                  )}
                </div>
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-[0.95fr_1.05fr] gap-6">
              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div>
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <ShieldAlert className="w-5 h-5 text-nornic-accent" />
                    Active readers
                  </h2>
                  <p className="text-sm text-norse-silver mt-1">
                    Long-lived readers are the primary reason debt stays pinned
                    behind the floor.
                  </p>
                </div>
                <div className="nornic-grid border border-norse-rune rounded-lg p-4">
                  <UiGrid options={readerGridOptions} />
                </div>
              </div>

              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div>
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <AlertTriangle className="w-5 h-5 text-nornic-accent" />
                    API surface used by this panel
                  </h2>
                  <p className="text-sm text-norse-silver mt-1">
                    Operators can match every control here to the admin
                    endpoints behind it.
                  </p>
                </div>
                <div className="space-y-3 text-sm">
                  {[
                    [
                      "GET",
                      `/admin/databases/${selectedDb}/mvcc/status`,
                      "Load live pressure, runtime mode, last run, readers, rollups, and per-namespace metrics.",
                    ],
                    [
                      "GET",
                      `/admin/databases/${selectedDb}/mvcc/debt?limit=${numericDebtLimit}`,
                      "Inspect the highest-debt logical keys before you change cadence or force pruning.",
                    ],
                    [
                      "POST",
                      `/admin/databases/${selectedDb}/mvcc/pause`,
                      "Pause automatic lifecycle work while keeping manual prune available.",
                    ],
                    [
                      "POST",
                      `/admin/databases/${selectedDb}/mvcc/resume`,
                      "Resume automatic lifecycle work after an incident or ingest window.",
                    ],
                    [
                      "POST",
                      `/admin/databases/${selectedDb}/mvcc/prune`,
                      "Trigger an immediate prune cycle for a controlled maintenance window.",
                    ],
                    [
                      "POST",
                      `/admin/databases/${selectedDb}/mvcc/schedule`,
                      "Apply a runtime lifecycle interval or switch to manual-only mode with 0s.",
                    ],
                  ].map(([method, path, description]) => (
                    <div
                      key={path}
                      className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                    >
                      <div className="flex flex-wrap items-center gap-2 mb-2">
                        <span
                          className={`px-2 py-1 rounded text-xs font-semibold ${method === "GET" ? "bg-blue-500/15 text-blue-300" : "bg-valhalla-gold/15 text-valhalla-gold"}`}
                        >
                          {method}
                        </span>
                        <code className="text-xs text-white break-all">
                          {path}
                        </code>
                      </div>
                      <p className="text-norse-silver">{description}</p>
                    </div>
                  ))}
                </div>
              </div>
            </section>
          </>
        )}
      </main>

      <Modal
        isOpen={pendingAction !== null}
        onClose={() => setPendingAction(null)}
        title={pendingAction?.title}
        size="md"
      >
        <div className="space-y-4">
          <p className="text-norse-silver">{pendingAction?.description}</p>
          <div className="rounded-lg border border-red-500/20 bg-red-500/5 p-3 text-sm text-red-200">
            Confirm this action only if the current database workload can
            tolerate the change.
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button
              type="button"
              variant="secondary"
              onClick={() => setPendingAction(null)}
              disabled={actionLoading !== null}
            >
              Cancel
            </Button>
            <Button
              type="button"
              variant={pendingAction?.variant || "primary"}
              onClick={executePendingAction}
              disabled={actionLoading !== null}
            >
              {actionLoading === pendingAction?.kind
                ? "Applying..."
                : pendingAction?.confirmLabel}
            </Button>
          </div>
        </div>
      </Modal>
    </PageLayout>
  );
}
