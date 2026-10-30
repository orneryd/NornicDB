import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { UiGrid } from "@ornery/ui-grid-react";
import type { GridCellTemplateContext, GridColumnDef, GridOptions, GridRecord } from "@ornery/ui-grid-core";
import {
  AlertTriangle,
  Clock3,
  Database,
  Edit2,
  PlayCircle,
  Plus,
  RefreshCw,
  Shield,
  Trash2,
} from "lucide-react";
import { Alert } from "../components/common/Alert";
import { Button } from "../components/common/Button";
import { FormInput } from "../components/common/FormInput";
import { Modal } from "../components/common/Modal";
import { PageHeader } from "../components/common/PageHeader";
import { PageLayout } from "../components/common/PageLayout";
import {
  api,
  type RetentionCategory,
  type RetentionErasureRequest,
  type RetentionLegalHold,
  type RetentionPolicy,
  type RetentionStatus,
} from "../utils/api";
import { BASE_PATH, joinBasePath } from "../utils/basePath";

const NANOSECONDS_PER_DAY = 24 * 60 * 60 * 1_000_000_000;

const RETENTION_CATEGORIES: Array<{
  value: RetentionCategory;
  label: string;
  description: string;
}> = [
  { value: "USER", label: "User", description: "General user-created graph data." },
  { value: "PII", label: "PII", description: "Personally identifiable information." },
  { value: "PHI", label: "PHI", description: "Protected health information." },
  { value: "FINANCIAL", label: "Financial", description: "Financial records and ledgers." },
  { value: "AUDIT", label: "Audit", description: "Audit trails and activity history." },
  { value: "ANALYTICS", label: "Analytics", description: "Derived metrics and telemetry." },
  { value: "LEGAL", label: "Legal", description: "Matter-driven legal records." },
  { value: "SYSTEM", label: "System", description: "Platform and infrastructure data." },
  { value: "BACKUP", label: "Backup", description: "Backup copies and restore assets." },
  { value: "ARCHIVE", label: "Archive", description: "Already archived records." },
];

type PendingAction =
  | { kind: "load-defaults" }
  | { kind: "run-sweep" }
  | { kind: "delete-policy"; policy: RetentionPolicy }
  | { kind: "release-hold"; hold: RetentionLegalHold }
  | { kind: "process-erasure"; request: RetentionErasureRequest };

interface PolicyFormState {
  id: string;
  name: string;
  category: RetentionCategory;
  retentionDays: string;
  indefinite: boolean;
  archiveBeforeDelete: boolean;
  archivePath: string;
  complianceFrameworks: string;
  active: boolean;
  description: string;
}

interface HoldFormState {
  id: string;
  description: string;
  matter: string;
  placedBy: string;
  expiresAt: string;
  subjectIds: string;
  categories: RetentionCategory[];
  active: boolean;
}

interface ErasureFormState {
  subjectId: string;
  subjectEmail: string;
}

function emptyPolicyForm(): PolicyFormState {
  return {
    id: "",
    name: "",
    category: "USER",
    retentionDays: "365",
    indefinite: false,
    archiveBeforeDelete: false,
    archivePath: "",
    complianceFrameworks: "",
    active: true,
    description: "",
  };
}

function emptyHoldForm(): HoldFormState {
  return {
    id: "",
    description: "",
    matter: "",
    placedBy: "",
    expiresAt: "",
    subjectIds: "",
    categories: [],
    active: true,
  };
}

function emptyErasureForm(): ErasureFormState {
  return {
    subjectId: "",
    subjectEmail: "",
  };
}

function formatTimestamp(value?: string): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatRetentionPeriod(policy: RetentionPolicy): string {
  if (policy.retention_period?.Indefinite) {
    return "Indefinite";
  }
  const days = Math.round((policy.retention_period?.Duration ?? 0) / NANOSECONDS_PER_DAY);
  if (days <= 0) {
    return "Custom";
  }
  if (days % 365 === 0) {
    return `${days / 365}y`;
  }
  if (days % 30 === 0) {
    return `${days / 30}mo`;
  }
  return `${days}d`;
}

function csvToList(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function policyToForm(policy: RetentionPolicy): PolicyFormState {
  const days = Math.round((policy.retention_period?.Duration ?? 0) / NANOSECONDS_PER_DAY);
  return {
    id: policy.id,
    name: policy.name,
    category: policy.category,
    retentionDays: days > 0 ? String(days) : "",
    indefinite: Boolean(policy.retention_period?.Indefinite),
    archiveBeforeDelete: Boolean(policy.archive_before_delete),
    archivePath: policy.archive_path ?? "",
    complianceFrameworks: (policy.compliance_frameworks ?? []).join(", "),
    active: Boolean(policy.active),
    description: policy.description ?? "",
  };
}

function policyFormToPayload(form: PolicyFormState): RetentionPolicy {
  const id = form.id.trim();
  const name = form.name.trim();
  if (!id) {
    throw new Error("Policy ID is required.");
  }
  if (!name) {
    throw new Error("Policy name is required.");
  }

  const indefinite = form.indefinite;
  const days = Number.parseInt(form.retentionDays, 10);
  if (!indefinite && (!Number.isFinite(days) || days <= 0)) {
    throw new Error("Retention days must be a positive integer.");
  }
  if (form.archiveBeforeDelete && !form.archivePath.trim()) {
    throw new Error("Archive path is required when archive-before-delete is enabled.");
  }

  return {
    id,
    name,
    category: form.category,
    retention_period: {
      Duration: indefinite ? 0 : days * NANOSECONDS_PER_DAY,
      Indefinite: indefinite,
    },
    archive_before_delete: form.archiveBeforeDelete,
    archive_path: form.archivePath.trim() || undefined,
    compliance_frameworks: csvToList(form.complianceFrameworks),
    active: form.active,
    description: form.description.trim() || undefined,
  };
}

function holdToCategorySummary(hold: RetentionLegalHold): string {
  if (!hold.categories || hold.categories.length === 0) {
    return "All categories";
  }
  return hold.categories.join(", ");
}

function holdToSubjectSummary(hold: RetentionLegalHold): string {
  if (!hold.subject_ids || hold.subject_ids.length === 0) {
    return "All subjects";
  }
  return hold.subject_ids.join(", ");
}

function canProcessErasure(request: RetentionErasureRequest): boolean {
  return ["PENDING", "PARTIAL", "FAILED"].includes(request.status);
}

export function RetentionAdmin() {
  const navigate = useNavigate();
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [disabledMessage, setDisabledMessage] = useState("");
  const [status, setStatus] = useState<RetentionStatus | null>(null);
  const [policies, setPolicies] = useState<RetentionPolicy[]>([]);
  const [holds, setHolds] = useState<RetentionLegalHold[]>([]);
  const [erasures, setErasures] = useState<RetentionErasureRequest[]>([]);

  const [policyModalOpen, setPolicyModalOpen] = useState(false);
  const [policySaving, setPolicySaving] = useState(false);
  const [policyModalMode, setPolicyModalMode] = useState<"create" | "edit">("create");
  const [policyForm, setPolicyForm] = useState<PolicyFormState>(emptyPolicyForm());

  const [holdModalOpen, setHoldModalOpen] = useState(false);
  const [holdSaving, setHoldSaving] = useState(false);
  const [holdForm, setHoldForm] = useState<HoldFormState>(emptyHoldForm());

  const [erasureModalOpen, setErasureModalOpen] = useState(false);
  const [erasureSaving, setErasureSaving] = useState(false);
  const [erasureForm, setErasureForm] = useState<ErasureFormState>(emptyErasureForm());

  const [pendingAction, setPendingAction] = useState<PendingAction | null>(null);
  const [actionLoading, setActionLoading] = useState(false);

  const activePolicyCount = useMemo(
    () => policies.filter((policy) => policy.active).length,
    [policies],
  );

  const policyGridData = useMemo<GridRecord[]>(
    () => policies.map((policy) => ({ ...policy, __gridId: policy.id })),
    [policies],
  );

  const policyColumnDefs = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "name",
        displayName: "Name",
        field: "name",
        width: "minmax(12rem, 1.1fr)",
      },
      {
        name: "id",
        displayName: "ID",
        field: "id",
        width: "minmax(11rem, 1fr)",
      },
      {
        name: "description",
        displayName: "Description",
        field: "description",
        width: "minmax(16rem, 1.5fr)",
      },
      {
        name: "category",
        displayName: "Category",
        field: "category",
        width: "150px",
      },
      {
        name: "retention",
        displayName: "Retention",
        width: "120px",
        valueGetter: (row) => formatRetentionPeriod(row as unknown as RetentionPolicy),
      },
      {
        name: "archive",
        displayName: "Archive",
        width: "minmax(12rem, 1.2fr)",
        valueGetter: (row) => {
          const policy = row as unknown as RetentionPolicy;
          return policy.archive_before_delete ? policy.archive_path || "Archive enabled" : "Delete directly";
        },
      },
      {
        name: "active",
        displayName: "State",
        field: "active",
        width: "120px",
      },
      {
        name: "actions",
        displayName: "Actions",
        width: "170px",
        enableSorting: false,
        enableFiltering: false,
      },
    ],
    [],
  );

  const policyGridOptions = useMemo<GridOptions>(
    () => ({
      id: "retention-policy-grid",
      data: policyGridData,
      columnDefs: policyColumnDefs,
      rowIdentity: (row) => String(row.__gridId),
      enableSorting: true,
      enableFiltering: true,
      viewportHeight: 480,
      emptyMessage: "No retention policies loaded. Add one manually or load the built-in defaults.",
    }),
    [policyColumnDefs, policyGridData],
  );

  const loadRetention = useCallback(async () => {
    setError("");
    const nextStatus = await api.getRetentionStatus();
    const [nextPolicies, nextHolds, nextErasures] = await Promise.all([
      api.listRetentionPolicies(),
      api.listRetentionHolds(),
      api.listRetentionErasures(),
    ]);
    setDisabledMessage("");
    setStatus(nextStatus);
    setPolicies(nextPolicies);
    setHolds(nextHolds);
    setErasures(nextErasures);
  }, []);

  useEffect(() => {
    fetch(joinBasePath(BASE_PATH, "/auth/me"), { credentials: "include" })
      .then((res) => res.json())
      .then(async (data) => {
        const roles = data.roles || [];
        if (!roles.includes("admin")) {
          navigate("/security");
          return;
        }
        try {
          await loadRetention();
        } catch (err) {
          const message = err instanceof Error ? err.message : "Failed to load retention controls";
          if (message.toLowerCase().includes("disabled")) {
            setDisabledMessage(message);
            setStatus(null);
            setPolicies([]);
            setHolds([]);
            setErasures([]);
          } else {
            setError(message);
          }
        } finally {
          setCheckingAuth(false);
          setLoading(false);
        }
      })
      .catch(() => {
        navigate("/security");
      });
  }, [loadRetention, navigate]);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    setError("");
    setSuccess("");
    try {
      await loadRetention();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Failed to refresh retention controls";
      if (message.toLowerCase().includes("disabled")) {
        setDisabledMessage(message);
        setStatus(null);
        setPolicies([]);
        setHolds([]);
        setErasures([]);
      } else {
        setError(message);
      }
    } finally {
      setRefreshing(false);
    }
  }, [loadRetention]);

  const openCreatePolicy = () => {
    setPolicyModalMode("create");
    setPolicyForm(emptyPolicyForm());
    setPolicyModalOpen(true);
  };

  const openEditPolicy = (policy: RetentionPolicy) => {
    setPolicyModalMode("edit");
    setPolicyForm(policyToForm(policy));
    setPolicyModalOpen(true);
  };

  const updatePolicyInline = useCallback(
    async (policy: RetentionPolicy, patch: Partial<RetentionPolicy>) => {
      setError("");
      setSuccess("");
      try {
        const {
          __gridId: _,
          retention: _retention,
          archive: _archive,
          ...cleanPolicy
        } = policy as RetentionPolicy & { __gridId?: unknown; retention?: unknown; archive?: unknown };
        await api.updateRetentionPolicy(policy.id, { ...cleanPolicy, ...patch });
        await refresh();
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to update retention policy");
        await refresh();
      }
    },
    [refresh],
  );

  const renderPolicyCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & RetentionPolicy;

    if (ctx.column.name === "description") {
      return (
        <div className="py-1 text-sm text-norse-silver whitespace-pre-wrap">
          {row.description || "—"}
        </div>
      );
    }

    if (ctx.column.name === "category") {
      return (
        <div className="py-1" onClick={(e) => e.stopPropagation()}>
          <select
            value={row.category}
            onChange={(e) =>
              void updatePolicyInline(row, {
                category: e.target.value as RetentionCategory,
              })
            }
            className="w-full rounded border border-norse-rune bg-norse-stone px-2 py-1 text-sm text-white"
          >
            {RETENTION_CATEGORIES.map((category) => (
              <option key={category.value} value={category.value}>
                {category.label}
              </option>
            ))}
          </select>
        </div>
      );
    }

    if (ctx.column.name === "active") {
      return (
        <div className="py-1" onClick={(e) => e.stopPropagation()}>
          <button
            type="button"
            onClick={() => void updatePolicyInline(row, { active: !row.active })}
            className={`px-2 py-1 rounded text-xs font-medium ${row.active ? "bg-green-500/20 text-green-300" : "bg-norse-stone text-norse-silver"}`}
          >
            {row.active ? "Active" : "Inactive"}
          </button>
          {(row.compliance_frameworks ?? []).length > 0 ? (
            <div className="text-xs text-norse-fog mt-2">
              {(row.compliance_frameworks ?? []).join(", ")}
            </div>
          ) : null}
        </div>
      );
    }

    if (ctx.column.name === "actions") {
      return (
        <div className="flex justify-end gap-2 py-1" onClick={(e) => e.stopPropagation()}>
          <Button
            variant="secondary"
            size="sm"
            icon={Edit2}
            onClick={() => openEditPolicy(row)}
          >
            Edit
          </Button>
          <Button
            variant="danger"
            size="sm"
            icon={Trash2}
            onClick={() => setPendingAction({ kind: "delete-policy", policy: row })}
          >
            Delete
          </Button>
        </div>
      );
    }

    if (ctx.column.name === "name" || ctx.column.name === "id") {
      return <div className={`py-1 ${ctx.column.name === "name" ? "font-medium text-white" : "text-xs text-norse-fog"}`}>{String(ctx.value ?? "—")}</div>;
    }

    if (ctx.column.name === "retention" || ctx.column.name === "archive") {
      return <div className="py-1 text-white">{String(ctx.value ?? "—")}</div>;
    }

    return null;
  };

  const policyCellRenderers = useMemo(() => ({
    name: renderPolicyCell,
    id: renderPolicyCell,
    description: renderPolicyCell,
    category: renderPolicyCell,
    retention: renderPolicyCell,
    archive: renderPolicyCell,
    active: renderPolicyCell,
    actions: renderPolicyCell,
  }), [renderPolicyCell]);

  const savePolicy = async () => {
    setPolicySaving(true);
    setError("");
    setSuccess("");
    try {
      const payload = policyFormToPayload(policyForm);
      if (policyModalMode === "create") {
        await api.createRetentionPolicy(payload);
        setSuccess(`Created retention policy ${payload.id}.`);
      } else {
        await api.updateRetentionPolicy(policyForm.id, payload);
        setSuccess(`Updated retention policy ${payload.id}.`);
      }
      setPolicyModalOpen(false);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save retention policy");
    } finally {
      setPolicySaving(false);
    }
  };

  const saveHold = async () => {
    setHoldSaving(true);
    setError("");
    setSuccess("");
    try {
      if (!holdForm.id.trim()) {
        throw new Error("Hold ID is required.");
      }
      if (!holdForm.description.trim()) {
        throw new Error("Hold description is required.");
      }
      if (!holdForm.placedBy.trim()) {
        throw new Error("Placed-by value is required.");
      }
      await api.createRetentionHold({
        id: holdForm.id.trim(),
        description: holdForm.description.trim(),
        matter: holdForm.matter.trim() || undefined,
        placed_by: holdForm.placedBy.trim(),
        expires_at: holdForm.expiresAt ? new Date(holdForm.expiresAt).toISOString() : undefined,
        subject_ids: csvToList(holdForm.subjectIds),
        categories: holdForm.categories,
        active: holdForm.active,
      });
      setHoldModalOpen(false);
      setHoldForm(emptyHoldForm());
      setSuccess(`Created legal hold ${holdForm.id.trim()}.`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create legal hold");
    } finally {
      setHoldSaving(false);
    }
  };

  const saveErasureRequest = async () => {
    setErasureSaving(true);
    setError("");
    setSuccess("");
    try {
      if (!erasureForm.subjectId.trim()) {
        throw new Error("Subject ID is required.");
      }
      await api.createRetentionErasureRequest({
        subject_id: erasureForm.subjectId.trim(),
        subject_email: erasureForm.subjectEmail.trim() || undefined,
      });
      setErasureModalOpen(false);
      setErasureForm(emptyErasureForm());
      setSuccess(`Created erasure request for ${erasureForm.subjectId.trim()}.`);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create erasure request");
    } finally {
      setErasureSaving(false);
    }
  };

  const confirmActionLabel = useMemo(() => {
    switch (pendingAction?.kind) {
      case "load-defaults":
        return "Load defaults";
      case "run-sweep":
        return "Run sweep";
      case "delete-policy":
        return "Delete policy";
      case "release-hold":
        return "Release hold";
      case "process-erasure":
        return "Process request";
      default:
        return "Apply";
    }
  }, [pendingAction]);

  const confirmActionTitle = useMemo(() => {
    switch (pendingAction?.kind) {
      case "load-defaults":
        return "Load built-in retention defaults?";
      case "run-sweep":
        return "Run a manual retention sweep now?";
      case "delete-policy":
        return `Delete policy ${pendingAction.policy.id}?`;
      case "release-hold":
        return `Release hold ${pendingAction.hold.id}?`;
      case "process-erasure":
        return `Process erasure request ${pendingAction.request.id}?`;
      default:
        return "Confirm action";
    }
  }, [pendingAction]);

  const confirmActionDescription = useMemo(() => {
    switch (pendingAction?.kind) {
      case "load-defaults":
        return "This loads the built-in compliance starter policies into the active retention manager.";
      case "run-sweep":
        return "This triggers an immediate pass across retention-managed records using the current policies and holds.";
      case "delete-policy":
        return "Removing a policy changes future retention decisions for that category. Existing deletes are not rolled back.";
      case "release-hold":
        return "Once released, affected data can be deleted by sweep or erasure processing if policies allow it.";
      case "process-erasure":
        return "This executes the request against current subject selectors and legal holds, and may delete matching graph records.";
      default:
        return "Apply this retention action.";
    }
  }, [pendingAction]);

  const executePendingAction = async () => {
    if (!pendingAction) return;
    setActionLoading(true);
    setError("");
    setSuccess("");
    try {
      switch (pendingAction.kind) {
        case "load-defaults": {
          const result = await api.loadDefaultRetentionPolicies();
          setSuccess(
            `Loaded ${result.loaded} default policies, skipped ${result.skipped}, total now ${result.total}.`,
          );
          break;
        }
        case "run-sweep":
          await api.triggerRetentionSweep();
          setSuccess("Triggered a retention sweep.");
          break;
        case "delete-policy":
          await api.deleteRetentionPolicy(pendingAction.policy.id);
          setSuccess(`Deleted retention policy ${pendingAction.policy.id}.`);
          break;
        case "release-hold":
          await api.releaseRetentionHold(pendingAction.hold.id);
          setSuccess(`Released legal hold ${pendingAction.hold.id}.`);
          break;
        case "process-erasure":
          await api.processRetentionErasure(pendingAction.request.id);
          setSuccess(`Processed erasure request ${pendingAction.request.id}.`);
          break;
      }
      setPendingAction(null);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Retention action failed");
    } finally {
      setActionLoading(false);
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
        title="Retention Policies"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <div className="flex gap-2">
            <Button
              variant="secondary"
              onClick={refresh}
              disabled={refreshing}
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
                Retention control plane
              </h2>
              <p className="text-sm text-norse-silver max-w-3xl">
                Configure category-based retention the way operators expect from a Neo4j-style admin surface: policy catalog, legal holds, erasure queue, and manual sweep controls in one place.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="secondary"
                onClick={() => setPendingAction({ kind: "load-defaults" })}
                disabled={Boolean(disabledMessage) || loading}
              >
                Load defaults
              </Button>
              <Button
                variant="primary"
                onClick={() => setPendingAction({ kind: "run-sweep" })}
                disabled={Boolean(disabledMessage) || loading}
                icon={PlayCircle}
              >
                Run sweep now
              </Button>
            </div>
          </div>

          {disabledMessage ? (
            <Alert
              type="warning"
              title="Retention is disabled"
              message={`${disabledMessage}. Enable compliance.retention_enabled before using this control plane.`}
            />
          ) : null}
        </section>

        {loading ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-12 flex items-center justify-center">
            <div className="w-10 h-10 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
          </section>
        ) : disabledMessage ? (
          <section className="bg-norse-shadow border border-norse-rune rounded-lg p-8 space-y-4">
            <h2 className="text-lg font-semibold text-white">How to turn retention on</h2>
            <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4 text-sm text-norse-silver space-y-2">
              <p>Set <span className="text-white font-medium">compliance.retention_enabled</span> to <span className="text-white font-medium">true</span>.</p>
              <p>Optionally set <span className="text-white font-medium">retention.sweep_interval</span>, <span className="text-white font-medium">retention.default_policies</span>, and inline policy definitions.</p>
              <p>Once enabled, refresh this page to manage policies, legal holds, erasure requests, and manual sweeps.</p>
            </div>
          </section>
        ) : (
          <>
            <section className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
              <div className="rounded-lg border border-green-500/30 bg-green-500/10 p-4 text-green-200">
                <div className="text-xs uppercase tracking-wide mb-2 opacity-80">Runtime</div>
                <div className="text-2xl font-semibold">Enabled</div>
                <div className="text-sm opacity-80 mt-2">Last refresh: {formatTimestamp(status?.timestamp)}</div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">Policies</div>
                <div className="text-2xl font-semibold text-white">{status?.policy_count ?? 0}</div>
                <div className="text-sm text-norse-silver mt-2">Active: {activePolicyCount}</div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">Legal holds</div>
                <div className="text-2xl font-semibold text-white">{status?.hold_count ?? 0}</div>
                <div className="text-sm text-norse-silver mt-2">Blocking erasure and sweep deletions</div>
              </div>
              <div className="rounded-lg border border-norse-rune bg-norse-shadow p-4">
                <div className="text-xs uppercase tracking-wide text-norse-silver mb-2">Erasure queue</div>
                <div className="text-2xl font-semibold text-white">{status?.erasure_count ?? 0}</div>
                <div className="text-sm text-norse-silver mt-2">GDPR subject requests in manager state</div>
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-[1.15fr_0.85fr] gap-6">
              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <Clock3 className="w-5 h-5 text-nornic-accent" />
                      Policy catalog
                    </h2>
                    <p className="text-sm text-norse-silver mt-1">
                      Retention is configured per category with explicit duration, archive behavior, and activation state.
                    </p>
                  </div>
                  <Button variant="primary" icon={Plus} onClick={openCreatePolicy}>
                    Add policy
                  </Button>
                </div>

                <div className="nornic-grid border border-norse-rune rounded-lg p-4">
                  <UiGrid options={policyGridOptions} cellRenderers={policyCellRenderers} />
                </div>
              </div>

              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div>
                  <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                    <Shield className="w-5 h-5 text-nornic-accent" />
                    Category guide
                  </h2>
                  <p className="text-sm text-norse-silver mt-1">
                    Categories are the control surface this UI exposes, which maps cleanly to the retention manager and matches the way operators reason about policy domains.
                  </p>
                </div>
                <div className="space-y-3">
                  {RETENTION_CATEGORIES.map((category) => (
                    <div
                      key={category.value}
                      className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                    >
                      <div className="flex items-center justify-between gap-3">
                        <div className="font-medium text-white">{category.label}</div>
                        <div className="text-xs text-norse-fog">{category.value}</div>
                      </div>
                      <p className="text-sm text-norse-silver mt-2">{category.description}</p>
                    </div>
                  ))}
                </div>
              </div>
            </section>

            <section className="grid grid-cols-1 xl:grid-cols-[1.05fr_0.95fr] gap-6">
              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <Shield className="w-5 h-5 text-nornic-accent" />
                      Legal holds
                    </h2>
                    <p className="text-sm text-norse-silver mt-1">
                      Holds block deletion and erasure across the selected subject set or category set.
                    </p>
                  </div>
                  <Button variant="primary" icon={Plus} onClick={() => setHoldModalOpen(true)}>
                    Add hold
                  </Button>
                </div>

                <div className="space-y-3">
                  {holds.length === 0 ? (
                    <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4 text-sm text-norse-silver">
                      No active legal holds are registered.
                    </div>
                  ) : (
                    holds.map((hold) => (
                      <div
                        key={hold.id}
                        className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                      >
                        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                          <div className="space-y-2">
                            <div className="flex items-center gap-2 flex-wrap">
                              <div className="font-medium text-white">{hold.id}</div>
                              <span
                                className={`px-2 py-1 rounded text-xs font-medium ${hold.active ? "bg-red-500/20 text-red-300" : "bg-norse-stone text-norse-silver"}`}
                              >
                                {hold.active ? "Active" : "Inactive"}
                              </span>
                              {hold.matter ? (
                                <span className="text-xs text-norse-fog">Matter: {hold.matter}</span>
                              ) : null}
                            </div>
                            <p className="text-sm text-norse-silver">{hold.description}</p>
                            <div className="text-xs text-norse-fog">
                              Placed by {hold.placed_by} on {formatTimestamp(hold.placed_at)}
                            </div>
                            <div className="text-xs text-norse-fog">
                              Subjects: {holdToSubjectSummary(hold)}
                            </div>
                            <div className="text-xs text-norse-fog">
                              Categories: {holdToCategorySummary(hold)}
                            </div>
                            <div className="text-xs text-norse-fog">
                              Expires: {hold.expires_at ? formatTimestamp(hold.expires_at) : "Indefinite"}
                            </div>
                          </div>
                          <div>
                            <Button
                              variant="danger"
                              size="sm"
                              icon={Trash2}
                              onClick={() => setPendingAction({ kind: "release-hold", hold })}
                            >
                              Release
                            </Button>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      <AlertTriangle className="w-5 h-5 text-nornic-accent" />
                      Erasure queue
                    </h2>
                    <p className="text-sm text-norse-silver mt-1">
                      Create and process GDPR-style erasure requests using the same retention state the server enforces.
                    </p>
                  </div>
                  <Button variant="primary" icon={Plus} onClick={() => setErasureModalOpen(true)}>
                    Add request
                  </Button>
                </div>

                <div className="space-y-3 max-h-[38rem] overflow-y-auto pr-1">
                  {erasures.length === 0 ? (
                    <div className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4 text-sm text-norse-silver">
                      No erasure requests are queued.
                    </div>
                  ) : (
                    erasures.map((request) => (
                      <div
                        key={request.id}
                        className="rounded-lg bg-norse-stone/50 border border-norse-rune p-4"
                      >
                        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                          <div className="space-y-2">
                            <div className="flex items-center gap-2 flex-wrap">
                              <div className="font-medium text-white">{request.id}</div>
                              <span className="px-2 py-1 rounded text-xs font-medium bg-blue-500/20 text-blue-300">
                                {request.status}
                              </span>
                            </div>
                            <div className="text-sm text-norse-silver">
                              Subject: <span className="text-white">{request.subject_id}</span>
                              {request.subject_email ? ` (${request.subject_email})` : ""}
                            </div>
                            <div className="grid grid-cols-2 gap-3 text-xs text-norse-fog">
                              <div>Requested: {formatTimestamp(request.requested_at)}</div>
                              <div>Deadline: {formatTimestamp(request.deadline)}</div>
                              <div>Found: {request.items_found}</div>
                              <div>Erased: {request.items_erased}</div>
                              <div>Retained: {request.items_retained}</div>
                              <div>Notified: {request.subject_notified ? "yes" : "no"}</div>
                            </div>
                            {request.retained_reason ? (
                              <div className="text-xs text-amber-300">
                                Retained reason: {request.retained_reason}
                              </div>
                            ) : null}
                            {request.error ? (
                              <div className="text-xs text-red-300">Error: {request.error}</div>
                            ) : null}
                          </div>
                          {canProcessErasure(request) ? (
                            <div>
                              <Button
                                variant="primary"
                                size="sm"
                                icon={PlayCircle}
                                onClick={() => setPendingAction({ kind: "process-erasure", request })}
                              >
                                Process
                              </Button>
                            </div>
                          ) : null}
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>
            </section>

            <section className="bg-norse-shadow border border-norse-rune rounded-lg p-6 space-y-4">
              <div>
                <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                  <AlertTriangle className="w-5 h-5 text-nornic-accent" />
                  API surface used by this panel
                </h2>
                <p className="text-sm text-norse-silver mt-1">
                  Operators can map every action here directly to the retention admin endpoints behind it.
                </p>
              </div>
              <div className="space-y-3 text-sm">
                {[
                  ["GET", "/admin/retention/status", "Load manager availability and aggregate counts."],
                  ["GET", "/admin/retention/policies", "List configured policies."],
                  ["POST", "/admin/retention/policies", "Create a new policy."],
                  ["PUT", "/admin/retention/policies/{id}", "Update an existing policy."],
                  ["DELETE", "/admin/retention/policies/{id}", "Delete a policy by ID."],
                  ["POST", "/admin/retention/policies/defaults", "Load built-in compliance defaults."],
                  ["GET", "/admin/retention/holds", "List legal holds."],
                  ["POST", "/admin/retention/holds", "Create a hold."],
                  ["DELETE", "/admin/retention/holds/{id}", "Release a hold."],
                  ["GET", "/admin/retention/erasures", "List erasure requests."],
                  ["POST", "/admin/retention/erasures", "Create an erasure request."],
                  ["POST", "/admin/retention/erasures/{id}/process", "Process an erasure request."],
                  ["POST", "/admin/retention/sweep", "Run a manual sweep."],
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
                      <code className="text-xs text-white break-all">{path}</code>
                    </div>
                    <p className="text-norse-silver">{description}</p>
                  </div>
                ))}
              </div>
            </section>
          </>
        )}
      </main>

      <Modal
        isOpen={policyModalOpen}
        onClose={() => setPolicyModalOpen(false)}
        title={policyModalMode === "create" ? "Add retention policy" : `Edit policy ${policyForm.id}`}
        size="xl"
        className="max-h-[90vh] overflow-y-auto"
      >
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <FormInput
              id="retention-policy-id"
              label="Policy ID"
              value={policyForm.id}
              onChange={(value) => setPolicyForm((current) => ({ ...current, id: value }))}
              placeholder="pii-1y"
              disabled={policyModalMode === "edit"}
            />
            <FormInput
              id="retention-policy-name"
              label="Policy name"
              value={policyForm.name}
              onChange={(value) => setPolicyForm((current) => ({ ...current, name: value }))}
              placeholder="PII one year"
            />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label htmlFor="retention-policy-category" className="block text-sm font-medium text-norse-silver mb-2">
                Category
              </label>
              <select
                id="retention-policy-category"
                value={policyForm.category}
                onChange={(event) =>
                  setPolicyForm((current) => ({
                    ...current,
                    category: event.target.value as RetentionCategory,
                  }))
                }
                className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
              >
                {RETENTION_CATEGORIES.map((category) => (
                  <option key={category.value} value={category.value}>
                    {category.label} ({category.value})
                  </option>
                ))}
              </select>
            </div>
            <FormInput
              id="retention-policy-days"
              type="number"
              label="Retention days"
              value={policyForm.retentionDays}
              onChange={(value) => setPolicyForm((current) => ({ ...current, retentionDays: value }))}
              placeholder="365"
              disabled={policyForm.indefinite}
            />
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            <label className="inline-flex items-center gap-2 text-norse-silver">
              <input
                type="checkbox"
                checked={policyForm.indefinite}
                onChange={(event) =>
                  setPolicyForm((current) => ({ ...current, indefinite: event.target.checked }))
                }
                className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
              />
              Indefinite retention
            </label>
            <label className="inline-flex items-center gap-2 text-norse-silver">
              <input
                type="checkbox"
                checked={policyForm.archiveBeforeDelete}
                onChange={(event) =>
                  setPolicyForm((current) => ({
                    ...current,
                    archiveBeforeDelete: event.target.checked,
                  }))
                }
                className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
              />
              Archive before delete
            </label>
            <label className="inline-flex items-center gap-2 text-norse-silver">
              <input
                type="checkbox"
                checked={policyForm.active}
                onChange={(event) =>
                  setPolicyForm((current) => ({ ...current, active: event.target.checked }))
                }
                className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
              />
              Policy active
            </label>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <FormInput
              id="retention-policy-archive-path"
              label="Archive path"
              value={policyForm.archivePath}
              onChange={(value) => setPolicyForm((current) => ({ ...current, archivePath: value }))}
              placeholder="/var/lib/nornicdb/archive"
              disabled={!policyForm.archiveBeforeDelete}
            />
            <FormInput
              id="retention-policy-frameworks"
              label="Compliance frameworks"
              value={policyForm.complianceFrameworks}
              onChange={(value) =>
                setPolicyForm((current) => ({ ...current, complianceFrameworks: value }))
              }
              placeholder="GDPR, HIPAA, SOX"
            />
          </div>

          <div>
            <label htmlFor="retention-policy-description" className="block text-sm font-medium text-norse-silver mb-2">
              Description
            </label>
            <textarea
              id="retention-policy-description"
              value={policyForm.description}
              onChange={(event) =>
                setPolicyForm((current) => ({ ...current, description: event.target.value }))
              }
              rows={4}
              placeholder="Explain why this category is retained and how operators should use it."
              className="w-full px-4 py-3 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary"
            />
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setPolicyModalOpen(false)} disabled={policySaving}>
              Cancel
            </Button>
            <Button variant="primary" onClick={savePolicy} disabled={policySaving}>
              {policySaving ? "Saving..." : policyModalMode === "create" ? "Create policy" : "Save changes"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={holdModalOpen}
        onClose={() => setHoldModalOpen(false)}
        title="Add legal hold"
        size="xl"
        className="max-h-[90vh] overflow-y-auto"
      >
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <FormInput
              id="retention-hold-id"
              label="Hold ID"
              value={holdForm.id}
              onChange={(value) => setHoldForm((current) => ({ ...current, id: value }))}
              placeholder="legal-hold-001"
            />
            <FormInput
              id="retention-hold-matter"
              label="Matter"
              value={holdForm.matter}
              onChange={(value) => setHoldForm((current) => ({ ...current, matter: value }))}
              placeholder="Matter-2026-042"
            />
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <FormInput
              id="retention-hold-placed-by"
              label="Placed by"
              value={holdForm.placedBy}
              onChange={(value) => setHoldForm((current) => ({ ...current, placedBy: value }))}
              placeholder="legal-team"
            />
            <div>
              <label htmlFor="retention-hold-expires" className="block text-sm font-medium text-norse-silver mb-2">
                Expires at
              </label>
              <input
                id="retention-hold-expires"
                type="datetime-local"
                value={holdForm.expiresAt}
                onChange={(event) =>
                  setHoldForm((current) => ({ ...current, expiresAt: event.target.value }))
                }
                className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
              />
            </div>
          </div>
          <FormInput
            id="retention-hold-subjects"
            label="Subject IDs"
            value={holdForm.subjectIds}
            onChange={(value) => setHoldForm((current) => ({ ...current, subjectIds: value }))}
            placeholder="user-123, acct-456"
          />
          <div>
            <label className="block text-sm font-medium text-norse-silver mb-2">Categories in scope</label>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3 rounded-lg bg-norse-stone/50 border border-norse-rune p-4">
              {RETENTION_CATEGORIES.map((category) => {
                const checked = holdForm.categories.includes(category.value);
                return (
                  <label key={category.value} className="inline-flex items-center gap-2 text-sm text-norse-silver">
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={(event) =>
                        setHoldForm((current) => ({
                          ...current,
                          categories: event.target.checked
                            ? [...current.categories, category.value]
                            : current.categories.filter((value) => value !== category.value),
                        }))
                      }
                      className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
                    />
                    {category.value}
                  </label>
                );
              })}
            </div>
            <p className="mt-2 text-xs text-norse-fog">Leave this empty to cover all categories.</p>
          </div>
          <div>
            <label htmlFor="retention-hold-description" className="block text-sm font-medium text-norse-silver mb-2">
              Description
            </label>
            <textarea
              id="retention-hold-description"
              value={holdForm.description}
              onChange={(event) =>
                setHoldForm((current) => ({ ...current, description: event.target.value }))
              }
              rows={4}
              placeholder="Describe the legal or regulatory reason this data must not be deleted."
              className="w-full px-4 py-3 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary"
            />
          </div>
          <label className="inline-flex items-center gap-2 text-sm text-norse-silver">
            <input
              type="checkbox"
              checked={holdForm.active}
              onChange={(event) => setHoldForm((current) => ({ ...current, active: event.target.checked }))}
              className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
            />
            Hold active
          </label>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setHoldModalOpen(false)} disabled={holdSaving}>
              Cancel
            </Button>
            <Button variant="primary" onClick={saveHold} disabled={holdSaving}>
              {holdSaving ? "Saving..." : "Create hold"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={erasureModalOpen}
        onClose={() => setErasureModalOpen(false)}
        title="Create erasure request"
        size="md"
      >
        <div className="space-y-4">
          <FormInput
            id="retention-erasure-subject-id"
            label="Subject ID"
            value={erasureForm.subjectId}
            onChange={(value) => setErasureForm((current) => ({ ...current, subjectId: value }))}
            placeholder="user-123"
          />
          <FormInput
            id="retention-erasure-subject-email"
            type="email"
            label="Subject email"
            value={erasureForm.subjectEmail}
            onChange={(value) => setErasureForm((current) => ({ ...current, subjectEmail: value }))}
            placeholder="user@example.com"
          />
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setErasureModalOpen(false)} disabled={erasureSaving}>
              Cancel
            </Button>
            <Button variant="primary" onClick={saveErasureRequest} disabled={erasureSaving}>
              {erasureSaving ? "Saving..." : "Create request"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={pendingAction !== null}
        onClose={() => setPendingAction(null)}
        title={confirmActionTitle}
        size="md"
      >
        <div className="space-y-4">
          <p className="text-norse-silver">{confirmActionDescription}</p>
          <div className="rounded-lg border border-red-500/20 bg-red-500/5 p-3 text-sm text-red-200">
            Confirm this action only if the current policy and hold state reflects the intended compliance posture.
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" onClick={() => setPendingAction(null)} disabled={actionLoading}>
              Cancel
            </Button>
            <Button variant="primary" onClick={executePendingAction} disabled={actionLoading}>
              {actionLoading ? "Applying..." : confirmActionLabel}
            </Button>
          </div>
        </div>
      </Modal>
    </PageLayout>
  );
}