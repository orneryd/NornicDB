import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { UiGrid } from "@ornery/ui-grid-react";
import type { GridCellTemplateContext, GridColumnDef, GridOptions, GridRecord, UiGridApi } from "@ornery/ui-grid-core";
import { Activity, Database, Info, Plus, Settings, Trash2 } from "lucide-react";
import { api } from "../utils/api";
import type { DatabaseInfo } from "../utils/api";
import { Alert } from "../components/common/Alert";
import { Button } from "../components/common/Button";
import { FormInput } from "../components/common/FormInput";
import { Modal } from "../components/common/Modal";
import { PageHeader } from "../components/common/PageHeader";
import { PageLayout } from "../components/common/PageLayout";
import { BASE_PATH, joinBasePath } from "../utils/basePath";

export function Databases() {
  const navigate = useNavigate();
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [refreshing, setRefreshing] = useState(false);

  // Create database state
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [newName, setNewName] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState("");

  // Delete database state
  const [deleteTarget, setDeleteTarget] = useState<DatabaseInfo | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState("");

  // Database details state
  const [selectedDatabase, setSelectedDatabase] = useState<DatabaseInfo | null>(
    null,
  );
  const [showDetailsModal, setShowDetailsModal] = useState(false);

  // Admin check (for showing config cog)
  const [isAdmin, setIsAdmin] = useState(false);

  // Database config modal (admin only)
  const [configDbName, setConfigDbName] = useState<string | null>(null);
  const [configKeys, setConfigKeys] = useState<
    Array<{ key: string; type: string; category: string }>
  >([]);
  const [configEffective, setConfigEffective] = useState<
    Record<string, string>
  >({});
  const [configUseDefault, setConfigUseDefault] = useState<
    Record<string, boolean>
  >({});
  const [configFormValues, setConfigFormValues] = useState<
    Record<string, string>
  >({});
  const [configLoading, setConfigLoading] = useState(false);
  const [configSaving, setConfigSaving] = useState(false);
  const [configError, setConfigError] = useState("");
  const [configGridApi, setConfigGridApi] = useState<UiGridApi | null>(null);
  const configSectionRef = useRef<HTMLElement | null>(null);

  const formatEta = (etaSeconds?: number): string => {
    if (etaSeconds == null || etaSeconds < 0) return "estimating...";
    if (etaSeconds < 60) return `${etaSeconds}s`;
    const m = Math.floor(etaSeconds / 60);
    const s = etaSeconds % 60;
    return `${m}m ${s}s`;
  };

  const formatBytes = (bytes?: number): string => {
    const n =
      typeof bytes === "number" && Number.isFinite(bytes)
        ? Math.max(0, bytes)
        : 0;
    if (n < 1024) return `${n} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let value = n;
    let unitIdx = -1;
    do {
      value /= 1024;
      unitIdx++;
    } while (value >= 1024 && unitIdx < units.length - 1);
    return `${value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2)} ${units[unitIdx]}`;
  };

  const loadDatabases = useCallback(async () => {
    try {
      setError("");
      const data = await api.listDatabases();
      setDatabases(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load databases");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    loadDatabases();
  }, [loadDatabases]);

  useEffect(() => {
    fetch(joinBasePath(BASE_PATH, "/auth/me"), { credentials: "include" })
      .then((r) => (r.ok ? r.json() : null))
      .then((me: { roles?: string[] } | null) => {
        setIsAdmin(Array.isArray(me?.roles) && me.roles.includes("admin"));
      })
      .catch(() => setIsAdmin(false));
  }, []);

  const handleRefresh = () => {
    setRefreshing(true);
    loadDatabases();
  };

  const handleCreateDatabase = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreateError("");
    setCreating(true);

    try {
      await api.createDatabase(newName);
      setShowCreateModal(false);
      setNewName("");
      await loadDatabases();
    } catch (err) {
      setCreateError(
        err instanceof Error ? err.message : "Failed to create database",
      );
    } finally {
      setCreating(false);
    }
  };

  const handleDeleteDatabase = async () => {
    if (!deleteTarget) return;
    setDeleteError("");
    setDeleting(true);

    try {
      await api.dropDatabase(deleteTarget.name);
      setDeleteTarget(null);
      await loadDatabases();
    } catch (err) {
      setDeleteError(
        err instanceof Error ? err.message : "Failed to delete database",
      );
    } finally {
      setDeleting(false);
    }
  };

  const handleViewDetails = async (db: DatabaseInfo) => {
    try {
      const info = await api.getDatabaseInfo(db.name);
      setSelectedDatabase(info);
      setShowDetailsModal(true);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load database details",
      );
    }
  };

  const handleOpenConfig = useCallback(async (dbName: string) => {
    setConfigDbName(dbName);
    setConfigError("");
    setConfigLoading(true);
    try {
      const [configRes, keysRes] = await Promise.all([
        api.getDatabaseConfig(dbName),
        api.getDatabaseConfigKeys(),
      ]);
      setConfigEffective(configRes.effective ?? {});
      setConfigKeys(keysRes);
      const overrides = configRes.overrides ?? {};
      const useDefault: Record<string, boolean> = {};
      const formValues: Record<string, string> = {};
      for (const k of keysRes) {
        const hasOverride =
          overrides[k.key] !== undefined && overrides[k.key] !== "";
        useDefault[k.key] = !hasOverride;
        formValues[k.key] = hasOverride
          ? (overrides[k.key] ?? "")
          : (configRes.effective?.[k.key] ?? "");
      }
      setConfigUseDefault(useDefault);
      setConfigFormValues(formValues);
    } catch (err) {
      setConfigError(
        err instanceof Error ? err.message : "Failed to load config",
      );
    } finally {
      setConfigLoading(false);
    }
  }, []);

  const handleConfigSave = async () => {
    if (!configDbName) return;
    setConfigError("");
    setConfigSaving(true);
    try {
      const overrides: Record<string, string> = {};
      for (const k of configKeys) {
        if (configUseDefault[k.key]) continue;
        const v = (configFormValues[k.key] ?? "").trim();
        if (v !== "") overrides[k.key] = v;
      }
      await api.putDatabaseConfig(configDbName, overrides);
      await loadDatabases();
      if (selectedDatabase?.name === configDbName) {
        try {
          const refreshed = await api.getDatabaseInfo(configDbName);
          setSelectedDatabase(refreshed);
        } catch {
          // Ignore modal refresh errors; list refresh above already applied.
        }
      }
      setConfigDbName(null);
    } catch (err) {
      setConfigError(
        err instanceof Error ? err.message : "Failed to save config",
      );
    } finally {
      setConfigSaving(false);
    }
  };

  const setConfigFormValue = (key: string, value: string) => {
    setConfigFormValues((prev) => ({ ...prev, [key]: value }));
  };
  const setConfigUseDefaultForKey = (key: string, useDefault: boolean) => {
    setConfigUseDefault((prev) => ({ ...prev, [key]: useDefault }));
    if (useDefault && configEffective[key] !== undefined) {
      setConfigFormValues((prev) => ({
        ...prev,
        [key]: configEffective[key] ?? "",
      }));
    }
  };

  const configGridData = useMemo<GridRecord[]>(() => configKeys.map((meta) => ({
    __gridId: meta.key,
    key: meta.key.replace(/^NORNICDB_/, ""),
    rawKey: meta.key,
    type: meta.type,
    category: meta.category || "Other",
    value: configFormValues[meta.key] ?? "",
    useDefault: configUseDefault[meta.key] ?? true,
    effectiveDefault: configEffective[meta.key] ?? "",
  })), [configEffective, configFormValues, configKeys, configUseDefault]);

  const configColumnDefs = useMemo<GridColumnDef[]>(() => [
    {
      name: "category",
      displayName: "Category",
      field: "category",
      width: "160px",
    },
    {
      name: "key",
      displayName: "Key",
      field: "key",
      width: "minmax(18rem, 1.4fr)",
    },
    {
      name: "value",
      displayName: "Value",
      field: "value",
      width: "minmax(14rem, 1.3fr)",
      enableCellEdit: true,
      cellEditableCondition: (ctx) => !Boolean(ctx.row.useDefault) && ctx.row.type !== "boolean",
    },
    {
      name: "useDefault",
      displayName: "Use Default",
      field: "useDefault",
      width: "120px",
      enableSorting: false,
    },
    {
      name: "effectiveDefault",
      displayName: "Effective Default",
      field: "effectiveDefault",
      width: "minmax(12rem, 1fr)",
    },
  ], []);

  const configGridOptions = useMemo<GridOptions>(() => ({
    id: "database-config-grid",
    data: configGridData,
    columnDefs: configColumnDefs,
    rowIdentity: (row) => String(row.__gridId),
    enableGrouping: true,
    grouping: { groupBy: ["category"] },
    enableSorting: true,
    enableFiltering: true,
    enableCellEdit: true,
    enableCellEditOnFocus: true,
    viewportHeight: 400,
    emptyMessage: "No configuration keys available",
  }), [configColumnDefs, configGridData]);

  useEffect(() => {
    if (!configGridApi) {
      return;
    }

    return configGridApi.edit.on.afterCellEdit((row, column, newValue, oldValue) => {
      if (column.name !== "value") {
        return;
      }
      if (String(newValue ?? "") === String(oldValue ?? "")) {
        return;
      }
      setConfigFormValue(String(row.rawKey), String(newValue ?? ""));
    });
  }, [configGridApi]);

  useEffect(() => {
    if (!configDbName || !configSectionRef.current) {
      return;
    }

    configSectionRef.current.scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
  }, [configDbName]);

  const renderConfigCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & {
      rawKey: string;
      type: string;
      useDefault: boolean;
      value: string;
      effectiveDefault: string;
    };

    if (ctx.column.name === "value" && row.type === "boolean") {
      return (
        <div className="py-1" onClick={(e) => e.stopPropagation()}>
          <input
            type="checkbox"
            checked={String(row.value) === "true"}
            disabled={row.useDefault}
            onChange={(e) => setConfigFormValue(row.rawKey, e.target.checked ? "true" : "false")}
            className="rounded border-norse-rune bg-norse-stone text-nornic-primary"
          />
        </div>
      );
    }

    if (ctx.column.name === "useDefault") {
      return (
        <div className="py-1" onClick={(e) => e.stopPropagation()}>
          <input
            type="checkbox"
            checked={row.useDefault}
            onChange={(e) => setConfigUseDefaultForKey(row.rawKey, e.target.checked)}
            className="rounded border-norse-rune bg-norse-stone text-nornic-primary"
          />
        </div>
      );
    }

    if (ctx.column.name === "effectiveDefault") {
      return <div className="py-1 text-norse-silver">{String(ctx.value || "—")}</div>;
    }

    if (ctx.column.name === "key") {
      return <div className="py-1 text-white font-medium">{String(ctx.value ?? "")}</div>;
    }

    if (ctx.column.name === "value" && row.useDefault) {
      return <div className="py-1 text-norse-fog">{String(row.value || "—")}</div>;
    }

    return null;
  };

  const configCellRenderers = useMemo(() => ({
    key: renderConfigCell,
    value: renderConfigCell,
    useDefault: renderConfigCell,
    effectiveDefault: renderConfigCell,
  }), [renderConfigCell]);
  if (loading) {
    return (
      <PageLayout>
        <PageHeader title="Databases" backTo="/" backLabel="Back to Browser" />
        <div className="flex items-center justify-center min-h-[400px]">
          <div className="text-norse-silver">Loading databases...</div>
        </div>
      </PageLayout>
    );
  }

  return (
    <PageLayout>
      <PageHeader
        title="Databases"
        backTo="/"
        backLabel="Back to Browser"
        actions={
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              onClick={handleRefresh}
              disabled={refreshing}
            >
              {refreshing ? "Refreshing..." : "Refresh"}
            </Button>
            <Button
              variant="primary"
              onClick={() => setShowCreateModal(true)}
              icon={Plus}
            >
              Create Database
            </Button>
          </div>
        }
      />

      <main className="max-w-6xl mx-auto p-6">
        {error && (
          <Alert
            type="error"
            message={error}
            className="mb-6"
            dismissible
            onDismiss={() => setError("")}
          />
        )}

        {databases.length === 0 ? (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-12 text-center">
            <Database className="w-16 h-16 text-norse-silver mx-auto mb-4" />
            <h3 className="text-lg font-semibold text-white mb-2">
              No Databases
            </h3>
            <p className="text-norse-silver mb-6">
              Create your first database to start organizing data
            </p>
            <Button
              variant="primary"
              onClick={() => setShowCreateModal(true)}
              icon={Plus}
            >
              Create Database
            </Button>
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {databases.map((db) => {
              const isComposite = db.type === "composite";
              const ready = db.searchReady === true;
              const searchLabel = ready
                ? "ready"
                : db.searchBuilding
                  ? "warming"
                  : "pending";
              const strategyLabel =
                db.searchStrategy && db.searchStrategy !== "unknown"
                  ? ` (${db.searchStrategy})`
                  : "";
                return (
                  <div
                    key={db.name}
                    className="bg-norse-shadow border border-norse-rune rounded-lg p-4 hover:border-nornic-primary transition-colors"
                  >
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <h3 className="text-lg font-semibold text-white mb-1">
                        {db.name}
                      </h3>
                      <div className="flex items-center gap-2 text-sm text-norse-silver flex-wrap">
                        <span
                          className={`px-2 py-1 rounded text-xs ${
                            db.status === "online"
                              ? "bg-green-900/30 text-green-400"
                              : "bg-red-900/30 text-red-400"
                          }`}
                        >
                          {db.status}
                        </span>
                        {isComposite ? (
                          <span className="px-2 py-1 rounded text-xs bg-blue-900/30 text-blue-400 border border-blue-700/50">
                            composite
                          </span>
                        ) : (
                          <span
                            className={`inline-flex items-center gap-1.5 px-2 py-1 rounded text-xs border ${
                              ready
                                ? "bg-green-900/20 text-green-300 border-green-700/50"
                                : "bg-yellow-900/20 text-yellow-300 border-yellow-700/50"
                            }`}
                          >
                            <span
                              className={`inline-block w-2 h-2 rounded-full ${
                                ready
                                  ? "bg-green-400 shadow-[0_0_8px_rgba(74,222,128,0.9)]"
                                  : "bg-yellow-400 shadow-[0_0_8px_rgba(250,204,21,0.9)] animate-pulse"
                              }`}
                            />
                            search {searchLabel}
                            {strategyLabel}
                          </span>
                        )}
                        {db.default && (
                          <span className="px-2 py-1 rounded text-xs bg-valhalla-gold/20 text-valhalla-gold">
                            default
                          </span>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-1">
                      {isAdmin && (
                        <div
                          title={
                            db.name === "system" || isComposite
                              ? "Lifecycle controls are only available for standard databases"
                              : "Manage MVCC lifecycle"
                          }
                        >
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() =>
                              navigate(
                                `/security/lifecycle?db=${encodeURIComponent(db.name)}`,
                              )
                            }
                            icon={Activity}
                            disabled={db.name === "system" || isComposite}
                          >
                            <span className="sr-only">Manage lifecycle</span>
                          </Button>
                        </div>
                      )}
                      {isAdmin && (
                        <div title="Configure database">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleOpenConfig(db.name)}
                            icon={Settings}
                          >
                            <span className="sr-only">Configure</span>
                          </Button>
                        </div>
                      )}
                      <div title="View details">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleViewDetails(db)}
                          icon={Info}
                        >
                          <span className="sr-only">View details</span>
                        </Button>
                      </div>
                      <div
                        title={
                          db.default
                            ? "Default database cannot be deleted"
                            : "Delete database"
                        }
                      >
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setDeleteTarget(db)}
                          icon={Trash2}
                          disabled={db.default}
                          className="text-red-400 hover:text-red-300 hover:bg-red-900/20 disabled:opacity-40 disabled:hover:bg-transparent"
                        >
                          <span className="sr-only">Delete database</span>
                        </Button>
                      </div>
                    </div>
                  </div>

                  {isComposite ? (
                    <div className="space-y-2 text-sm">
                      <div className="text-norse-silver text-xs font-medium uppercase tracking-wide">
                        Constituents
                      </div>
                      {db.constituents && db.constituents.length > 0 ? (
                        <div className="space-y-1.5">
                          {db.constituents.map((c) => (
                            <div
                              key={c.alias}
                              className="flex items-center justify-between gap-2"
                            >
                              <div className="flex items-center gap-1.5 min-w-0">
                                <span className="text-white font-medium truncate">
                                  {c.alias}
                                </span>
                                <span className="text-norse-silver">→</span>
                                <span className="text-norse-silver truncate">
                                  {c.type === "remote" && c.uri
                                    ? c.uri
                                    : c.databaseName}
                                </span>
                              </div>
                              <span
                                className={`px-1.5 py-0.5 rounded text-[10px] shrink-0 ${
                                  c.type === "remote"
                                    ? "bg-purple-900/30 text-purple-400"
                                    : "bg-norse-rune/50 text-norse-silver"
                                }`}
                              >
                                {c.type}
                              </span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-norse-silver italic">
                          No constituents
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="space-y-2 text-sm">
                      {!ready && (
                        <div className="flex justify-between">
                          <span className="text-norse-silver">Search ETA:</span>
                          <span className="text-yellow-300 font-medium">
                            {formatEta(db.searchEtaSeconds)}
                          </span>
                        </div>
                      )}
                      <div className="flex justify-between">
                        <span className="text-norse-silver">Nodes:</span>
                        <span className="text-white font-medium">
                          {db.nodeCount.toLocaleString()}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-norse-silver">Edges:</span>
                        <span className="text-white font-medium">
                          {db.edgeCount.toLocaleString()}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-norse-silver">Node Storage:</span>
                        <span className="text-white font-medium">
                          {formatBytes(db.nodeStorageBytes)}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-norse-silver">
                          Managed Embeddings:
                        </span>
                        <span className="text-white font-medium">
                          {formatBytes(db.managedEmbeddingBytes)}
                        </span>
                      </div>
                    </div>
                  )}
                  </div>
                );
              })}
            </div>

            {configDbName && (
              <section
                ref={configSectionRef}
                className="mt-8 bg-norse-shadow border border-norse-rune rounded-lg"
              >
                <div className="flex items-start justify-between gap-4 border-b border-norse-rune px-6 py-5">
                  <div>
                    <h2 className="text-xl font-semibold text-white">
                      Database Settings
                    </h2>
                    <p className="mt-1 text-sm text-norse-silver">
                      Editing configuration for {configDbName}.
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button
                      type="button"
                      variant="secondary"
                      onClick={() => setConfigDbName(null)}
                      disabled={configSaving}
                    >
                      Close
                    </Button>
                    <Button
                      type="button"
                      variant="primary"
                      onClick={handleConfigSave}
                      disabled={configSaving || configLoading}
                    >
                      {configSaving ? "Saving..." : "Save"}
                    </Button>
                  </div>
                </div>

                <div className="p-6 space-y-4">
                  {configLoading ? (
                    <div className="text-norse-silver py-8 text-center">Loading...</div>
                  ) : (
                    <>
                      {configError && (
                        <Alert
                          type="error"
                          message={configError}
                          dismissible
                          onDismiss={() => setConfigError("")}
                        />
                      )}
                      <div className="nornic-grid">
                        <UiGrid
                          options={configGridOptions}
                          onRegisterApi={setConfigGridApi}
                          cellRenderers={configCellRenderers}
                        />
                      </div>
                    </>
                  )}
                </div>
              </section>
            )}
          </>
        )}
      </main>

      <Modal
        isOpen={showCreateModal}
        onClose={() => setShowCreateModal(false)}
        title="Create Database"
        size="md"
      >
        <form onSubmit={handleCreateDatabase} className="space-y-4">
          {createError && (
            <Alert
              type="error"
              message={createError}
              dismissible
              onDismiss={() => setCreateError("")}
            />
          )}
          <FormInput
            label="Database Name"
            value={newName}
            onChange={(value) => setNewName(value)}
            placeholder="e.g. tenant_a"
            required
            disabled={creating}
          />
          <div className="text-sm text-norse-silver">
            Creates a new database namespace. Qdrant collections are also mapped
            to databases.
          </div>
          <div className="flex justify-end gap-2 pt-4">
            <Button
              type="button"
              variant="secondary"
              onClick={() => setShowCreateModal(false)}
              disabled={creating}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              variant="primary"
              disabled={creating || !newName.trim()}
            >
              {creating ? "Creating..." : "Create"}
            </Button>
          </div>
        </form>
      </Modal>

      <Modal
        isOpen={Boolean(deleteTarget)}
        onClose={() => setDeleteTarget(null)}
        title="Delete Database"
        size="md"
      >
        <div className="space-y-4">
          {deleteError && (
            <Alert
              type="error"
              message={deleteError}
              dismissible
              onDismiss={() => setDeleteError("")}
            />
          )}
          <p className="text-norse-silver">
            Delete database{" "}
            <span className="text-white font-semibold">
              {deleteTarget?.name}
            </span>
            ? This removes all data in that namespace.
          </p>
          <div className="flex justify-end gap-2 pt-4">
            <Button
              type="button"
              variant="secondary"
              onClick={() => setDeleteTarget(null)}
              disabled={deleting}
            >
              Cancel
            </Button>
            <Button
              type="button"
              variant="danger"
              onClick={handleDeleteDatabase}
              disabled={deleting}
            >
              {deleting ? "Deleting..." : "Delete"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        isOpen={showDetailsModal}
        onClose={() => setShowDetailsModal(false)}
        title="Database Details"
        size="md"
      >
        {selectedDatabase ? (
          <div className="space-y-3 text-sm">
            <div className="flex justify-between">
              <span className="text-norse-silver">Name:</span>
              <span className="text-white font-medium">
                {selectedDatabase.name}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-norse-silver">Type:</span>
              <span className="text-white font-medium">
                {selectedDatabase.type ?? "standard"}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-norse-silver">Status:</span>
              <span className="text-white font-medium">
                {selectedDatabase.status}
              </span>
            </div>
            {selectedDatabase.type === "composite" ? (
              <>
                <div className="border-t border-norse-rune pt-3 mt-3">
                  <div className="text-norse-silver text-xs font-medium uppercase tracking-wide mb-2">
                    Constituents
                  </div>
                  {selectedDatabase.constituents &&
                  selectedDatabase.constituents.length > 0 ? (
                    <div className="space-y-2">
                      {selectedDatabase.constituents.map((c) => (
                        <div
                          key={c.alias}
                          className="bg-norse-stone/50 rounded px-3 py-2 space-y-1"
                        >
                          <div className="flex items-center justify-between">
                            <span className="text-white font-medium">
                              {c.alias}
                            </span>
                            <div className="flex items-center gap-1.5">
                              <span
                                className={`px-1.5 py-0.5 rounded text-[10px] ${
                                  c.type === "remote"
                                    ? "bg-purple-900/30 text-purple-400"
                                    : "bg-norse-rune/50 text-norse-silver"
                                }`}
                              >
                                {c.type}
                              </span>
                              <span className="px-1.5 py-0.5 rounded text-[10px] bg-norse-rune/50 text-norse-silver">
                                {c.accessMode}
                              </span>
                            </div>
                          </div>
                          <div className="text-norse-silver text-xs truncate">
                            {c.type === "remote" && c.uri
                              ? c.uri
                              : c.databaseName}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-norse-silver italic">
                      No constituents
                    </div>
                  )}
                </div>
              </>
            ) : (
              <>
                <div className="flex justify-between">
                  <span className="text-norse-silver">Search:</span>
                  <span
                    className={`font-medium ${selectedDatabase.searchReady ? "text-green-300" : "text-yellow-300"}`}
                  >
                    {selectedDatabase.searchReady
                      ? "ready"
                      : selectedDatabase.searchBuilding
                        ? "warming"
                        : "pending"}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-norse-silver">Search Strategy:</span>
                  <span className="text-white font-medium">
                    {selectedDatabase.searchStrategy || "unknown"}
                  </span>
                </div>
                {!selectedDatabase.searchReady && (
                  <div className="flex justify-between">
                    <span className="text-norse-silver">Search ETA:</span>
                    <span className="text-yellow-300 font-medium">
                      {formatEta(selectedDatabase.searchEtaSeconds)}
                    </span>
                  </div>
                )}
                <div className="flex justify-between">
                  <span className="text-norse-silver">Nodes:</span>
                  <span className="text-white font-medium">
                    {selectedDatabase.nodeCount.toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-norse-silver">Edges:</span>
                  <span className="text-white font-medium">
                    {selectedDatabase.edgeCount.toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-norse-silver">Node Storage:</span>
                  <span className="text-white font-medium">
                    {formatBytes(selectedDatabase.nodeStorageBytes)}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-norse-silver">Managed Embeddings:</span>
                  <span className="text-white font-medium">
                    {formatBytes(selectedDatabase.managedEmbeddingBytes)}
                  </span>
                </div>
              </>
            )}
          </div>
        ) : (
          <div className="text-norse-silver">No database selected.</div>
        )}
      </Modal>
    </PageLayout>
  );
}
