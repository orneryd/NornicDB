import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation, useNavigate, useParams } from "react-router-dom";
import type { GridOptions } from "@ornery/ui-grid-core";
import { api, type DatabaseInfo } from "../utils/api";
import {
  quoteIdentifier,
  resultRecords,
  schemaStatement,
} from "../utils/schemaOperations";
import { useUrlState, useViewState } from "../utils/useUrlState";
import { StatefulGrid } from "../components/common/StatefulGrid";
import { PageLayout } from "../components/common/PageLayout";
import { PageHeader } from "../components/common/PageHeader";
import { Button } from "../components/common/Button";
import { Alert } from "../components/common/Alert";
import { Modal } from "../components/common/Modal";
import { FormInput } from "../components/common/FormInput";
import { Disclosure } from "../components/common/Disclosure";

export function Schema() {
  const route = useParams(),
    navigate = useNavigate(),
    location = useLocation(),
    { params, patch } = useUrlState();
  const database = route.database ?? "",
    kind = route.schemaKind === "constraints" ? "constraint" : "index";
  const creating = route.definition === "new";
  const [tab, setTab] = useViewState<"indexes" | "constraints">(
    "tab",
    "indexes",
  );
  const [databases, setDatabases] = useState<DatabaseInfo[]>([]),
    [indexes, setIndexes] = useState<Record<string, unknown>[]>([]),
    [constraints, setConstraints] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [success, setSuccess] = useState("");
  const [name, setName] = useState(""),
    [entity, setEntity] = useState<"node" | "relationship">("node"),
    [target, setTarget] = useState(""),
    [properties, setProperties] = useState(""),
    [constraint, setConstraint] = useState<"unique" | "exists">("unique");
  const currentDb = useRef(database);
  currentDb.current = database;
  const base = `/schema/${encodeURIComponent(database)}`;
  const open = (path: string) =>
    navigate({ pathname: path, search: location.search });
  useEffect(() => {
    let current = true;
    api
      .listDatabases()
      .then((list) => {
        if (current)
          setDatabases(
            list.filter(
              (db) => db.type !== "composite" && db.name !== "system",
            ),
          );
      })
      .catch((e) => {
        if (current) setError(e.message);
      });
    return () => {
      current = false;
    };
  }, []);
  const load = async () => {
    if (!database) return;
    setLoading(true);
    setError("");
    try {
      const [i, c] = await Promise.all([
        api.executeCypherOverHttp(database, "SHOW INDEXES"),
        api.executeCypherOverHttp(database, "SHOW CONSTRAINTS"),
      ]);
      if (currentDb.current !== database) return;
      setIndexes(resultRecords(i));
      setConstraints(resultRecords(c));
    } catch (e) {
      if(currentDb.current!==database)return;
      setError(e instanceof Error ? e.message : "Unable to load schema");
    } finally {
      if (currentDb.current === database) setLoading(false);
    }
  };
  useEffect(() => {
    setIndexes([]);
    setConstraints([]);
    void load();
  }, [database]);
  let statement = "",
    validation = "";
  try {
    statement = schemaStatement({
      kind,
      name,
      entity,
      target,
      properties: properties
        .split(",")
        .map((p) => p.trim())
        .filter(Boolean),
      constraint,
    });
  } catch (e) {
    validation = e instanceof Error ? e.message : "Complete the form";
  }
  const drop = params.get("confirm") === "drop" ? params.get("name") : null,
    dropKind = params.get("kind") === "constraint" ? "constraint" : "index";
  const execute = async (query: string, message: string) => {
    setBusy(true);
    setError("");
    setSuccess("");
    try {
      const response = await api.executeCypherOverHttp(database, query);
      if (response.errors?.length)
        throw Error(response.errors.map((e) => e.message).join("; "));
      if(currentDb.current!==database)return;
      setSuccess(message);
      patch({ confirm: null, name: null, kind: null });
      if (creating) open(base);
      await load();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Schema operation failed");
    } finally {
      setBusy(false);
    }
  };
  const data = tab === "constraints" ? constraints : indexes;
  const options = useMemo<GridOptions>(
    () => ({
      id: `schema-${tab}-${database}`,
      title: tab === "constraints" ? "Constraints" : "Indexes",
      data: data.map((record, index) => ({
        ...record,
        __gridId: String(record.name ?? index),
      })),
      rowIdentity: (r) => String(r.__gridId),
      enableSorting: true,
      enableFiltering: true,
      columnDefs: [
        ...Object.keys(
          data[0] ?? { name: "", type: "", labelsOrTypes: [], properties: [] },
        ).map((field) => ({
          name: field,
          displayName: ({name:'Name',type:'Type',labelsOrTypes:'Labels / types',entityType:'Entity kind',properties:'Properties',state:'State',populationPercent:'Population (%)',owningConstraint:'Owning constraint',createStatement:'Definition'} as Record<string,string>)[field]??field.replace(/([a-z])([A-Z])/g,'$1 $2'),
          pinnedLeft: field === "name",
          width: field === "name" ? "240px" : "200px",
          enableCellEdit: false,
          formatter: (value: unknown) =>
            typeof value === "object"
              ? JSON.stringify(value)
              : String(value ?? "—"),
        })),
        {
          name: "actions",
          displayName: "Actions",
          pinnedRight: true,
          width: "120px",
          enableSorting: false,
          enableFiltering: false,
          enableCellEdit: false,
        },
      ],
    }),
    [tab, database, data],
  );
  return (
    <PageLayout>
      <PageHeader title="Schema" />
      <div className="max-w-7xl mx-auto p-6 space-y-5">
        <p className="text-norse-silver">
          Inspect indexes and constraints for one database. Indexes help queries
          find data; constraints enforce valid writes. The server checks your
          schema permissions for every operation.
        </p>
        <label className="block text-sm text-norse-silver">
          Database
          <select
            className="ml-3 rounded border border-norse-rune bg-norse-stone text-white p-2"
            value={database}
            disabled={busy}
            onChange={(e) =>
              open(`/schema/${encodeURIComponent(e.target.value)}`)
            }
          >
            <option value="">Choose a database</option>
            {databases.map((db) => (
              <option key={db.name}>{db.name}</option>
            ))}
          </select>
        </label>
        {error && !creating && !drop && <Alert type="error" message={error} />}
        {success && <Alert type="success" message={success} />}
        {database && (
          <>
            <div className="flex flex-wrap gap-3">
              <Button
                variant={tab === "indexes" ? "primary" : "secondary"}
                onClick={() => setTab("indexes")}
              >
                Indexes ({indexes.length})
              </Button>
              <Button
                variant={tab === "constraints" ? "primary" : "secondary"}
                onClick={() => setTab("constraints")}
              >
                Constraints ({constraints.length})
              </Button>
              <Button
                variant="secondary"
                onClick={() => void load()}
                disabled={loading || busy}
              >
                {loading ? "Refreshing…" : "Refresh schema"}
              </Button>
              <Button onClick={() => open(`${base}/${tab}/new`)}>
                Create {tab === "constraints" ? "constraint" : "index"}
              </Button>
            </div>
            <div aria-busy={loading}>
              <StatefulGrid key={`${tab}:${database}`} height={Math.min(500,Math.max(220,data.length*48+120))}
                options={options}
                cellRenderers={{
                  actions: ({ row }) => (
                    <Button
                      variant="danger"
                      size="sm"
                      disabled={busy || typeof row.name !== "string"}
                      onClick={() => {
                        setError("");
                        patch({
                          confirm: "drop",
                          name: String(row.name),
                          kind: tab === "constraints" ? "constraint" : "index",
                        });
                      }}
                    >
                      Drop
                    </Button>
                  ),
                }}
              />
            </div>
            <Disclosure id="schema-help" title="Choose an index or constraint">
              <ul className="list-disc pl-5 space-y-2 text-sm text-norse-silver">
                <li>
                  Use a property index for repeated lookups on a node label or
                  relationship type.
                </li>
                <li>
                  Use uniqueness to reject duplicate property values. For a
                  composite key, enter properties separated by commas.
                </li>
                <li>
                  Use property existence to require a property on every matching
                  entity. Existing invalid data may prevent creation.
                </li>
                <li>
                  Dropping a constraint removes its validation for future
                  writes. Dropping an index can slow queries.
                </li>
              </ul>
              <p className="mt-3 text-sm text-norse-silver">
                For vector/full-text indexes, type constraints or contract
                rules, use the{" "}
                <Link
                  className="text-nornic-primary underline"
                  to={`/workspace/${encodeURIComponent(database)}?query=${encodeURIComponent("SHOW CONSTRAINTS")}`}
                >
                  Cypher workspace
                </Link>
                . See{" "}
                <a
                  className="text-nornic-primary underline"
                  href="https://github.com/orneryd/NornicDB/blob/main/docs/user-guides/canonical-graph-ledger.md"
                  target="_blank"
                  rel="noreferrer"
                >
                  schema examples
                </a>
                .
              </p>
            </Disclosure>
          </>
        )}
        <Modal
          isOpen={creating}
          onClose={() => open(base)}
          title={`Create ${kind}`}
        >
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (statement)
                void execute(
                  statement,
                  `${kind} statement completed in ${database}.`,
                );
            }}
          >
            <p className="text-sm text-norse-silver">
              Target database: <strong>{database}</strong>. Existing data is
              checked by the server.
            </p>
            <FormInput
              label="Schema name"
              value={name}
              onChange={setName}
              required
            />
            <label className="block text-sm text-norse-silver">
              Entity
              <select
                className="block w-full rounded bg-norse-stone p-2 text-white"
                value={entity}
                onChange={(e) => setEntity(e.target.value as typeof entity)}
              >
                <option value="node">Nodes</option>
                <option value="relationship">Relationships</option>
              </select>
            </label>
            <FormInput
              label={entity === "node" ? "Node label" : "Relationship type"}
              value={target}
              onChange={setTarget}
              required
            />
            <FormInput
              label="Properties"
              value={properties}
              onChange={setProperties}
              required
              placeholder="email, tenantId"
            />
            {kind === "constraint" && (
              <label className="block text-sm text-norse-silver">
                Rule
                <select
                  className="block w-full rounded bg-norse-stone p-2 text-white"
                  value={constraint}
                  onChange={(e) =>
                    setConstraint(e.target.value as typeof constraint)
                  }
                >
                  <option value="unique">Unique values</option>
                  <option value="exists">Property must exist</option>
                </select>
              </label>
            )}
            <p className="text-sm text-norse-silver">
              {validation || "Review the statement before creating it."}
            </p>
            {statement && (
              <pre className="p-3 text-xs text-norse-silver bg-norse-night overflow-auto">
                {statement}
              </pre>
            )}
            {error && <Alert type="error" message={error} />}
            <div className="flex gap-3">
              <Button
                variant="secondary"
                type="button"
                onClick={() => open(base)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={!statement || busy}>
                {busy ? "Creating…" : `Create ${kind}`}
              </Button>
            </div>
          </form>
        </Modal>
        <Modal
          isOpen={Boolean(drop)}
          onClose={() => patch({ confirm: null, name: null, kind: null })}
          title={`Drop ${dropKind}`}
        >
          <p className="text-norse-silver mb-4">
            Drop <strong>{drop}</strong> from <strong>{database}</strong>?{" "}
            {dropKind === "constraint"
              ? "Writes will no longer be checked by this constraint."
              : "Queries may lose this index and become slower."}
          </p>
          {error && <Alert type="error" message={error} />}
          <div className="flex gap-3">
            <Button
              data-autofocus
              variant="secondary"
              onClick={() => patch({ confirm: null, name: null, kind: null })}
            >
              Cancel
            </Button>
            <Button
              variant="danger"
              disabled={busy}
              onClick={() =>
                drop &&
                void execute(
                  `DROP ${dropKind.toUpperCase()} ${quoteIdentifier(drop)} IF EXISTS`,
                  `${dropKind} dropped.`,
                )
              }
            >
              {busy ? "Dropping…" : `Drop ${dropKind}`}
            </Button>
          </div>
        </Modal>
      </div>
    </PageLayout>
  );
}
