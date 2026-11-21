/**
 * Fine-grained entitlement (FGAC) administration for the Database Access page.
 *
 * The editor reads `GET /auth/fgac/catalog` with the page's authenticated
 * cookie, builds a local draft and replaces the stored catalog with
 * `PUT /auth/fgac/catalog` including the revision that was read. The server
 * remains the policy authority: it rejects structural errors and reports a
 * stale revision. This editor is loaded only when FGAC is enabled.
 */

import { useCallback, useEffect, useId, useMemo, useState } from "react";
import { Lock, Save } from "lucide-react";
import { Alert } from "../common/Alert";
import { Button } from "../common/Button";
import { FormInput } from "../common/FormInput";
import { Modal } from "../common/Modal";
import { useUnsavedChanges } from "../common/useUnsavedChanges";
import { FgacRoleWorkspace, type FgacRolePermissions } from "./FgacRoleWorkspace";
import { api, type DatabaseIdentity } from "../../utils/api";
import { BASE_PATH, joinBasePath } from "../../utils/basePath";
import {
  catalogPutBody,
  catalogSignature,
  definitionInput,
  describeAction,
  describeFgacActionName,
  groupFgacActions,
  inputForAction,
  isPredicateAction,
  isEntityKind,
  normalizeCatalog,
  removeDefinition,
  toggleRoleAssignment,
  upsertDefinition,
  validateCatalog,
  validateDefinitionInput,
  type FgacAction,
  type FgacCatalogDocument,
  type FgacDefinition,
  type FgacCypherPrivilege,
  type FgacDefinitionInput,
} from "../../utils/fgacCatalog";

const CATALOG_PATH = "/auth/fgac/catalog";

type LoadState = "loading" | "ready" | "failed";

async function responseMessage(response: Response, fallback: string): Promise<string> {
  const body: unknown = await response.json().catch(() => null);
  if (body && typeof body === "object") {
    const document = body as { message?: unknown; errors?: unknown };
    if (Array.isArray(document.errors)) {
      const first = document.errors[0] as { message?: unknown } | undefined;
      if (typeof first?.message === "string" && first.message) return first.message;
    }
    if (typeof document.message === "string" && document.message) return document.message;
  }
  return `${fallback} (HTTP ${response.status})`;
}

export function FineGrainedEntitlements({ rolePermissions }: { rolePermissions: FgacRolePermissions }) {
  const [loadState, setLoadState] = useState<LoadState>("loading");
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");
  const [actions, setActions] = useState<FgacAction[]>([]);
  const [definitions, setDefinitions] = useState<FgacDefinition[]>([]);
  const [privileges, setPrivileges] = useState<FgacCypherPrivilege[]>([]);
  const [assignments, setAssignments] = useState<Record<string, string[]>>({});
  const [revision, setRevision] = useState("");
  const [savedSignature, setSavedSignature] = useState("");
  const [saving, setSaving] = useState(false);
  const [databases, setDatabases] = useState<DatabaseIdentity[]>([]);
  const [editor, setEditor] = useState<{ input: FgacDefinitionInput; editingId?: string; assignTo?: string; scopeChosen?: boolean } | null>(
    null,
  );
  const [editorErrors, setEditorErrors] = useState<string[]>([]);
  const [pendingRemoval, setPendingRemoval] = useState<FgacDefinition>();
  const [discardOpen, setDiscardOpen] = useState(false);

  const definitionIdField = useId();
  const actionField = useId();
  const actionSearchField = useId();
  const [actionSearch, setActionSearch] = useState("");
  const databaseField = useId();
  const typesField = useId();
  const entityIdsField = useId();
  const predicatePropertyField = useId();
  const predicateValueField = useId();
  const [elementId, setElementId] = useState("");
  const [resolvingEntity, setResolvingEntity] = useState(false);
  const propertiesField = useId();

  const [serverRoles, setServerRoles] = useState<string[]>();

  const applyDocument = useCallback((document: FgacCatalogDocument) => {
    const draft = normalizeCatalog(document.catalog);
    setServerRoles(document.roles);
    setDefinitions(draft.definitions);
    setPrivileges(Array.isArray(document.catalog?.privileges) ? document.catalog.privileges : []);
    setAssignments(draft.assignments);
    setActions(Array.isArray(document.actions) ? document.actions : []);
    setRevision(typeof document.revision === "string" ? document.revision : "");
    setSavedSignature(catalogSignature(draft));
  }, []);

  const load = useCallback(async () => {
    const response = await fetch(joinBasePath(BASE_PATH, CATALOG_PATH), {
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error(
        await responseMessage(response, "Failed to load the fine-grained entitlement catalog"),
      );
    }
    applyDocument((await response.json()) as FgacCatalogDocument);
    setLoadState("ready");
  }, [applyDocument]);

  useEffect(() => {
    load().catch((cause) => {
      setError(
        cause instanceof Error
          ? cause.message
          : "Failed to load the fine-grained entitlement catalog",
      );
      setLoadState("failed");
    });
  }, [load]);

  useEffect(() => {
    if (loadState !== "ready") return;
    let active = true;
    api
      .listDatabaseIdentities()
      .then((identities) => {
        if (active) setDatabases(identities);
      })
      .catch(() => {
        if (active) setDatabases([]);
      });
    return () => {
      active = false;
    };
  }, [loadState]);

  const actionList = useMemo(
    () => [...actions].sort((left, right) => left.id.localeCompare(right.id)),
    [actions],
  );
  const databaseNames = useMemo(
    () => Object.fromEntries(databases.map((database) => [database.databaseID, database.name])),
    [databases],
  );
  const roleRows = useMemo(() => {
    const names = [...(serverRoles ?? [])];
    for (const role of Object.keys(assignments)) {
      if (!names.includes(role)) names.push(role);
    }
    return names;
  }, [assignments, serverRoles]);
  const dirty =
    loadState === "ready" &&
    catalogSignature({ definitions, assignments }) !== savedSignature;
  const originalDefinition = editor?.editingId
    ? definitions.find((definition) => definition.id === editor.editingId)
    : undefined;
  const editorChanged = editor && JSON.stringify(editor.input) !== JSON.stringify(definitionInput(originalDefinition));
  const { warning: navigationWarning } = useUnsavedChanges(dirty || Boolean(editorChanged));

  const updateEditor = (changes: Partial<FgacDefinitionInput>) =>
    setEditor((current) =>
      current ? { ...current, input: { ...current.input, ...changes } } : current,
    );

  const openCreate = (assignTo?: string) => {
    setActionSearch("");
    setEditorErrors([]);
    setElementId("");
    setEditor({ input: definitionInput(), assignTo, scopeChosen: false });
  };

  const openEdit = (definition: FgacDefinition) => {
    setActionSearch("");
    setEditorErrors([]);
    setElementId("");
    setEditor({ input: definitionInput(definition), editingId: definition.id, scopeChosen: true });
  };

  const closeEditor = () => {
    setEditor(null);
    setEditorErrors([]);
  };

  const selectAction = (actionId: string) => {
    const action = actionList.find((candidate) => candidate.id === actionId);
    setEditor((current) =>
      current
        ? { ...current, input: inputForAction({ ...current.input, action: actionId }, action) }
        : current,
    );
  };

  const addEntityTarget = async () => {
    if (!editor || !elementId.trim()) return;
    setResolvingEntity(true);
    try {
      const query = new URLSearchParams({ element_id: elementId.trim() });
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/fgac/entity?${query}`), { credentials: "include" });
      if (!response.ok) throw new Error(await responseMessage(response, "Entity could not be resolved."));
      const target: { databaseId: string; entityId: string } = await response.json();
      const ids = new Set(editor.input.entityIds.split(",").map((id) => id.trim()).filter(Boolean));
      ids.add(target.entityId);
      if (editor.input.databaseId !== target.databaseId) {
        throw new Error("The element belongs to a different database.");
      }
      setEditor((current) => current === editor
        ? { ...current, input: { ...current.input, entityIds: Array.from(ids).join(", ") } }
        : current);
      setEditorErrors([]);
      setElementId("");
    } catch (error) {
      setEditorErrors([error instanceof Error ? error.message : "Entity could not be resolved."]);
    } finally {
      setResolvingEntity(false);
    }
  };

  const submitDefinition = () => {
    if (!editor) return;
    const action = actionList.find((candidate) => candidate.id === editor.input.action);
    if (action && action.resourceKind !== "dbms" && !editor.scopeChosen) {
      setEditorErrors(["Choose the database scope."]);
      return;
    }
    const result = validateDefinitionInput(
      editor.input,
      actionList,
      definitions,
      editor.editingId,
    );
    if (!result.definition) {
      setEditorErrors(result.errors);
      return;
    }
    const next = upsertDefinition({ definitions, assignments }, result.definition);
    setDefinitions(next.definitions);
    setAssignments(editor.assignTo && !editor.editingId
      ? toggleRoleAssignment(next.assignments, editor.assignTo, result.definition.id)
      : next.assignments);
    setSuccess("");
    closeEditor();
  };

  const handleRemoveDefinition = (definition: FgacDefinition) => {
    const next = removeDefinition({ definitions, assignments }, definition.id);
    setDefinitions(next.definitions);
    setAssignments(next.assignments);
    setSuccess("");
  };

  const handleToggleAssignment = (role: string, definitionId: string) => {
    setAssignments((current) => toggleRoleAssignment(current, role, definitionId));
    setSuccess("");
  };

  const handleSave = async () => {
    const problems = validateCatalog({ definitions, assignments }, actions, serverRoles ?? []);
    if (problems.length > 0) {
      setSuccess("");
      setError(problems.join(" "));
      return;
    }
    setSaving(true);
    setError("");
    setSuccess("");
    try {
      const response = await fetch(joinBasePath(BASE_PATH, CATALOG_PATH), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify(catalogPutBody({ definitions, assignments }, revision)),
      });
      if (response.status === 204) {
        const message = "Access changes saved.";
        try {
          await load();
          setSuccess(message);
        } catch {
          setSuccess(message);
          setError(
            "The catalog was saved but could not be reloaded. Reload the page to read its current revision.",
          );
        }
        return;
      }
      if (response.status === 409) {
        setError(
          "The stored catalog changed since this page loaded it, so nothing was saved. Your draft is kept; reload the page to edit the current catalog.",
        );
        return;
      }
      setError(
        await responseMessage(response, "Failed to save the fine-grained entitlement catalog"),
      );
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Failed to save the fine-grained entitlement catalog",
      );
    } finally {
      setSaving(false);
    }
  };

  if (loadState === "loading") {
    return (
      <section className="bg-norse-shadow border border-norse-rune rounded-lg p-8 flex items-center justify-center">
        <div className="w-8 h-8 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
      </section>
    );
  }

  if (loadState === "failed") {
    return (
      <section className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
        <h2 className="px-4 py-3 bg-norse-stone text-sm font-semibold text-norse-silver flex items-center gap-2">
          <Lock className="w-4 h-4" />
          Fine-grained entitlements
        </h2>
        <div className="p-4 space-y-3">
          <Alert
            type="error"
            title="Fine-grained entitlements unavailable"
            message={error || "Failed to load the fine-grained entitlement catalog"}
          />
        </div>
      </section>
    );
  }

  const selectedAction = editor
    ? actionList.find((action) => action.id === editor.input.action)
    : undefined;
  const entityAction =
    selectedAction && isEntityKind(selectedAction.resourceKind) ? selectedAction : undefined;
  const actionGroups = groupFgacActions(actionList, actionSearch, selectedAction?.id);

  return (
    <section className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
      <header className="px-4 py-3 bg-norse-stone text-sm font-semibold text-norse-silver flex flex-wrap items-center justify-between gap-2">
        <h2 className="flex items-center gap-2">
          <Lock className="w-4 h-4" aria-hidden />
          Fine-grained access
        </h2>
        <span className="flex flex-wrap items-center gap-2">
          <span role="status" className={`text-xs font-normal ${dirty ? "text-amber-300" : "text-norse-fog"}`}>
            {saving ? "Saving access changes…" : dirty ? "Unsaved changes" : "All changes saved"}
          </span>
          {dirty && <Button variant="secondary" size="sm" disabled={saving} onClick={() => setDiscardOpen(true)}>Discard</Button>}
          <Button
            variant="primary"
            size="sm"
            onClick={handleSave}
            disabled={!dirty || saving}
            loading={saving}
            icon={Save}
          >
            Save changes
          </Button>
        </span>
      </header>
      <div className="p-4 space-y-6">
        {error && (
          <Alert type="error" message={error} dismissible onDismiss={() => setError("")} />
        )}
        {success && (
          <Alert type="success" message={success} dismissible onDismiss={() => setSuccess("")} />
        )}
        <FgacRoleWorkspace
          roles={roleRows}
          definitions={definitions}
          privileges={privileges}
          assignments={assignments}
          databaseNames={databaseNames}
          permissions={rolePermissions}
          dirty={dirty}
          saving={saving}
          onRolesChange={async (role) => {
            if (!dirty || !role) {
              await load();
            } else {
              setServerRoles((names) => names?.includes(role) ? names : [...(names ?? roleRows), role]);
            }
          }}
          onCreateEntitlement={openCreate}
          onEditEntitlement={openEdit}
          onRemoveEntitlement={setPendingRemoval}
          onToggle={handleToggleAssignment}
        />
      </div>

      {editor && (
        <Modal
          isOpen
          onClose={closeEditor}
          title={editor.editingId ? "Edit entitlement" : editor.assignTo ? `Create entitlement for ${editor.assignTo}` : "Create entitlement"}
          size="lg"
        >
          <div className="space-y-4">
            {editorErrors.length > 0 && (
              <Alert
                type="error"
                title="Check this entitlement"
                message={editorErrors.join(" ")}
              />
            )}
            <FormInput
              id={definitionIdField}
              label="Entitlement name"
              value={editor.input.id}
              onChange={(value) => updateEditor({ id: value })}
              placeholder="e.g. reviewers_node_traverse"
              disabled={Boolean(editor.editingId)}
              description={
                editor.editingId
                  ? "A definition ID is permanent; add a new definition to rename it."
                  : 'Lowercase letters and digits with "_", "." or "-"; must start with a letter.'
              }
            />
            <FormInput
              id={actionSearchField}
              label="Find an action"
              type="search"
              value={actionSearch}
              onChange={setActionSearch}
              placeholder="e.g. read properties"
            />
            <div>
              <label
                htmlFor={actionField}
                className="block text-sm font-medium text-norse-silver mb-2"
              >
                Action
              </label>
              <select
                id={actionField}
                value={editor.input.action}
                onChange={(event) => selectAction(event.target.value)}
                className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
              >
                <option value="">Select an action</option>
                {[...actionGroups].map(([group, entries]) => (
                  <optgroup key={group} label={group}>
                    {entries.map((action) => (
                      <option key={action.id} value={action.id}>
                        {describeFgacActionName(action.id)} · {describeAction(action)}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
              {actionGroups.size === 0 && <p role="status" className="mt-1 text-sm text-norse-silver">No matching actions.</p>}
            </div>
            {selectedAction && selectedAction.resourceKind !== "dbms" && (
              <div>
                <label
                  htmlFor={databaseField}
                  className="block text-sm font-medium text-norse-silver mb-2"
                >
                  Database
                </label>
                <select
                  id={databaseField}
                  value={editor.scopeChosen ? editor.input.databaseId : ":choose"}
                  onChange={(event) => {
                    const databaseId = event.target.value;
                    setEditor((current) => current ? {
                      ...current, scopeChosen: true, input: { ...current.input, databaseId, entityIds: "" },
                    } : current);
                  }}
                  className="w-full px-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary"
                >
                  {!editor.scopeChosen && <option value=":choose" disabled>Choose a scope</option>}
                  <option value="">All databases</option>
                  <option value=":home">Home database</option>
                  {databases.map((database) => (
                    <option key={database.databaseID} value={database.databaseID}>
                      {database.name}
                    </option>
                  ))}
                </select>
                <p className="mt-1 text-xs text-norse-silver">
                  {databases.length > 0
                    ? "Scopes store the persisted database ID, so a renamed database keeps its scope."
                    : "No specific databases are available. You can still choose Home database or All databases."}
                </p>
              </div>
            )}
            {entityAction && (
              <FormInput
                id={typesField}
                label={entityAction.resourceKind === "node" ? "Labels" : "Relationship types"}
                value={editor.input.types}
                onChange={(value) => updateEditor({ types: value })}
                placeholder="Comma separated; empty selects every one"
                description="Matched exactly against the entity's labels or relationship type."
              />
            )}
            {entityAction && !entityAction.creates && editor.input.databaseId.trim() !== "" && editor.input.databaseId !== ":home" && (
              <div className="space-y-2">
              <FormInput
                id={entityIdsField}
                label="Entity IDs"
                value={editor.input.entityIds}
                onChange={(value) => updateEditor({ entityIds: value })}
                placeholder="Comma separated; empty selects every entity in scope"
              />
              <div className="flex items-center gap-2">
                <input
                  aria-label="Cypher element ID"
                  className="flex-1 rounded-lg bg-norse-stone border border-norse-rune px-3 py-2 text-sm"
                  placeholder="Cypher element ID"
                  value={elementId}
                  onChange={(event) => setElementId(event.target.value)}
                />
                <button type="button" onClick={() => void addEntityTarget()} disabled={resolvingEntity || !elementId.trim()}
                  className="px-3 py-2 rounded-lg bg-norse-stone border border-norse-rune disabled:opacity-50">
                  Add entity
                </button>
              </div>
              </div>
            )}
            {selectedAction?.property && (
              <FormInput
                id={propertiesField}
                label="Properties"
                value={editor.input.properties}
                onChange={(value) => updateEditor({ properties: value })}
                placeholder="Comma separated; empty selects every property"
              />
            )}
            {selectedAction && isPredicateAction(selectedAction.id) && (
              <fieldset className="space-y-2 border border-norse-rune rounded-lg p-3">
                <legend className="text-sm text-norse-silver px-1">Property condition</legend>
                <select aria-label="Comparison operator" value={editor.input.predicateOperator ?? ""}
                  onChange={(event) => updateEditor({ predicateOperator: event.target.value })}
                  className="w-full px-3 py-2 bg-norse-stone border border-norse-rune rounded-lg">
                  <option value="">Always</option>
                  {["=", "<>", "<", "<=", ">", ">=", "IS NULL", "IS NOT NULL", "IN", "CONTAINS VALUE"].map((operator) => (
                    <option key={operator} value={operator}>{operator}</option>
                  ))}
                </select>
                {editor.input.predicateOperator && (
                  <FormInput id={predicatePropertyField} label="Condition property"
                    value={editor.input.predicateProperty ?? ""}
                    onChange={(value) => updateEditor({ predicateProperty: value })} />
                )}
                {editor.input.predicateOperator && !editor.input.predicateOperator.startsWith("IS ") && (
                  <FormInput id={predicateValueField} label="Comparison value"
                    value={editor.input.predicateValue ?? ""}
                    onChange={(value) => updateEditor({ predicateValue: value })}
                    placeholder={'"published"'} />
                )}
              </fieldset>
            )}
            <div>
              <label className="flex items-center gap-2 text-sm text-norse-silver">
                <input
                  type="checkbox"
                  checked={editor.input.deny}
                  onChange={(event) => updateEditor({ deny: event.target.checked })}
                  className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
                />
                Deny — overrides matching grants for this scope
              </label>
            </div>
            <div className="flex gap-2 justify-end">
              <Button variant="secondary" onClick={closeEditor}>
                Cancel
              </Button>
              <Button onClick={submitDefinition}>
                {editor.editingId ? "Apply changes" : editor.assignTo ? "Stage assignment" : "Add to catalog"}
              </Button>
            </div>
          </div>
        </Modal>
      )}
      {pendingRemoval && (
        <Modal isOpen onClose={() => setPendingRemoval(undefined)} title="Remove entitlement">
          <p className="text-sm text-norse-silver">
            Remove {pendingRemoval.id} and its assignments from {Object.values(assignments).filter((ids) => ids.includes(pendingRemoval.id)).length} roles when you save?
          </p>
          <div className="flex justify-end gap-2 mt-4">
            <Button variant="secondary" onClick={() => setPendingRemoval(undefined)}>Cancel</Button>
            <Button variant="danger" onClick={() => { handleRemoveDefinition(pendingRemoval); setPendingRemoval(undefined); }}>Stage removal</Button>
          </div>
        </Modal>
      )}
      {discardOpen && (
        <Modal isOpen onClose={() => setDiscardOpen(false)} title="Discard unsaved changes">
          <p className="text-sm text-norse-silver">Reload the saved catalog and discard changes made on this page.</p>
          <div className="flex justify-end gap-2 mt-4">
            <Button variant="secondary" onClick={() => setDiscardOpen(false)}>Keep editing</Button>
            <Button variant="danger" onClick={() => {
              setDiscardOpen(false);
              void load().then(() => { setError(""); setSuccess("Unsaved changes discarded."); })
                .catch(() => setError("Could not reload the saved catalog. Your draft is still here."));
            }}>Discard changes</Button>
          </div>
        </Modal>
      )}
      {navigationWarning}
    </section>
  );
}
