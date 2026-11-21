import { useEffect, useId, useMemo, useState } from "react";
import { Edit2, Plus, Shield, Trash2, Users } from "lucide-react";
import { api } from "../../utils/api";
import { BASE_PATH, joinBasePath } from "../../utils/basePath";
import { describeFgacActionName, describeScope, type FgacDefinition, type FgacCypherPrivilege } from "../../utils/fgacCatalog";
import { Alert } from "../common/Alert";
import { Button } from "../common/Button";
import { FormInput } from "../common/FormInput";
import { Modal } from "../common/Modal";

export interface FgacRolePermissions {
  create: boolean;
  rename: boolean;
  remove: boolean;
  listUsers: boolean;
  assignUsers: boolean;
  removeUsers: boolean;
}

interface RoleUser {
  username: string;
  roles: string[];
}

interface WorkspaceProps {
  roles: string[];
  definitions: FgacDefinition[];
  privileges: FgacCypherPrivilege[];
  assignments: Record<string, string[]>;
  databaseNames: Record<string, string>;
  permissions: FgacRolePermissions;
  dirty: boolean;
  saving: boolean;
  onRolesChange: (role?: string) => Promise<void>;
  onCreateEntitlement: (role?: string) => void;
  onEditEntitlement: (definition: FgacDefinition) => void;
  onRemoveEntitlement: (definition: FgacDefinition) => void;
  onToggle: (role: string, entitlement: string) => void;
}

type Pane = "entitlements" | "members" | "library";
type RoleDialog = "create" | "rename" | "delete";

const fixedRoles = new Set(["admin", "editor", "viewer", "public"]);

function matchesEntitlement(definition: FgacDefinition, search: string): boolean {
  if (!search) return true;
  return [definition.id, definition.action, describeFgacActionName(definition.action)].some((value) =>
    value.toLowerCase().includes(search));
}

async function runRoleStatement(statement: string, parameters: Record<string, string>): Promise<void> {
  const response = await api.executeSystemCypher(statement, parameters);
  if (response.errors?.length) throw new Error(response.errors[0].message);
}

export function FgacRoleWorkspace(props: WorkspaceProps) {
  const { roles, definitions, privileges, assignments, databaseNames, permissions, dirty, saving } = props;
  const [role, setRole] = useState("");
  const [roleFilter, setRoleFilter] = useState("");
  const [filter, setFilter] = useState("");
  const [pane, setPane] = useState<Pane>("entitlements");
  const [roleDialog, setRoleDialog] = useState<RoleDialog>();
  const [roleName, setRoleName] = useState("");
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [busy, setBusy] = useState(false);
  const [users, setUsers] = useState<RoleUser[]>([]);
  const [usersLoaded, setUsersLoaded] = useState(false);
  const [user, setUser] = useState("");
  const roleNameId = useId();
  const rolesFilterId = useId();
  const entitlementFilterId = useId();
  const memberId = useId();

  const orderedRoles = useMemo(() => [...roles].sort((left, right) => left.localeCompare(right)), [roles]);
  const selectedRole = orderedRoles.includes(role)
    ? role
    : orderedRoles.find((name) => !fixedRoles.has(name)) ?? orderedRoles.find((name) => name === "public") ?? orderedRoles[0] ?? "";
  const rolePrivileges = privileges.filter((privilege) => privilege.role === selectedRole);
  const chosen = assignments[selectedRole] ?? [];
  const selectedIsFixed = fixedRoles.has(selectedRole.toLowerCase());
  const normalizedFilter = filter.trim().toLowerCase();
  const shown = definitions.filter((definition) => matchesEntitlement(definition, normalizedFilter));
  const shownRoles = orderedRoles.filter((name) => name.toLowerCase().includes(roleFilter.trim().toLowerCase()));

  const choosePane = (next: Pane) => {
    setPane(next);
    setFilter("");
    setError("");
    setNotice("");
  };

  useEffect(() => {
    if (pane !== "members" || usersLoaded || !permissions.listUsers) return;
    const controller = new AbortController();
    fetch(joinBasePath(BASE_PATH, "/auth/users"), { credentials: "include", signal: controller.signal })
      .then(async (response) => {
        if (!response.ok) throw new Error("Could not load role members.");
        const accounts = await response.json() as RoleUser[];
        if (controller.signal.aborted) return;
        setUsers(Array.isArray(accounts) ? accounts : []);
        setUsersLoaded(true);
      })
      .catch((cause: unknown) => {
        if (!controller.signal.aborted) setError(cause instanceof Error ? cause.message : "Could not load role members.");
      });
    return () => controller.abort();
  }, [pane, permissions.listUsers, usersLoaded]);

  const openRoleDialog = (next: RoleDialog) => {
    setRoleName(next === "rename" ? selectedRole : "");
    setError("");
    setRoleDialog(next);
  };

  const changeRole = async (event: React.FormEvent) => {
    event.preventDefault();
    if (busy || !roleDialog) return;
    const name = roleName.trim().toLowerCase();
    if (roleDialog !== "delete" && !name) return;
    const oldRole = selectedRole;
    const action = roleDialog;
    setBusy(true);
    setError("");
    try {
      switch (action) {
        case "create":
          await runRoleStatement("CREATE ROLE $role", { role: name });
          break;
        case "rename":
          await runRoleStatement("RENAME ROLE $role TO $name", { role: oldRole, name });
          break;
        case "delete":
          await runRoleStatement("DROP ROLE $role", { role: oldRole });
          break;
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The role could not be changed.");
      setBusy(false);
      return;
    }
    const nextRole = action === "delete" ? undefined : name;
    setRole(nextRole ?? "");
    setRoleFilter("");
    setPane("entitlements");
    setUsersLoaded(false);
    setRoleDialog(undefined);
    setNotice(action === "delete" ? `Removed role ${oldRole}.` : `Saved role ${name}.`);
    try {
      await props.onRolesChange(nextRole);
    } catch {
      setError("The role change was saved. Reload the catalog to see the current roles.");
    } finally {
      setBusy(false);
    }
  };

  const changeMembership = async (username: string, assign: boolean) => {
    setBusy(true);
    setError("");
    setNotice("");
    try {
      await runRoleStatement(
        assign ? "GRANT ROLE $role TO $user" : "REVOKE ROLE $role FROM $user",
        { role: selectedRole, user: username },
      );
      setUsers((accounts) => accounts.map((account) => account.username === username
        ? { ...account, roles: assign ? [...account.roles, selectedRole] : account.roles.filter((value) => value !== selectedRole) }
        : account));
      setUser("");
      setNotice(assign ? `Assigned ${selectedRole} to ${username}.` : `Removed ${selectedRole} from ${username}.`);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The role assignment could not be changed.");
    } finally {
      setBusy(false);
    }
  };

  const canManageRole = !dirty && !saving && !busy;
  const assignedUsers = users.filter((account) => account.roles.includes(selectedRole));
  const availableUsers = users.filter((account) => !account.roles.includes(selectedRole));

  const entitlementRow = (definition: FgacDefinition, allowToggle: boolean) => {
    const assigned = chosen.includes(definition.id);
    const assignedCount = Object.values(assignments).filter((ids) => ids.includes(definition.id)).length;
    return (
      <li key={definition.id} className="rounded-lg border border-norse-rune bg-norse-deep/70 px-4 py-3 flex flex-wrap gap-3 justify-between items-start">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <strong className="text-sm text-white break-words">{definition.id}</strong>
            <span className={`text-xs rounded-md px-2 py-0.5 ${definition.deny ? "bg-red-500/15 text-red-300" : "bg-emerald-500/15 text-emerald-300"}`}>
              {definition.deny ? "Deny" : "Grant"}
            </span>
          </div>
          <p className="mt-1 text-sm capitalize text-norse-silver">{describeFgacActionName(definition.action)}</p>
          <p className="mt-1 break-words text-xs text-norse-fog">{describeScope(definition, databaseNames)}</p>
          {!allowToggle && <p className="mt-1 text-xs text-norse-fog">{assignedCount} {assignedCount === 1 ? "role" : "roles"}</p>}
        </div>
        <div className="flex flex-wrap gap-1">
          {allowToggle ? (
            <Button size="sm" variant="secondary" disabled={saving}
              aria-label={`${assigned ? "Unassign" : "Assign"} ${definition.id} ${assigned ? "from" : "to"} ${selectedRole}`}
              onClick={() => props.onToggle(selectedRole, definition.id)}>
              {assigned ? "Unassign" : "Assign"}
            </Button>
          ) : (
            <>
              <Button size="sm" variant="secondary" icon={Edit2} disabled={saving} onClick={() => props.onEditEntitlement(definition)}>Edit</Button>
              <Button size="sm" variant="secondary" icon={Trash2} disabled={saving} onClick={() => props.onRemoveEntitlement(definition)}>Remove</Button>
            </>
          )}
        </div>
      </li>
    );
  };

  return (
    <>
      <div className="grid gap-4 lg:grid-cols-[minmax(12rem,17rem)_minmax(0,1fr)]">
        <aside className="rounded-xl border border-norse-rune bg-norse-deep/70 p-4 min-w-0" aria-labelledby="fgac-role-list">
          <div className="flex items-center gap-2 justify-between">
            <h3 id="fgac-role-list" className="font-semibold flex items-center gap-2"><Shield size={16} /> Roles</h3>
            {permissions.create && <Button variant="secondary" size="sm" icon={Plus} disabled={saving || busy} onClick={() => openRoleDialog("create")}>New role</Button>}
          </div>
          <div className="mt-4">
            <FormInput id={rolesFilterId} label="Find a role" type="search" value={roleFilter} onChange={setRoleFilter} />
          </div>
          <ul className="mt-3 space-y-1 max-h-80 overflow-auto">
            {shownRoles.map((name) => (
              <li key={name}>
                <button type="button" aria-pressed={name === selectedRole}
                  className={`w-full rounded-lg px-3 py-2.5 flex items-center justify-between gap-2 text-left text-sm focus-visible:outline focus-visible:outline-2 focus-visible:outline-nornic-primary ${name === selectedRole ? "bg-norse-stone font-semibold text-white" : "text-norse-silver hover:bg-norse-stone/60"}`}
                  onClick={() => { setRole(name); setUser(""); choosePane("entitlements"); }}>
                  <span className="break-all">{name}</span>
                  <span className="text-xs text-norse-fog" aria-label={`${assignments[name]?.length ?? 0} entitlements`}>{assignments[name]?.length ?? 0}</span>
                </button>
              </li>
            ))}
          </ul>
          {shownRoles.length === 0 && <p className="text-sm text-norse-fog py-2">No matching roles.</p>}
        </aside>

        <section className="rounded-xl border border-norse-rune bg-norse-shadow p-4 sm:p-5 min-w-0" aria-labelledby="fgac-selected-role">
          <div className="flex items-start justify-between flex-wrap gap-3">
            <div className="min-w-0">
              <h3 id="fgac-selected-role" className="text-xl font-semibold break-all">{selectedRole || "Select a role"}</h3>
              {!!selectedRole && <p className="text-sm text-norse-fog mt-1">{chosen.length} {chosen.length === 1 ? "assigned entitlement" : "assigned entitlements"}</p>}
            </div>
            {!!selectedRole && !selectedIsFixed && (permissions.rename || permissions.remove) && (
              <div className="flex flex-wrap gap-2">
                {permissions.rename && <Button size="sm" variant="secondary" disabled={!canManageRole} onClick={() => openRoleDialog("rename")}>Rename</Button>}
                {permissions.remove && <Button size="sm" variant="secondary" disabled={!canManageRole} onClick={() => openRoleDialog("delete")}>Remove role</Button>}
              </div>
            )}
          </div>
          {dirty && !!selectedRole && !selectedIsFixed && (permissions.rename || permissions.remove) &&
            <p className="text-xs text-norse-fog mt-2">Save your changes before renaming or removing a role.</p>}

          <nav aria-label="Fine-grained access sections" className="flex flex-wrap gap-2 my-4 border-b border-norse-rune pb-3">
            <Button size="sm" className="text-white!" variant={pane === "entitlements" ? "success" : "secondary"} aria-pressed={pane === "entitlements"} onClick={() => choosePane("entitlements")}>Entitlements</Button>
            {permissions.listUsers && selectedRole !== "public" && <Button size="sm" className="text-white!" icon={Users} variant={pane === "members" ? "success" : "secondary"} aria-pressed={pane === "members"} onClick={() => choosePane("members")}>Members</Button>}
            <Button size="sm" className="text-white!" variant={pane === "library" ? "success" : "secondary"} aria-pressed={pane === "library"} onClick={() => choosePane("library")}>Entitlement library</Button>
          </nav>

          {error && !roleDialog && <Alert type="error" message={error} dismissible onDismiss={() => setError("")} />}
          {notice && <p role="status" className="text-sm text-emerald-300 mb-3">{notice}</p>}

          {pane !== "members" ? (
            <div className="space-y-4">
              <div className="flex flex-wrap justify-between items-end gap-3">
                <div className="flex-1 min-w-[10rem] max-w-sm">
                  <FormInput id={entitlementFilterId} label="Find an entitlement" type="search" value={filter} onChange={setFilter} />
                </div>
                <Button size="sm" icon={Plus} disabled={saving} onClick={() => props.onCreateEntitlement(pane === "entitlements" ? selectedRole : undefined)}>
                  {pane === "entitlements" && selectedRole ? "Create and assign" : "New entitlement"}
                </Button>
              </div>
              {pane === "library" ? (
                shown.length ? <ul className="space-y-2">{shown.map((entry) => entitlementRow(entry, false))}</ul>
                  : <p className="text-sm text-norse-fog">{normalizedFilter ? "No entitlements match your search." : "Create an entitlement to add it to the library."}</p>
              ) : (
                selectedRole ? (
                  <>
                    <div className="space-y-2">
                      <h4 className="text-sm font-semibold">Assigned</h4>
                      <ul className="space-y-2">{shown.filter((entry) => chosen.includes(entry.id)).map((entry) => entitlementRow(entry, true))}</ul>
                      {!shown.some((entry) => chosen.includes(entry.id)) && <p className="text-sm text-norse-fog">{normalizedFilter ? "No assigned entitlements match your search." : "This role has no assigned entitlements."}</p>}
                    </div>
                    {rolePrivileges.length > 0 && (
                      <details className="rounded-lg border border-norse-rune p-3">
                        <summary className="text-sm font-medium cursor-pointer">
                          Cypher privileges ({rolePrivileges.length})
                        </summary>
                        <ul className="mt-2 space-y-2 text-xs text-norse-silver">
                          {rolePrivileges.map((privilege) => (
                            <li key={`${privilege.key}-${Boolean(privilege.deny)}`} className="break-words">
                              <span className={privilege.deny ? "text-red-300" : "text-emerald-300"}>{privilege.deny ? "Deny" : "Grant"}</span>
                              {" "}{privilege.clause}
                            </li>
                          ))}
                        </ul>
                      </details>
                    )}
                    <div className="space-y-2">
                      <h4 className="text-sm font-semibold">Available to assign</h4>
                      <ul className="space-y-2">{shown.filter((entry) => !chosen.includes(entry.id)).map((entry) => entitlementRow(entry, true))}</ul>
                      {!shown.some((entry) => !chosen.includes(entry.id)) && <p className="text-sm text-norse-fog">{normalizedFilter ? "No available entitlements match your search." : "Create a scoped entitlement or choose another role."}</p>}
                    </div>
                  </>
                ) : <p className="text-sm text-norse-fog">Create a role to assign entitlements.</p>
              )}
            </div>
          ) : (
            <div className="space-y-3">
              <h4 className="text-sm font-semibold">Members of {selectedRole}</h4>
              {!usersLoaded && !error && <p role="status" className="text-sm text-norse-fog">Loading members…</p>}
              {usersLoaded && !assignedUsers.length && <p className="text-sm text-norse-fog">No users have this role.</p>}
              <ul className="space-y-2">
                {assignedUsers.map((account) => (
                  <li key={account.username} className="flex items-center justify-between gap-2 py-2 px-3 rounded bg-norse-deep/70 text-sm break-all">
                    {account.username}
                    {permissions.removeUsers && <Button size="sm" variant="secondary" disabled={busy} onClick={() => void changeMembership(account.username, false)}>Remove member</Button>}
                  </li>
                ))}
              </ul>
              {permissions.assignUsers && availableUsers.length > 0 && (
                <form className="flex flex-wrap items-end gap-2" onSubmit={(event) => { event.preventDefault(); if (user) void changeMembership(user, true); }}>
                  <label htmlFor={memberId} className="text-sm space-y-1 flex-1 min-w-[10rem]">
                    <span>Add a user</span>
                    <select id={memberId} value={user} onChange={(event) => setUser(event.target.value)} className="block w-full rounded-lg border border-norse-rune bg-norse-stone px-3 py-2">
                      <option value="">Select a user</option>
                      {availableUsers.map(({ username }) => <option key={username}>{username}</option>)}
                    </select>
                  </label>
                  <Button type="submit" size="sm" disabled={!user || busy}>Assign role</Button>
                </form>
              )}
            </div>
          )}
        </section>
      </div>

      {roleDialog && (
        <Modal isOpen onClose={() => { if (!busy) setRoleDialog(undefined); }} title={roleDialog === "create" ? "Create role" : roleDialog === "rename" ? "Rename role" : "Remove role"}>
          <form onSubmit={(event) => void changeRole(event)} className="space-y-4">
            {error && <Alert type="error" message={error} />}
            {roleDialog === "delete" ? (
              <p className="text-sm text-norse-silver">Remove {selectedRole} and all of its assignments? Users will lose access granted through this role.</p>
            ) : (
              <FormInput id={roleNameId} label="Role name" value={roleName} onChange={setRoleName} required disabled={busy} />
            )}
            <div className="flex justify-end gap-2">
              <Button type="button" variant="secondary" disabled={busy} onClick={() => setRoleDialog(undefined)}>Cancel</Button>
              <Button type="submit" variant={roleDialog === "delete" ? "danger" : "primary"} loading={busy} disabled={busy || (roleDialog !== "delete" && !roleName.trim())}>
                {roleDialog === "create" ? "Create role" : roleDialog === "rename" ? "Save name" : "Remove role"}
              </Button>
            </div>
          </form>
        </Modal>
      )}
    </>
  );
}
