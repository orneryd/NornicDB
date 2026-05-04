import { useState, useEffect, useCallback, useId, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { UiGrid } from '@ornery/ui-grid-react';
import type { GridCellTemplateContext, GridColumnDef, GridOptions, GridRecord } from '@ornery/ui-grid-core';
import { PageLayout } from '../components/common/PageLayout';
import { PageHeader } from '../components/common/PageHeader';
import { Button } from '../components/common/Button';
import { Alert } from '../components/common/Alert';
import { FormInput } from '../components/common/FormInput';
import { Modal } from '../components/common/Modal';
import { api } from '../utils/api';
import { Database, Plus, Save, Trash2, Edit2, Shield } from 'lucide-react';
import { BASE_PATH, joinBasePath } from '../utils/basePath';

const BUILTIN_ROLES = ['admin', 'editor', 'viewer'];
const SYSTEM_DB = 'system';
const DEFAULT_DB = 'nornic';

interface PrivilegeEntry {
  role: string;
  database: string;
  read: boolean;
  write: boolean;
}

/** Permission label(s) for a role, derived from per-database privileges first, then from global role entitlements. When the privileges matrix has entries for this role, use them; otherwise use the role's entitlements (write/admin → read-write, read only → read-only). */
function getEffectivePermissionTagsForRole(
  role: string,
  privilegesMatrix: PrivilegeEntry[],
  roleEntitlements: Record<string, string[]>,
): string[] {
  const r = role.toLowerCase().trim();
  const entries = privilegesMatrix.filter((p) => p.role.toLowerCase().trim() === r);
  if (entries.length > 0) {
    const hasWrite = entries.some((e) => e.write);
    const hasRead = entries.some((e) => e.read);
    if (hasWrite) return ['read-write'];
    if (hasRead) return ['read-only'];
    return [];
  }
  const ent = roleEntitlements[r] ?? roleEntitlements[role] ?? [];
  if (ent.includes('write') || ent.includes('admin')) return ['read-write'];
  if (ent.includes('read')) return ['read-only'];
  return [];
}

interface AllowlistEntry {
  role: string;
  databases: string[];
}

interface EntitlementDef {
  id: string;
  name: string;
  description: string;
  category: string;
}

interface RoleEntitlementRow {
  role: string;
  entitlements: string[];
}

export function DatabaseAccess() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [isAdmin, setIsAdmin] = useState(false);
  const [roles, setRoles] = useState<string[]>([]);
  const [allowlist, setAllowlist] = useState<AllowlistEntry[]>([]);
  const [privilegesMatrix, setPrivilegesMatrix] = useState<PrivilegeEntry[]>([]);
  const [dbNames, setDbNames] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);

  // Role entitlements (global permissions per role)
  const [globalEntitlements, setGlobalEntitlements] = useState<EntitlementDef[]>([]);
  const [roleEntitlements, setRoleEntitlements] = useState<Record<string, string[]>>({});
  const [entitlementsDirty, setEntitlementsDirty] = useState(false);
  const [savingEntitlements, setSavingEntitlements] = useState(false);

  // User-defined roles: create / delete / rename
  const [newRoleName, setNewRoleName] = useState('');
  const [creatingRole, setCreatingRole] = useState(false);
  const [createRoleError, setCreateRoleError] = useState('');
  const [deletingRole, setDeletingRole] = useState<string | null>(null);
  const [renameTarget, setRenameTarget] = useState<{ old: string; new: string } | null>(null);
  const [renaming, setRenaming] = useState(false);
  const newRoleId = useId();
  const renameRoleId = useId();

  const loadAll = useCallback(async () => {
    setError('');
    setLoading(true);
    try {
      const [rolesRes, allowlistRes, privilegesRes, dbs, entitlementsRes, roleEntRes] = await Promise.all([
        fetch(joinBasePath(BASE_PATH, '/auth/roles'), { credentials: 'include' }),
        fetch(joinBasePath(BASE_PATH, '/auth/access/databases'), { credentials: 'include' }),
        fetch(joinBasePath(BASE_PATH, '/auth/access/privileges'), { credentials: 'include' }).catch(() => null),
        api.listDatabases().catch(() => []),
        fetch(joinBasePath(BASE_PATH, '/auth/entitlements'), { credentials: 'include' }).catch(() => null),
        fetch(joinBasePath(BASE_PATH, '/auth/role-entitlements'), { credentials: 'include' }).catch(() => null),
      ]);

      if (!rolesRes.ok) throw new Error('Failed to load roles');
      if (!allowlistRes.ok) throw new Error('Failed to load database access');

      const roleNames: string[] = await rolesRes.json();
      const list: AllowlistEntry[] = await allowlistRes.json();
      const names = (dbs as { name: string }[]).map((d) => d.name).filter(Boolean);
      if (!names.includes('system')) names.unshift('system');

      setRoles(Array.isArray(roleNames) ? roleNames : []);
      setAllowlist(Array.isArray(list) ? list : []);
      if (privilegesRes?.ok) {
        const privList: PrivilegeEntry[] = await privilegesRes.json();
        setPrivilegesMatrix(Array.isArray(privList) ? privList : []);
      } else {
        setPrivilegesMatrix([]);
      }
      setDbNames(names.length > 0 ? names : ['nornic', 'system']);
      setDirty(false);

      if (entitlementsRes?.ok) {
        const allEnt: EntitlementDef[] = await entitlementsRes.json();
        setGlobalEntitlements(Array.isArray(allEnt) ? allEnt.filter((e) => e.category === 'global') : []);
      }
      if (roleEntRes?.ok) {
        const rows: RoleEntitlementRow[] = await roleEntRes.json();
        const map: Record<string, string[]> = {};
        if (Array.isArray(rows)) {
          for (const row of rows) {
            map[row.role] = row.entitlements ?? [];
          }
        }
        setRoleEntitlements(map);
      }
      setEntitlementsDirty(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetch(joinBasePath(BASE_PATH, '/auth/me'), { credentials: 'include' })
      .then((res) => res.json())
      .then((data) => {
        const r = data.roles || [];
        if (!r.includes('admin')) {
          navigate('/security');
          return;
        }
        setIsAdmin(true);
        loadAll();
      })
      .catch(() => navigate('/security'));
  }, [navigate, loadAll]);

  const getDatabasesForRole = (role: string): string[] => {
    const entry = allowlist.find((e) => e.role.toLowerCase() === role.toLowerCase());
    return entry ? entry.databases ?? [] : [];
  };

  const setDatabasesForRole = (role: string, databases: string[]) => {
    setAllowlist((prev) => {
      const next = prev.filter((e) => e.role.toLowerCase() !== role.toLowerCase());
      next.push({ role, databases });
      return next;
    });
    setDirty(true);
  };

  const toggleDbForRole = (role: string, db: string) => {
    const current = getDatabasesForRole(role);
    const isAll = current.length === 0;
    let next: string[];
    if (isAll) {
      next = dbNames.filter((d) => d !== db);
    } else if (current.includes(db)) {
      next = current.filter((d) => d !== db);
    } else {
      next = [...current, db];
    }
    if (next.length === dbNames.length) next = [];
    setDatabasesForRole(role, next);
  };

  const getEntitlementsForRole = (role: string): string[] =>
    roleEntitlements[role] ?? [];

  const setEntitlementsForRole = (role: string, ids: string[]) => {
    setRoleEntitlements((prev) => ({ ...prev, [role]: ids }));
    setEntitlementsDirty(true);
  };

  const toggleEntitlementForRole = (role: string, entitlementId: string) => {
    const current = getEntitlementsForRole(role);
    const next = current.includes(entitlementId)
      ? current.filter((e) => e !== entitlementId)
      : [...current, entitlementId];
    setEntitlementsForRole(role, next);
  };

  const handleSaveEntitlements = async () => {
    setError('');
    setSuccess('');
    setSavingEntitlements(true);
    try {
      const mappings = roles.map((role) => ({
        role,
        entitlements: getEntitlementsForRole(role),
      }));
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/role-entitlements'), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ mappings }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to save entitlements');
      }
      setSuccess('Role entitlements saved.');
      setEntitlementsDirty(false);
      setTimeout(() => setSuccess(''), 3000);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save entitlements');
    } finally {
      setSavingEntitlements(false);
    }
  };

  const handleSave = async () => {
    setError('');
    setSuccess('');
    setSaving(true);
    try {
      const mappings = roles.map((role) => ({
        role,
        databases: getDatabasesForRole(role),
      }));
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/access/databases'), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ mappings }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to save');
      }
      setSuccess('Database access saved.');
      setDirty(false);
      setTimeout(() => setSuccess(''), 3000);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to save');
    } finally {
      setSaving(false);
    }
  };

  const handleCreateRole = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreateRoleError('');
    if (!newRoleName.trim()) return;
    setCreatingRole(true);
    try {
      const res = await fetch(joinBasePath(BASE_PATH, '/auth/roles'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ name: newRoleName.trim() }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to create role');
      }
      setNewRoleName('');
      await loadAll();
    } catch (e) {
      setCreateRoleError(e instanceof Error ? e.message : 'Failed to create role');
    } finally {
      setCreatingRole(false);
    }
  };

  const handleDeleteRole = async (role: string) => {
    if (BUILTIN_ROLES.includes(role.toLowerCase())) return;
    if (!confirm(`Delete role "${role}"? This will fail if any user has this role.`)) return;
    setDeletingRole(role);
    try {
      const res = await fetch(joinBasePath(BASE_PATH, `/auth/roles/${encodeURIComponent(role)}`), {
        method: 'DELETE',
        credentials: 'include',
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to delete role');
      }
      await loadAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to delete role');
    } finally {
      setDeletingRole(null);
    }
  };

  const handleRenameRole = async () => {
    if (!renameTarget || !renameTarget.new.trim()) return;
    setRenaming(true);
    try {
      const res = await fetch(joinBasePath(BASE_PATH, `/auth/roles/${encodeURIComponent(renameTarget.old)}`), {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify({ name: renameTarget.new.trim() }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        throw new Error(data.message || 'Failed to rename role');
      }
      setRenameTarget(null);
      await loadAll();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to rename role');
    } finally {
      setRenaming(false);
    }
  };

  const userDefinedRoles = roles.filter((r) => !BUILTIN_ROLES.includes(r.toLowerCase()));

  const accessGridData = useMemo<GridRecord[]>(() => roles.map((role) => ({
    __gridId: role,
    role,
  })), [roles]);

  const accessColumnDefs = useMemo<GridColumnDef[]>(() => [
    {
      name: 'role',
      displayName: 'Role',
      field: 'role',
      width: 'minmax(12rem, 1fr)',
    },
    ...dbNames.map((db) => ({
      name: db,
      displayName: db,
      width: '120px',
      enableSorting: false,
      enableFiltering: false,
    })),
  ], [dbNames]);

  const accessGridOptions = useMemo<GridOptions>(() => ({
    id: 'database-access-grid',
    data: accessGridData,
    columnDefs: accessColumnDefs,
    rowIdentity: (row) => String(row.__gridId),
    enableSorting: true,
    enableFiltering: true,
    viewportHeight: 420,
    emptyMessage: 'No roles found',
  }), [accessColumnDefs, accessGridData]);

  const entitlementGridData = useMemo<GridRecord[]>(() => roles.map((role) => ({
    __gridId: role,
    role,
  })), [roles]);

  const entitlementColumnDefs = useMemo<GridColumnDef[]>(() => [
    {
      name: 'role',
      displayName: 'Role',
      field: 'role',
      width: 'minmax(12rem, 1fr)',
    },
    ...globalEntitlements.map((ent) => ({
      name: ent.id,
      displayName: ent.name,
      width: '120px',
      enableSorting: false,
      enableFiltering: false,
    })),
  ], [globalEntitlements]);

  const entitlementGridOptions = useMemo<GridOptions>(() => ({
    id: 'role-entitlements-grid',
    data: entitlementGridData,
    columnDefs: entitlementColumnDefs,
    rowIdentity: (row) => String(row.__gridId),
    enableSorting: true,
    enableFiltering: true,
    viewportHeight: 420,
    emptyMessage: 'No entitlements found',
  }), [entitlementColumnDefs, entitlementGridData]);

  const renderAccessCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & { role: string };
    const role = row.role;

    if (ctx.column.name === 'role') {
      const permissionTags = getEffectivePermissionTagsForRole(role, privilegesMatrix, roleEntitlements);
      return (
        <div className="flex items-center gap-2 flex-wrap py-1">
          <span className="font-medium text-white capitalize">{role}</span>
          <span className="flex gap-1.5">
            {permissionTags.map((tag) => (
              <span
                key={tag}
                className={`px-2 py-0.5 rounded text-xs font-medium ${
                  tag === 'read-only'
                    ? 'bg-amber-500/20 text-amber-400'
                    : 'bg-emerald-500/20 text-emerald-400'
                }`}
              >
                {tag}
              </span>
            ))}
          </span>
        </div>
      );
    }

    const db = ctx.column.name;
    const dbs = getDatabasesForRole(role);
    const isAll = dbs.length === 0;
    const checked = isAll || dbs.includes(db);
    const isAdminSystemOrDefault =
      role.toLowerCase() === 'admin' && (db === SYSTEM_DB || db === DEFAULT_DB);

    return (
      <div className="flex items-center justify-center py-1" onClick={(e) => e.stopPropagation()}>
        <input
          type="checkbox"
          checked={checked}
          disabled={isAdminSystemOrDefault}
          onChange={() => {
            toggleDbForRole(role, db);
            setDirty(true);
            setDirty(true);
          }}
          className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary disabled:opacity-70"
          title={isAdminSystemOrDefault ? 'Admin always has access to system and default database' : undefined}
        />
      </div>
    );
  };

  const accessCellRenderers = useMemo(
    () => Object.fromEntries(accessColumnDefs.map(({ name }) => [name, renderAccessCell])),
    [accessColumnDefs, renderAccessCell],
  );
  const renderEntitlementCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & { role: string };
    const role = row.role;

    if (ctx.column.name === 'role') {
      return <div className="font-medium text-white capitalize py-1">{role}</div>;
    }

    const adminEntitlementsReadOnly = role.toLowerCase() === 'admin';

    return (
      <div className="flex items-center justify-center py-1" onClick={(e) => e.stopPropagation()}>
        <input
          type="checkbox"
          checked={getEntitlementsForRole(role).includes(ctx.column.name)}
          disabled={adminEntitlementsReadOnly}
          onChange={() => toggleEntitlementForRole(role, ctx.column.name)}
          className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary disabled:opacity-70"
        />
      </div>
    );
  };

  const entitlementCellRenderers = useMemo(
    () => Object.fromEntries(entitlementColumnDefs.map(({ name }) => [name, renderEntitlementCell])),
    [entitlementColumnDefs, renderEntitlementCell],
  );
  if (!isAdmin) return null;

  if (loading) {
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
        title="Database Access"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <Button
            variant="primary"
            onClick={handleSave}
            disabled={!dirty || saving}
            loading={saving}
            icon={Save}
          >
            Save
          </Button>
        }
      />

      <main className="max-w-5xl mx-auto p-6 space-y-8">
        {error && (
          <Alert type="error" message={error} dismissible onDismiss={() => setError('')} />
        )}
        {success && (
          <Alert type="success" message={success} dismissible onDismiss={() => setSuccess('')} />
        )}

        <p className="text-norse-silver text-sm">
          Assign which databases each role can see and access. Empty list means <strong>all databases</strong>.
          Read-only / read-write labels are derived from per-database privileges when set, otherwise from each role's entitlements below (Read/Write).
        </p>

        {/* Database access by role */}
        <div className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
          <h2 className="px-4 py-3 bg-norse-stone text-sm font-semibold text-norse-silver flex items-center gap-2">
            <Database className="w-4 h-4" />
            Access by role
          </h2>
          <div className="nornic-grid p-4">
            <UiGrid options={accessGridOptions} cellRenderers={accessCellRenderers} />
          </div>
        </div>

        {/* Role entitlements (global permissions per role) */}
        {globalEntitlements.length > 0 && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
            <h2 className="px-4 py-3 bg-norse-stone text-sm font-semibold text-norse-silver flex items-center justify-between gap-2">
              <span className="flex items-center gap-2">
                <Shield className="w-4 h-4" />
                Role entitlements
              </span>
              <Button
                variant="primary"
                size="sm"
                onClick={handleSaveEntitlements}
                disabled={!entitlementsDirty || savingEntitlements}
                loading={savingEntitlements}
                icon={Save}
              >
                Save entitlements
              </Button>
            </h2>
            <p className="px-4 py-2 text-xs text-norse-fog border-b border-norse-rune">
              Assign global permissions to each role. These control API access (read, write, admin, user management, etc.).
            </p>
            <div className="nornic-grid p-4">
              <UiGrid options={entitlementGridOptions} cellRenderers={entitlementCellRenderers} />
            </div>
          </div>
        )}

        {/* User-defined roles */}
        <div className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
          <h2 className="px-4 py-3 bg-norse-stone text-sm font-semibold text-norse-silver">
            User-defined roles
          </h2>
          <div className="p-4 space-y-4">
            <form onSubmit={handleCreateRole} className="flex gap-2 flex-wrap items-end">
              <FormInput
                id={newRoleId}
                label="New role name"
                value={newRoleName}
                onChange={(v) => setNewRoleName(v)}
                placeholder="e.g. analyst"
              />
              <Button
                type="submit"
                variant="secondary"
                disabled={!newRoleName.trim() || creatingRole}
                loading={creatingRole}
                icon={Plus}
              >
                Create role
              </Button>
            </form>
            {createRoleError && (
              <Alert type="error" message={createRoleError} onDismiss={() => setCreateRoleError('')} />
            )}
            {userDefinedRoles.length > 0 ? (
              <ul className="space-y-2">
                {userDefinedRoles.map((role) => (
                  <li
                    key={role}
                    className="flex items-center justify-between py-2 px-3 rounded bg-norse-stone/50"
                  >
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-norse-silver">{role}</span>
                      {getEffectivePermissionTagsForRole(role, privilegesMatrix, roleEntitlements).map((tag) => (
                        <span
                          key={tag}
                          className={`px-2 py-0.5 rounded text-xs font-medium ${
                            tag === 'read-only'
                              ? 'bg-amber-500/20 text-amber-400'
                              : 'bg-emerald-500/20 text-emerald-400'
                          }`}
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                    <div className="flex gap-2">
                      <Button
                        size="sm"
                        variant="secondary"
                        icon={Edit2}
                        onClick={() => setRenameTarget({ old: role, new: role })}
                      >
                        Rename
                      </Button>
                      <Button
                        size="sm"
                        variant="danger"
                        icon={Trash2}
                        disabled={deletingRole === role}
                        onClick={() => handleDeleteRole(role)}
                      >
                        Delete
                      </Button>
                    </div>
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-norse-fog text-sm">No user-defined roles. Create one above.</p>
            )}
          </div>
        </div>
      </main>

      {renameTarget && (
        <Modal
          isOpen={!!renameTarget}
          onClose={() => setRenameTarget(null)}
          title="Rename role"
        >
          <div className="space-y-4">
            <FormInput
              id={renameRoleId}
              label="New name"
              value={renameTarget.new}
              onChange={(v) => setRenameTarget((prev) => prev && { ...prev, new: v })}
            />
            <div className="flex gap-2 justify-end">
              <Button variant="secondary" onClick={() => setRenameTarget(null)}>
                Cancel
              </Button>
              <Button
                onClick={handleRenameRole}
                disabled={!renameTarget.new.trim() || renaming}
                loading={renaming}
              >
                Rename
              </Button>
            </div>
          </div>
        </Modal>
      )}
    </PageLayout>
  );
}
