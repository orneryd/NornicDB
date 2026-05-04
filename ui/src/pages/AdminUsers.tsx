import { useState, useEffect, useCallback, useId, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { UiGrid } from '@ornery/ui-grid-react';
import type { GridCellTemplateContext, GridColumnDef, GridOptions, GridRecord, UiGridApi } from '@ornery/ui-grid-core';
import { PageLayout } from '../components/common/PageLayout';
import { PageHeader } from '../components/common/PageHeader';
import { FormInput } from '../components/common/FormInput';
import { Button } from '../components/common/Button';
import { Alert } from '../components/common/Alert';
import { Plus, Edit, Trash2 } from 'lucide-react';
import { BASE_PATH, joinBasePath } from '../utils/basePath';

interface User {
  username: string;
  email?: string;
  roles: string[];
  disabled?: boolean;
  created_at?: string;
  last_login?: string;
  auth_method?: string;
}

export function AdminUsers() {
  const navigate = useNavigate();
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [isAdmin, setIsAdmin] = useState(false);
  const [availableRoles, setAvailableRoles] = useState<string[]>(['admin', 'editor', 'viewer']);
  const createUsernameId = useId();
  const createPasswordId = useId();
  const createEmailId = useId();
  const createRolesId = useId();
  const [usersGridApi, setUsersGridApi] = useState<UiGridApi | null>(null);

  // Create user state
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newUsername, setNewUsername] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newEmail, setNewEmail] = useState('');
  const [newRoles, setNewRoles] = useState<string[]>(['viewer']);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState('');

  const loadUsers = useCallback(async () => {
    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/users'), {
        credentials: 'include'
      });
      
      if (!response.ok) {
        throw new Error('Failed to load users');
      }
      
      const data = await response.json();
      setUsers(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load users');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadRoles = useCallback(async () => {
    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/roles'), { credentials: 'include' });
      if (response.ok) {
        const names = await response.json();
        if (Array.isArray(names) && names.length > 0) {
          setAvailableRoles(names);
        }
      }
    } catch {
      // Keep default ['admin', 'editor', 'viewer'] if API unavailable
    }
  }, []);

  useEffect(() => {
    // Check if user is admin
    fetch(joinBasePath(BASE_PATH, '/auth/me'), {
      credentials: 'include'
    })
      .then(res => res.json())
      .then(data => {
        const roles = data.roles || [];
        if (!roles.includes('admin')) {
          navigate('/security');
          return;
        }
        setIsAdmin(true);
        loadUsers();
        loadRoles();
      })
      .catch(() => {
        navigate('/security');
      });
  }, [navigate, loadUsers, loadRoles]);

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreateError('');
    setCreating(true);

    try {
      const response = await fetch(joinBasePath(BASE_PATH, '/auth/users'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          username: newUsername,
          password: newPassword,
          email: newEmail || undefined,
          roles: newRoles,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to create user');
      }

      // Reset form
      setNewUsername('');
      setNewPassword('');
      setNewEmail('');
      setNewRoles(['viewer']);
      setShowCreateForm(false);
      
      // Reload users
      await loadUsers();
    } catch (err) {
      setCreateError(err instanceof Error ? err.message : 'Failed to create user');
    } finally {
      setCreating(false);
    }
  };

  const handleUpdateUser = useCallback(async (user: User, overrides: Partial<User>) => {
    setError('');
    try {
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/users/${user.username}`), {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          email: overrides.email ?? user.email ?? undefined,
          roles: overrides.roles ?? user.roles,
          disabled: overrides.disabled ?? user.disabled ?? false,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to update user');
      }

      await loadUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update user');
      await loadUsers();
    }
  }, [loadUsers]);

  const handleDeleteUser = async (username: string) => {
    if (!confirm(`Are you sure you want to delete user "${username}"? This action cannot be undone.`)) {
      return;
    }

    try {
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/users/${username}`), {
        method: 'DELETE',
        credentials: 'include',
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to delete user');
      }

      await loadUsers();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete user');
    }
  };

  const toggleRole = (role: string, roles: string[], setRoles: (roles: string[]) => void) => {
    if (roles.includes(role)) {
      setRoles(roles.filter(r => r !== role));
    } else {
      setRoles([...roles, role]);
    }
  };

  const usersGridData = useMemo<GridRecord[]>(() => users.map((user) => ({
    ...user,
    __gridId: user.username,
  })), [users]);

  const userColumns = useMemo<GridColumnDef[]>(() => [
    {
      name: 'username',
      displayName: 'Username',
      field: 'username',
      width: 'minmax(10rem, 1.1fr)',
    },
    {
      name: 'email',
      displayName: 'Email',
      field: 'email',
      enableCellEdit: true,
      width: 'minmax(14rem, 1.4fr)',
    },
    {
      name: 'roles',
      displayName: 'Roles',
      field: 'roles',
      enableSorting: false,
      width: 'minmax(16rem, 1.5fr)',
    },
    {
      name: 'status',
      displayName: 'Status',
      field: 'disabled',
      enableSorting: false,
      width: '140px',
    },
    {
      name: 'last_login',
      displayName: 'Last Login',
      field: 'last_login',
      width: 'minmax(12rem, 1fr)',
      formatter: (value) => (value ? new Date(String(value)).toLocaleString() : 'Never'),
    },
    {
      name: 'actions',
      displayName: 'Actions',
      width: '180px',
      enableSorting: false,
      enableFiltering: false,
    },
  ], []);

  const userGridOptions = useMemo<GridOptions>(() => ({
    id: 'admin-users-grid',
    data: usersGridData,
    columnDefs: userColumns,
    rowIdentity: (row) => String(row.__gridId),
    enableSorting: true,
    enableFiltering: true,
    enableCellEdit: true,
    enableCellEditOnFocus: true,
    viewportHeight: 560,
    emptyMessage: 'No users found',
  }), [userColumns, usersGridData]);

  useEffect(() => {
    if (!usersGridApi) {
      return;
    }

    return usersGridApi.edit.on.afterCellEdit((row, column, newValue, oldValue) => {
      if (column.name !== 'email') {
        return;
      }

      const username = String(row.username ?? '');
      const user = users.find((entry) => entry.username === username);
      if (!user) {
        return;
      }

      const nextEmail = String(newValue ?? '').trim();
      const previousEmail = String(oldValue ?? '').trim();
      if (nextEmail === previousEmail) {
        return;
      }

      void handleUpdateUser(user, { email: nextEmail || undefined });
    });
  }, [handleUpdateUser, users, usersGridApi]);

  const renderUserCell = (ctx: GridCellTemplateContext) => {
    const row = ctx.row as GridRecord & User;

    if (ctx.column.name === 'roles') {
      return (
        <div className="flex flex-wrap gap-3 py-1" onClick={(e) => e.stopPropagation()}>
          {availableRoles.map((role) => {
            const checked = row.roles.includes(role);
            const disableRemoval = checked && row.roles.length === 1;
            return (
              <label key={role} className="inline-flex items-center gap-2 text-xs text-norse-silver">
                <input
                  type="checkbox"
                  checked={checked}
                  disabled={disableRemoval}
                  onChange={() => {
                    const nextRoles = checked
                      ? row.roles.filter((entry) => entry !== role)
                      : [...row.roles, role];
                    if (nextRoles.length > 0) {
                      void handleUpdateUser(row, { roles: nextRoles });
                    }
                  }}
                  className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary"
                />
                <span className="capitalize">{role}</span>
              </label>
            );
          })}
        </div>
      );
    }

    if (ctx.column.name === 'status') {
      return (
        <div className="py-1" onClick={(e) => e.stopPropagation()}>
          <button
            type="button"
            onClick={() => void handleUpdateUser(row, { disabled: !row.disabled })}
            className={`px-2 py-1 rounded text-xs ${row.disabled ? 'bg-red-500/20 text-red-400' : 'bg-green-500/20 text-green-400'}`}
          >
            {row.disabled ? 'Disabled' : 'Active'}
          </button>
        </div>
      );
    }

    if (ctx.column.name === 'actions') {
      return (
        <div className="flex gap-2 py-1" onClick={(e) => e.stopPropagation()}>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => usersGridApi?.edit.beginCellEdit(String(row.__gridId), 'email')}
            icon={Edit}
          >
            Edit
          </Button>
          <Button
            variant="danger"
            size="sm"
            onClick={() => handleDeleteUser(row.username)}
            icon={Trash2}
          >
            Delete
          </Button>
        </div>
      );
    }

    if (ctx.column.name === 'username') {
      return <div className="font-medium text-white py-1">{String(ctx.value ?? '')}</div>;
    }

    if (ctx.column.name === 'last_login') {
      return (
        <div className="text-sm text-norse-fog py-1">
          {row.last_login ? new Date(row.last_login).toLocaleString() : 'Never'}
        </div>
      );
    }

    if (ctx.column.name === 'email') {
      return (
        <div className="text-norse-silver py-1">{String(ctx.value || '—')}</div>
      );
    }

    return null;
  };

  const userCellRenderers = useMemo(() => ({
    username: renderUserCell,
    roles: renderUserCell,
    status: renderUserCell,
    last_login: renderUserCell,
    actions: renderUserCell,
  }), [renderUserCell]);
  if (!isAdmin) {
    return null;
  }

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
        title="User Management"
        backTo="/security"
        backLabel="Back to Security"
        actions={
          <Button
            variant={showCreateForm ? "secondary" : "primary"}
            onClick={() => setShowCreateForm(!showCreateForm)}
            icon={showCreateForm ? undefined : Plus}
          >
            {showCreateForm ? 'Cancel' : 'Create User'}
          </Button>
        }
      />

      {/* Main Content */}
      <main className="max-w-6xl mx-auto p-6">
        {error && <Alert type="error" message={error} className="mb-6" dismissible onDismiss={() => setError('')} />}

        {/* Create User Form */}
        {showCreateForm && (
          <div className="bg-norse-shadow border border-norse-rune rounded-lg p-6 mb-6">
            <h2 className="text-lg font-semibold text-white mb-4">Create New User</h2>
            <form onSubmit={handleCreateUser} className="space-y-4">
              <FormInput
                id={createUsernameId}
                label="Username"
                value={newUsername}
                onChange={setNewUsername}
                required
              />

              <FormInput
                id={createPasswordId}
                type="password"
                label="Password"
                value={newPassword}
                onChange={setNewPassword}
                required
              />
              <p className="text-xs text-norse-fog -mt-2">Minimum 8 characters</p>

              <FormInput
                id={createEmailId}
                type="email"
                label="Email (optional)"
                value={newEmail}
                onChange={setNewEmail}
              />

              <fieldset>
                <legend id={createRolesId} className="text-sm text-norse-silver mb-2">Roles *</legend>
                <div className="flex gap-4 flex-wrap">
                  {availableRoles.map(role => (
                    <label key={role} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={newRoles.includes(role)}
                        onChange={() => toggleRole(role, newRoles, setNewRoles)}
                        className="w-4 h-4 rounded border-norse-rune bg-norse-stone text-nornic-primary focus:ring-nornic-primary"
                        aria-describedby={createRolesId}
                      />
                      <span className="text-sm capitalize text-norse-silver">{role}</span>
                    </label>
                  ))}
                </div>
              </fieldset>

              {createError && <Alert type="error" message={createError} />}

              <Button
                type="submit"
                disabled={creating || newRoles.length === 0}
                loading={creating}
                variant="success"
              >
                Create User
              </Button>
            </form>
          </div>
        )}

        {/* Users Table */}
        <div className="bg-norse-shadow border border-norse-rune rounded-lg overflow-hidden">
          <div className="nornic-grid p-4">
            <UiGrid
              options={userGridOptions}
              onRegisterApi={setUsersGridApi}
              cellRenderers={userCellRenderers}
            />
          </div>
        </div>
      </main>
    </PageLayout>
  );
}
