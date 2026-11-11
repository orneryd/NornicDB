import { useState, useEffect, useCallback, useId, useMemo, useRef, createContext, useContext } from 'react';
import { useNavigate, useLocation, useParams } from 'react-router-dom';
import { StatefulGrid as UiGrid } from "../components/common/StatefulGrid";
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

type RowSaveStatus={state:'saving'|'saved'|'error';message?:string;changes:Partial<User>};
const UserGridContext=createContext<{
 availableRoles:string[];rowStatus:Record<string,RowSaveStatus>;editingUsername?:string;usersGridApi:UiGridApi|null;
 handleUpdateUser:(user:User,changes:Partial<User>)=>Promise<void>;handleDeleteUser:(username:string)=>Promise<void>;
 discardChanges:(username:string)=>void;finishEditing:()=>void;
}|null>(null);
function UserCell({ctx}:{ctx:GridCellTemplateContext}) {
    const value=useContext(UserGridContext);
    if(!value)throw Error('User cell requires its grid context');
    const {availableRoles,rowStatus,editingUsername,usersGridApi,handleUpdateUser,handleDeleteUser,discardChanges,finishEditing}=value;
    const row = ctx.row as GridRecord & User;
    const save = rowStatus[row.username];
    const saveIndicator=<div role={save?.state === 'error' ? 'alert' : 'status'} className={save?.state === 'error' ? 'text-red-400 text-xs' : 'text-norse-silver text-xs'}>
      {save?.state === 'saving' ? 'Saving…' : save?.state === 'saved' ? 'Saved' : save?.state === 'error' ? <>{save.message} <button type="button" onClick={()=>void handleUpdateUser(row,save.changes).catch(()=>{})} className="underline">Retry saving {row.username}</button><button type="button" onClick={()=>discardChanges(row.username)} className="block underline">Discard changes for {row.username}</button></> : null}
    </div>;

    if (ctx.column.name === 'roles') {
      if(editingUsername!==row.username) return <div className="flex flex-wrap gap-1 py-1">{row.roles.map(role=><span key={role} className="rounded bg-norse-rune px-2 py-1 text-xs text-norse-silver">{role}</span>)}</div>;
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
                  disabled={disableRemoval || save?.state === 'saving'}
                  aria-label={`${role} role for ${row.username}`}
                  onChange={() => {
                    const nextRoles = checked
                      ? row.roles.filter((entry) => entry !== role)
                      : [...row.roles, role];
                    if (nextRoles.length > 0) {
                      void handleUpdateUser(row, { roles: nextRoles }).catch(()=>{});
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
            aria-label={`${row.disabled ? 'Enable' : 'Disable'} ${row.username}`}
            disabled={save?.state === 'saving'}
            onClick={() => void handleUpdateUser(row, { disabled: !row.disabled }).catch(()=>{})}
            className={`px-2 py-1 rounded text-xs ${row.disabled ? 'bg-red-500/20 text-red-400' : 'bg-green-500/20 text-green-400'}`}
          >
            {row.disabled ? 'Disabled' : 'Active'}
          </button>
        </div>
      );
    }

    if (ctx.column.name === 'actions') {
      return (
        <div className="flex flex-col gap-1 py-1" onClick={(e) => e.stopPropagation()}><div className="flex gap-2">
          <Button
            variant="secondary"
            size="sm"
            disabled={save?.state==='saving'}
            onClick={() => editingUsername===row.username?finishEditing():usersGridApi?.edit.beginCellEdit(String(row.__gridId), 'email')}
            icon={Edit}
          >
            {editingUsername===row.username?'Done':'Edit'}
          </Button>
          <Button
            variant="danger"
            size="sm"
            onClick={() => handleDeleteUser(row.username)}
            icon={Trash2}
          >
            Delete
          </Button>
          </div>{saveIndicator}</div>
      );
    }

    if (ctx.column.name === 'username') {
      return <div className={`font-medium py-1 ${save?.state==='error'?'text-red-300 border-l-2 border-red-400 pl-2':'text-white'}`}>{String(ctx.value ?? '')}{save?.state==='error' && <span aria-label={`Unsaved changes for ${row.username}`} className="block text-xs font-normal">Unsaved changes</span>}</div>;
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
  }

const renderUserCell=(ctx:GridCellTemplateContext)=><UserCell ctx={ctx}/>;
const userCellRenderers={username:renderUserCell,roles:renderUserCell,status:renderUserCell,last_login:renderUserCell,actions:renderUserCell};

export function AdminUsers() {
  const navigate = useNavigate();
  const location = useLocation();
  const route = useParams();
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
  const pendingChanges=useRef(new Map<string,Partial<User>>());

  // Create user state
  const showCreateForm = location.pathname.replace(/\/$/,'').endsWith('/new');
  const setShowCreateForm = (open:boolean) => navigate({pathname:open ? '/security/admin/new' : '/security/admin',search:location.search});
  const [rowStatus, setRowStatus] = useState<Record<string, {state: 'saving'|'saved'|'error'; message?:string; changes:Partial<User>}>>({});
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

  const persistUserUpdate = useCallback(async (user: User, overrides: Partial<User>) => {
    setError('');
    setRowStatus(previous=>({...previous,[user.username]:{state:'saving',changes:overrides}}));
    try {
      const response = await fetch(joinBasePath(BASE_PATH, `/auth/users/${encodeURIComponent(user.username)}`), {
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

      setUsers(previous=>previous.map(entry=>entry.username===user.username ? {...entry,...overrides} : entry));
      setRowStatus(previous=>({...previous,[user.username]:{state:'saved',changes:overrides}}));
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to update user';
      setRowStatus(previous=>({...previous,[user.username]:{state:'error',message,changes:overrides}}));
      throw err;
    }
  }, [loadUsers]);
  const handleUpdateUser=useCallback((user:User,changes:Partial<User>)=>{
    const row=usersGridApi?.core.getVisibleRows().find(entry=>entry.entity.username===user.username);
    if(!row)return Promise.reject(Error('This user is not available in the current grid.'));
    pendingChanges.current.set(user.username,{...pendingChanges.current.get(user.username),...changes});
    usersGridApi?.rowEdit.setRowsDirty([row.entity]);
    return Promise.resolve();
  },[usersGridApi]);
  const discardChanges=useCallback((username:string)=>{
    pendingChanges.current.delete(username);
    setRowStatus(previous=>{const next={...previous};delete next[username];return next});
  },[]);
  useEffect(()=>{
    if(!usersGridApi)return;
    return usersGridApi.rowEdit.on.saveRow(row=>{
      const user=row as GridRecord & User;
      const changes=pendingChanges.current.get(user.username);
      if(!changes)return;
      const promise=persistUserUpdate(user,changes);
      usersGridApi.rowEdit.setSavePromise(row,promise);
      void promise.then(()=>pendingChanges.current.delete(user.username)).catch(()=>{});
    });
  },[usersGridApi,persistUserUpdate]);

  useEffect(()=>{
    if(!usersGridApi || loading || !route.username)return;
    const user=users.find(entry=>entry.username===route.username);
    if(!user){setError(`User ${route.username} was not found.`);return;}
    usersGridApi.edit.beginCellEdit(route.username,'email');
  },[usersGridApi,loading,route.username]);

  useEffect(()=>{
    if(!usersGridApi)return;
    const begin=usersGridApi.edit.on.beginCellEdit(row=>navigate({pathname:`/security/admin/${encodeURIComponent(String(row.username))}/edit`,search:location.search}));
    const cancel=usersGridApi.edit.on.cancelCellEdit(()=>navigate({pathname:'/security/admin',search:location.search}));
    return ()=>{begin();cancel()};
  },[usersGridApi,navigate,location.search]);

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
    ...rowStatus[user.username]?.changes,
    __saving:rowStatus[user.username]?.state==='saving',
    __gridId: user.username,
  })), [users,rowStatus]);

  const userColumns = useMemo<GridColumnDef[]>(() => [
    {
      name: 'username',
      displayName: 'Username',
      field: 'username',
      pinnedLeft: true,
      enableCellEdit: false,
      width: '180px',
    },
    {
      name: 'email',
      displayName: 'Email',
      field: 'email',
      enableCellEdit: true,
      cellEditableCondition:({row})=>!row.__saving,
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
      pinnedRight: true,
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
    enableCellEditOnFocus: false,
    rowEditWaitInterval: 1,
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

      const save = handleUpdateUser(user, { email: nextEmail });
      void save.catch(()=>{});
    });
  }, [handleUpdateUser, users, usersGridApi]);

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
            <UserGridContext.Provider value={{availableRoles,rowStatus,editingUsername:route.username,usersGridApi,handleUpdateUser,handleDeleteUser,discardChanges,finishEditing:()=>{usersGridApi?.edit.endCellEdit();navigate({pathname:'/security/admin',search:location.search})}}}>
            <UiGrid height={Math.min(560,Math.max(220,usersGridData.length*72+140))}
              options={userGridOptions}
              onRegisterApi={setUsersGridApi}
              cellRenderers={userCellRenderers}
            />
            </UserGridContext.Provider>
          </div>
        </div>
      </main>
    </PageLayout>
  );
}
