import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

// Base path from environment variable (set at build time)
const BASE_PATH = import.meta.env.VITE_BASE_PATH || '';

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
  
  // Create user state
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newUsername, setNewUsername] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [newEmail, setNewEmail] = useState('');
  const [newRoles, setNewRoles] = useState<string[]>(['viewer']);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState('');
  
  // Edit user state
  const [editingUser, setEditingUser] = useState<User | null>(null);
  const [editRoles, setEditRoles] = useState<string[]>([]);
  const [editDisabled, setEditDisabled] = useState(false);
  const [updating, setUpdating] = useState(false);
  const [updateError, setUpdateError] = useState('');

  useEffect(() => {
    // Check if user is admin
    fetch(`${BASE_PATH}/auth/me`, {
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
      })
      .catch(() => {
        navigate('/security');
      });
  }, [navigate]);

  const loadUsers = async () => {
    try {
      const response = await fetch(`${BASE_PATH}/auth/users`, {
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
  };

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setCreateError('');
    setCreating(true);

    try {
      const response = await fetch(`${BASE_PATH}/auth/users`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          username: newUsername,
          password: newPassword,
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

  const handleEditUser = (user: User) => {
    setEditingUser(user);
    setEditRoles([...user.roles]);
    setEditDisabled(user.disabled || false);
    setUpdateError('');
  };

  const handleUpdateUser = async () => {
    if (!editingUser) return;
    
    setUpdateError('');
    setUpdating(true);

    try {
      const response = await fetch(`${BASE_PATH}/auth/users/${editingUser.username}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          roles: editRoles,
          disabled: editDisabled,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to update user');
      }

      setEditingUser(null);
      await loadUsers();
    } catch (err) {
      setUpdateError(err instanceof Error ? err.message : 'Failed to update user');
    } finally {
      setUpdating(false);
    }
  };

  const handleDeleteUser = async (username: string) => {
    if (!confirm(`Are you sure you want to delete user "${username}"? This action cannot be undone.`)) {
      return;
    }

    try {
      const response = await fetch(`${BASE_PATH}/auth/users/${username}`, {
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

  if (!isAdmin) {
    return null;
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="text-slate-400">Loading...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-slate-900 text-white">
      {/* Header */}
      <header className="bg-slate-800 border-b border-slate-700 p-4">
        <div className="max-w-6xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={() => navigate('/security')}
              className="text-slate-400 hover:text-white transition-colors"
            >
              ← Back to Security
            </button>
            <h1 className="text-xl font-semibold">User Management</h1>
          </div>
          <button
            type="button"
            onClick={() => setShowCreateForm(!showCreateForm)}
            className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors"
          >
            {showCreateForm ? 'Cancel' : '+ Create User'}
          </button>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto p-6">
        {error && (
          <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm mb-6">
            {error}
          </div>
        )}

        {/* Create User Form */}
        {showCreateForm && (
          <div className="bg-slate-800 rounded-lg p-6 mb-6">
            <h2 className="text-lg font-semibold mb-4">Create New User</h2>
            <form onSubmit={handleCreateUser} className="space-y-4">
              <div>
                <label htmlFor="username" className="block text-sm text-slate-400 mb-1">
                  Username *
                </label>
                <input
                  id="username"
                  type="text"
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.target.value)}
                  required
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <div>
                <label htmlFor="password" className="block text-sm text-slate-400 mb-1">
                  Password *
                </label>
                <input
                  id="password"
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  required
                  minLength={8}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
                <p className="text-xs text-slate-500 mt-1">Minimum 8 characters</p>
              </div>

              <div>
                <label htmlFor="email" className="block text-sm text-slate-400 mb-1">
                  Email (optional)
                </label>
                <input
                  id="email"
                  type="email"
                  value={newEmail}
                  onChange={(e) => setNewEmail(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <div>
                <label className="block text-sm text-slate-400 mb-2">Roles *</label>
                <div className="flex gap-4">
                  {['admin', 'editor', 'viewer'].map(role => (
                    <label key={role} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={newRoles.includes(role)}
                        onChange={() => toggleRole(role, newRoles, setNewRoles)}
                        className="w-4 h-4 rounded border-slate-600 bg-slate-700 text-blue-600 focus:ring-blue-500"
                      />
                      <span className="text-sm capitalize">{role}</span>
                    </label>
                  ))}
                </div>
              </div>

              {createError && (
                <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm">
                  {createError}
                </div>
              )}

              <button
                type="submit"
                disabled={creating || newRoles.length === 0}
                className="px-4 py-2 bg-green-600 text-white rounded font-medium hover:bg-green-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {creating ? 'Creating...' : 'Create User'}
              </button>
            </form>
          </div>
        )}

        {/* Users Table */}
        <div className="bg-slate-800 rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-slate-700">
                <tr>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Username</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Email</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Roles</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Status</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Last Login</th>
                  <th className="px-4 py-3 text-left text-sm font-semibold">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700">
                {users.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-slate-400">
                      No users found
                    </td>
                  </tr>
                ) : (
                  users.map((user) => (
                    <tr key={user.username} className="hover:bg-slate-750">
                      <td className="px-4 py-3">
                        <div className="font-medium">{user.username}</div>
                      </td>
                      <td className="px-4 py-3 text-slate-300">
                        {user.email || <span className="text-slate-500">—</span>}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex gap-1 flex-wrap">
                          {user.roles.map(role => (
                            <span
                              key={role}
                              className={`px-2 py-1 rounded text-xs ${
                                role === 'admin'
                                  ? 'bg-red-900/30 text-red-300'
                                  : role === 'editor'
                                  ? 'bg-blue-900/30 text-blue-300'
                                  : 'bg-slate-700 text-slate-300'
                              }`}
                            >
                              {role}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        {user.disabled ? (
                          <span className="px-2 py-1 rounded text-xs bg-red-900/30 text-red-300">
                            Disabled
                          </span>
                        ) : (
                          <span className="px-2 py-1 rounded text-xs bg-green-900/30 text-green-300">
                            Active
                          </span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-slate-400 text-sm">
                        {user.last_login
                          ? new Date(user.last_login).toLocaleString()
                          : <span className="text-slate-500">Never</span>}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex gap-2">
                          <button
                            type="button"
                            onClick={() => handleEditUser(user)}
                            className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 transition-colors"
                          >
                            Edit
                          </button>
                          <button
                            type="button"
                            onClick={() => handleDeleteUser(user.username)}
                            className="px-3 py-1 bg-red-600 text-white rounded text-sm hover:bg-red-700 transition-colors"
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Edit User Modal */}
        {editingUser && (
          <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
            <div className="bg-slate-800 rounded-lg p-6 max-w-md w-full">
              <h2 className="text-lg font-semibold mb-4">Edit User: {editingUser.username}</h2>
              
              <div className="space-y-4">
                <div>
                  <label className="block text-sm text-slate-400 mb-2">Roles</label>
                  <div className="flex gap-4">
                    {['admin', 'editor', 'viewer'].map(role => (
                      <label key={role} className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={editRoles.includes(role)}
                          onChange={() => toggleRole(role, editRoles, setEditRoles)}
                          className="w-4 h-4 rounded border-slate-600 bg-slate-700 text-blue-600 focus:ring-blue-500"
                        />
                        <span className="text-sm capitalize">{role}</span>
                      </label>
                    ))}
                  </div>
                </div>

                <div>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={editDisabled}
                      onChange={(e) => setEditDisabled(e.target.checked)}
                      className="w-4 h-4 rounded border-slate-600 bg-slate-700 text-blue-600 focus:ring-blue-500"
                    />
                    <span className="text-sm">Disabled</span>
                  </label>
                </div>

                {updateError && (
                  <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm">
                    {updateError}
                  </div>
                )}

                <div className="flex gap-2 justify-end">
                  <button
                    type="button"
                    onClick={() => setEditingUser(null)}
                    className="px-4 py-2 bg-slate-700 text-white rounded hover:bg-slate-600 transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={handleUpdateUser}
                    disabled={updating || editRoles.length === 0}
                    className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {updating ? 'Updating...' : 'Update'}
                  </button>
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

