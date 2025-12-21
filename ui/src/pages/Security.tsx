import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

// Base path from environment variable (set at build time)
const BASE_PATH = import.meta.env.VITE_BASE_PATH || '';

interface GeneratedToken {
  token: string;
  subject: string;
  expires_at?: string;
  expires_in?: number;
  roles: string[];
}

interface UserInfo {
  id: string;
  username: string;
  email?: string;
  roles: string[];
  auth_method?: string;
  oauth_provider?: string;
}

export function Security() {
  const navigate = useNavigate();
  const [subject, setSubject] = useState('');
  const [expiresIn, setExpiresIn] = useState('30d');
  const [customExpiry, setCustomExpiry] = useState('');
  const [generatedToken, setGeneratedToken] = useState<GeneratedToken | null>(null);
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [isAdmin, setIsAdmin] = useState(false);
  const [checkingAuth, setCheckingAuth] = useState(true);
  const [userInfo, setUserInfo] = useState<UserInfo | null>(null);
  
  // Password change state
  const [oldPassword, setOldPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [passwordError, setPasswordError] = useState('');
  const [passwordSuccess, setPasswordSuccess] = useState(false);
  const [changingPassword, setChangingPassword] = useState(false);
  
  // Profile update state
  const [email, setEmail] = useState('');
  const [profileError, setProfileError] = useState('');
  const [profileSuccess, setProfileSuccess] = useState(false);
  const [updatingProfile, setUpdatingProfile] = useState(false);

  useEffect(() => {
    // Check if user is admin and get user info
    fetch(`${BASE_PATH}/auth/me`, {
      credentials: 'include'
    })
      .then(res => res.json())
      .then(data => {
        const roles = data.roles || [];
        setIsAdmin(roles.includes('admin'));
        setUserInfo(data);
        setEmail(data.email || '');
        setCheckingAuth(false);
      })
      .catch(() => {
        setCheckingAuth(false);
        navigate('/login');
      });
  }, [navigate]);

  const handleGenerate = async () => {
    setError('');
    setIsLoading(true);
    setGeneratedToken(null);

    const expiry = expiresIn === 'custom' ? customExpiry : expiresIn;

    try {
      const response = await fetch(`${BASE_PATH}/auth/api-token`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        credentials: 'include',
        body: JSON.stringify({
          subject: subject || 'api-token',
          expires_in: expiry === 'never' ? '0' : expiry,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.message || 'Failed to generate token');
      }

      const data = await response.json();
      setGeneratedToken(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to generate token');
    } finally {
      setIsLoading(false);
    }
  };

  const copyToClipboard = () => {
    if (generatedToken) {
      navigator.clipboard.writeText(generatedToken.token);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (checkingAuth) {
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
        <div className="max-w-4xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={() => navigate('/')}
              className="text-slate-400 hover:text-white transition-colors"
            >
              ← Back
            </button>
            <h1 className="text-xl font-semibold">Security & API Tokens</h1>
          </div>
          {isAdmin && (
            <button
              type="button"
              onClick={() => navigate('/security/admin')}
              className="px-4 py-2 bg-purple-600 text-white rounded hover:bg-purple-700 transition-colors text-sm"
            >
              👥 Admin Panel
            </button>
          )}
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-4xl mx-auto p-6">
        {/* Authentication Info */}
        {userInfo && (
          <div className="bg-slate-800 rounded-lg p-4 mb-6">
            <h2 className="text-sm font-semibold text-slate-400 mb-2">Authentication Method</h2>
            <div className="flex items-center gap-2">
              {userInfo.auth_method === 'oauth' ? (
                <>
                  <span className="text-green-400">🔐 OAuth</span>
                  {userInfo.oauth_provider && (
                    <span className="text-slate-500 text-sm">({userInfo.oauth_provider})</span>
                  )}
                  <span className="text-slate-500 text-sm ml-auto">
                    Your account is managed by the OAuth provider. You can generate NornicDB API tokens below for programmatic access.
                  </span>
                </>
              ) : (
                <>
                  <span className="text-blue-400">🔑 Password</span>
                  <span className="text-slate-500 text-sm ml-auto">
                    Your account uses password authentication. You can generate API tokens for programmatic access.
                  </span>
                </>
              )}
            </div>
          </div>
        )}

        {/* Profile Update Section */}
        {userInfo && userInfo.auth_method !== 'oauth' && (
          <div className="bg-slate-800 rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold mb-4">Profile Settings</h2>
            
            <div className="space-y-4">
              <div>
                <label htmlFor="email" className="block text-sm text-slate-400 mb-1">
                  Email Address
                </label>
                <input
                  id="email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="your.email@example.com"
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <button
                type="button"
                onClick={async () => {
                  setProfileError('');
                  setProfileSuccess(false);
                  setUpdatingProfile(true);
                  
                  try {
                    const response = await fetch(`${BASE_PATH}/auth/profile`, {
                      method: 'PUT',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                      credentials: 'include',
                      body: JSON.stringify({ email }),
                    });

                    if (!response.ok) {
                      const data = await response.json();
                      throw new Error(data.message || 'Failed to update profile');
                    }

                    setProfileSuccess(true);
                    setTimeout(() => setProfileSuccess(false), 3000);
                    
                    // Refresh user info
                    const meResponse = await fetch(`${BASE_PATH}/auth/me`, {
                      credentials: 'include'
                    });
                    const meData = await meResponse.json();
                    setUserInfo(meData);
                    setEmail(meData.email || '');
                  } catch (err) {
                    setProfileError(err instanceof Error ? err.message : 'Failed to update profile');
                  } finally {
                    setUpdatingProfile(false);
                  }
                }}
                disabled={updatingProfile}
                className="px-4 py-2 bg-blue-600 text-white rounded font-medium hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {updatingProfile ? 'Updating...' : 'Update Profile'}
              </button>

              {profileError && (
                <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm">
                  {profileError}
                </div>
              )}

              {profileSuccess && (
                <div className="bg-green-900/30 border border-green-700/50 rounded p-3 text-green-300 text-sm">
                  Profile updated successfully!
                </div>
              )}
            </div>
          </div>
        )}

        {/* Password Change Section */}
        {userInfo && userInfo.auth_method !== 'oauth' && (
          <div className="bg-slate-800 rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold mb-4">Change Password</h2>
            
            <div className="space-y-4">
              <div>
                <label htmlFor="old-password" className="block text-sm text-slate-400 mb-1">
                  Current Password
                </label>
                <input
                  id="old-password"
                  type="password"
                  value={oldPassword}
                  onChange={(e) => setOldPassword(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <div>
                <label htmlFor="new-password" className="block text-sm text-slate-400 mb-1">
                  New Password
                </label>
                <input
                  id="new-password"
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <div>
                <label htmlFor="confirm-password" className="block text-sm text-slate-400 mb-1">
                  Confirm New Password
                </label>
                <input
                  id="confirm-password"
                  type="password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              </div>

              <button
                type="button"
                onClick={async () => {
                  setPasswordError('');
                  setPasswordSuccess(false);
                  
                  if (newPassword !== confirmPassword) {
                    setPasswordError('New passwords do not match');
                    return;
                  }
                  
                  if (newPassword.length < 8) {
                    setPasswordError('New password must be at least 8 characters');
                    return;
                  }
                  
                  setChangingPassword(true);
                  
                  try {
                    const response = await fetch(`${BASE_PATH}/auth/password`, {
                      method: 'POST',
                      headers: {
                        'Content-Type': 'application/json',
                      },
                      credentials: 'include',
                      body: JSON.stringify({
                        old_password: oldPassword,
                        new_password: newPassword,
                      }),
                    });

                    if (!response.ok) {
                      const data = await response.json();
                      throw new Error(data.message || 'Failed to change password');
                    }

                    setPasswordSuccess(true);
                    setOldPassword('');
                    setNewPassword('');
                    setConfirmPassword('');
                    setTimeout(() => setPasswordSuccess(false), 3000);
                  } catch (err) {
                    setPasswordError(err instanceof Error ? err.message : 'Failed to change password');
                  } finally {
                    setChangingPassword(false);
                  }
                }}
                disabled={changingPassword || !oldPassword || !newPassword || !confirmPassword}
                className="w-full py-2 bg-blue-600 text-white rounded font-medium hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {changingPassword ? 'Changing...' : 'Change Password'}
              </button>

              {passwordError && (
                <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm">
                  {passwordError}
                </div>
              )}

              {passwordSuccess && (
                <div className="bg-green-900/30 border border-green-700/50 rounded p-3 text-green-300 text-sm">
                  Password changed successfully!
                </div>
              )}
            </div>
          </div>
        )}

        {/* Info Banner */}
        <div className="bg-blue-900/30 border border-blue-700/50 rounded-lg p-4 mb-8">
          <div className="flex gap-3">
            <span className="text-blue-400 text-2xl">ℹ️</span>
            <div>
              <h3 className="font-semibold text-blue-300 mb-1">About API Tokens</h3>
              <p className="text-slate-300 text-sm">
                API tokens are stateless JWT tokens that can be used for MCP server configurations
                and other API integrations. These tokens inherit your current roles and permissions.
                <strong className="text-blue-300"> Tokens are not stored</strong> — once generated,
                save them securely as they cannot be retrieved later.
              </p>
            </div>
          </div>
        </div>

        {/* Token Generator - Admin Only */}
        {isAdmin && (
          <div className="bg-slate-800 rounded-lg p-6 mb-8">
            <h2 className="text-lg font-semibold mb-4">Generate API Token</h2>
          
          <div className="space-y-4">
            {/* Subject/Label */}
            <div>
              <label htmlFor="token-subject" className="block text-sm text-slate-400 mb-1">
                Token Label (Subject)
              </label>
              <input
                id="token-subject"
                type="text"
                value={subject}
                onChange={(e) => setSubject(e.target.value)}
                placeholder="e.g., my-mcp-server, prod-api, cursor-agent"
                className="w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
              />
              <p className="text-xs text-slate-500 mt-1">
                A descriptive label to help you identify this token later
              </p>
            </div>

            {/* Expiration */}
            <div>
              <span className="block text-sm text-slate-400 mb-1">
                Token Expiration
              </span>
              <div className="flex gap-2 flex-wrap">
                {['1h', '24h', '7d', '30d', '90d', '365d', 'never', 'custom'].map((option) => (
                  <button
                    type="button"
                    key={option}
                    onClick={() => setExpiresIn(option)}
                    className={`px-3 py-1.5 rounded text-sm transition-colors ${
                      expiresIn === option
                        ? 'bg-blue-600 text-white'
                        : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
                    }`}
                  >
                    {option === 'never' ? 'Never' : option === 'custom' ? 'Custom' : option}
                  </button>
                ))}
              </div>
              {expiresIn === 'custom' && (
                <input
                  type="text"
                  value={customExpiry}
                  onChange={(e) => setCustomExpiry(e.target.value)}
                  placeholder="e.g., 48h, 14d, 6mo"
                  className="mt-2 w-full bg-slate-700 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:outline-none focus:border-blue-500"
                />
              )}
            </div>

            {/* Generate Button */}
            <button
              type="button"
              onClick={handleGenerate}
              disabled={isLoading}
              className="w-full py-2 bg-green-600 text-white rounded font-medium hover:bg-green-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? 'Generating...' : 'Generate Token'}
            </button>

            {error && (
              <div className="bg-red-900/30 border border-red-700/50 rounded p-3 text-red-300 text-sm">
                {error}
              </div>
            )}
          </div>
          </div>
        )}

        {/* Generated Token Display */}
        {generatedToken && (
          <div className="bg-slate-800 rounded-lg p-6 mb-8">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-green-400">✓ Token Generated</h2>
              <span className="text-xs text-slate-500">
                {generatedToken.expires_at 
                  ? `Expires: ${new Date(generatedToken.expires_at).toLocaleString()}`
                  : 'Never expires'}
              </span>
            </div>

            <div className="bg-slate-900 rounded p-4 mb-4">
              <div className="flex items-start justify-between gap-4">
                <code className="text-sm text-green-300 break-all flex-1 font-mono">
                  {generatedToken.token}
                </code>
                <button
                  type="button"
                  onClick={copyToClipboard}
                  className={`px-3 py-1 rounded text-sm transition-colors flex-shrink-0 ${
                    copied
                      ? 'bg-green-600 text-white'
                      : 'bg-slate-700 text-slate-300 hover:bg-slate-600'
                  }`}
                >
                  {copied ? '✓ Copied!' : 'Copy'}
                </button>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-slate-500">Subject:</span>
                <span className="text-white ml-2">{generatedToken.subject}</span>
              </div>
              <div>
                <span className="text-slate-500">Roles:</span>
                <span className="text-white ml-2">{generatedToken.roles.join(', ')}</span>
              </div>
            </div>

            {/* Usage Example */}
            <div className="mt-4 pt-4 border-t border-slate-700">
              <h3 className="text-sm font-semibold text-slate-400 mb-2">Usage Example (Claude Desktop / MCP Config)</h3>
              <pre className="bg-slate-900 rounded p-3 text-xs overflow-x-auto">
                <code className="text-slate-300">{`{
  "mcpServers": {
    "nornicdb": {
      "url": "http://127.0.0.1:7474/mcp",
      "name": "Knowledge Graph TODO MCP Server",
      "description": "MCP server for TODO tracking with Graph-RAG memory system",
      "headers": {
        "Authorization": "Bearer ${generatedToken.token.substring(0, 40)}..."
      }
    }
  }
}`}</code>
              </pre>
              <p className="text-xs text-slate-500 mt-2">
                For Claude Desktop: Add this to your <code className="text-slate-400">~/Library/Application Support/Claude/claude_desktop_config.json</code>
              </p>
            </div>
          </div>
        )}

        {/* Security Tips */}
        <div className="bg-slate-800 rounded-lg p-6">
          <h2 className="text-lg font-semibold mb-4">🔐 Security Best Practices</h2>
          <ul className="space-y-2 text-sm text-slate-300">
            <li className="flex gap-2">
              <span className="text-yellow-400">•</span>
              <span>Use descriptive labels to track which token is used where</span>
            </li>
            <li className="flex gap-2">
              <span className="text-yellow-400">•</span>
              <span>Set appropriate expiration times — shorter is more secure</span>
            </li>
            <li className="flex gap-2">
              <span className="text-yellow-400">•</span>
              <span>Store tokens securely (environment variables, secrets managers)</span>
            </li>
            <li className="flex gap-2">
              <span className="text-yellow-400">•</span>
              <span>Never commit tokens to version control</span>
            </li>
            <li className="flex gap-2">
              <span className="text-yellow-400">•</span>
              <span>Rotate tokens periodically, especially for long-lived integrations</span>
            </li>
          </ul>
        </div>
      </main>
    </div>
  );
}
