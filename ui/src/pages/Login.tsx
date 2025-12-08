import { useState, useEffect, FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { Database, Lock, User } from 'lucide-react';
import { api, AuthConfig } from '../utils/api';

export function Login() {
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [authConfig, setAuthConfig] = useState<AuthConfig | null>(null);

  useEffect(() => {
    // Check auth config
    api.getAuthConfig().then(config => {
      if (!config.securityEnabled) {
        // Auth disabled - go straight to browser
        navigate('/', { replace: true });
        return;
      }
      
      // Check if already authenticated
      api.checkAuth().then(result => {
        if (result.authenticated) {
          navigate('/', { replace: true });
        } else {
          setAuthConfig(config);
        }
      });
    });
  }, [navigate]);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    const result = await api.login(username, password);
    
    if (result.success) {
      navigate('/', { replace: true });
    } else {
      setError(result.error || 'Login failed');
    }
    
    setLoading(false);
  };

  if (!authConfig) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-norse-night">
        <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-norse-night">
      {/* Background pattern */}
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_50%,rgba(16,185,129,0.1),transparent_50%)]" />
      
      <div className="relative max-w-md w-full mx-4">
        <div className="bg-norse-shadow border border-norse-rune rounded-xl p-8 shadow-2xl">
          {/* Logo */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center justify-center w-16 h-16 rounded-full bg-gradient-to-br from-nornic-primary to-nornic-secondary mb-4">
              <Database className="w-8 h-8 text-white" />
            </div>
            <h1 className="text-2xl font-bold text-white">NornicDB</h1>
            <p className="text-norse-silver text-sm mt-1">Graph Database Browser</p>
          </div>

          {authConfig.devLoginEnabled ? (
            // Dev mode login form
            <form onSubmit={handleSubmit} className="space-y-4">
              <div>
                <label htmlFor="username" className="block text-sm font-medium text-norse-silver mb-2">
                  Username
                </label>
                <div className="relative">
                  <User className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-norse-fog" />
                  <input
                    id="username"
                    type="text"
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    className="w-full pl-10 pr-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                    placeholder="admin"
                    required
                  />
                </div>
              </div>

              <div>
                <label htmlFor="password" className="block text-sm font-medium text-norse-silver mb-2">
                  Password
                </label>
                <div className="relative">
                  <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-norse-fog" />
                  <input
                    id="password"
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    className="w-full pl-10 pr-4 py-2 bg-norse-stone border border-norse-rune rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent"
                    placeholder="••••••••"
                    required
                  />
                </div>
              </div>

              {error && (
                <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg">
                  <p className="text-sm text-red-400">{error}</p>
                </div>
              )}

              <button
                type="submit"
                disabled={loading}
                className="w-full py-2 px-4 bg-gradient-to-r from-nornic-primary to-nornic-secondary text-white font-medium rounded-lg hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:ring-offset-2 focus:ring-offset-norse-shadow disabled:opacity-50 transition-opacity"
              >
                {loading ? 'Connecting...' : 'Connect'}
              </button>

              <p className="text-xs text-center text-norse-fog mt-4">
                Development Mode - Use configured credentials
              </p>
            </form>
          ) : (
            // OAuth providers
            <div className="space-y-3">
              {authConfig.oauthProviders.map((provider) => (
                <a
                  key={provider.name}
                  href={provider.url}
                  className="flex items-center justify-center gap-2 w-full py-2 px-4 bg-norse-stone border border-norse-rune rounded-lg text-white hover:bg-norse-rune transition-colors"
                >
                  <Lock className="w-4 h-4" />
                  Sign in with {provider.displayName}
                </a>
              ))}
              
              {authConfig.oauthProviders.length === 0 && (
                <p className="text-center text-norse-silver">
                  No authentication providers configured
                </p>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
