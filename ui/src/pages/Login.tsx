import { useState, useEffect, useId, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { Lock, User } from 'lucide-react';
import { api, type AuthConfig } from '../utils/api';
import { PageLayout } from '../components/common/PageLayout';
import { FormInput } from '../components/common/FormInput';
import { Button } from '../components/common/Button';
import { Alert } from '../components/common/Alert';

export function Login() {
  const navigate = useNavigate();
  const usernameId = useId();
  const passwordId = useId();
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
      <PageLayout showHeader={false}>
        <div className="flex items-center justify-center flex-1">
          <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
        </div>
      </PageLayout>
    );
  }

  return (
    <PageLayout showHeader={false}>
      <div className="flex items-center justify-center flex-1">
        {/* Background pattern */}
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_50%,rgba(16,185,129,0.1),transparent_50%)]" />
        
        <div className="relative max-w-md w-full mx-4">
          <div className="bg-norse-shadow border border-norse-rune rounded-xl p-8 shadow-2xl">
            {/* Logo */}
            <div className="text-center mb-8">
              <div className="inline-flex items-center justify-center mb-4">
                <img src="/nornicdb.svg" alt="NornicDB" className="w-16 h-16" />
              </div>
              <h1 className="text-2xl font-bold text-white">NornicDB</h1>
              <p className="text-norse-silver text-sm mt-1">Graph Database Browser</p>
            </div>

            <div className="space-y-4">
              {authConfig.devLoginEnabled && (
                <form onSubmit={handleSubmit} className="space-y-4">
                  <FormInput
                    id={usernameId}
                    label="Username"
                    value={username}
                    onChange={setUsername}
                    placeholder="admin"
                    required
                    icon={User}
                  />

                  <FormInput
                    id={passwordId}
                    type="password"
                    label="Password"
                    value={password}
                    onChange={setPassword}
                    placeholder="••••••••"
                    required
                    icon={Lock}
                  />

                  {error && <Alert type="error" message={error} />}

                  <Button
                    type="submit"
                    disabled={loading}
                    loading={loading}
                    className="w-full"
                  >
                    Connect
                  </Button>

                  <p className="text-xs text-center text-norse-fog mt-4">
                    Development Mode - Use configured credentials
                  </p>
                </form>
              )}

              {/* OAuth providers */}
              {authConfig.oauthProviders.length > 0 && (
                <>
                  {authConfig.devLoginEnabled && (
                    <div className="relative">
                      <div className="absolute inset-0 flex items-center">
                        <div className="w-full border-t border-norse-rune"></div>
                      </div>
                      <div className="relative flex justify-center text-sm">
                        <span className="px-2 bg-norse-shadow text-norse-silver">or</span>
                      </div>
                    </div>
                  )}
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
                  </div>
                </>
              )}

              {/* Show message if no auth methods available */}
              {!authConfig.devLoginEnabled && authConfig.oauthProviders.length === 0 && (
                <Alert
                  type="info"
                  message="No authentication providers configured"
                />
              )}
            </div>
          </div>
        </div>
      </div>
    </PageLayout>
  );
}
