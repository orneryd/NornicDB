import { useEffect, useState } from 'react';
import { Navigate } from 'react-router-dom';
import { api } from '../utils/api';

interface ProtectedRouteProps {
  children: React.ReactNode;
}

export function ProtectedRoute({ children }: ProtectedRouteProps) {
  const [authState, setAuthState] = useState<'loading' | 'authenticated' | 'unauthenticated'>('loading');

  useEffect(() => {
    // First check if auth is even enabled
    api.getAuthConfig().then(config => {
      if (!config.securityEnabled) {
        // Auth disabled - allow access
        setAuthState('authenticated');
        return;
      }
      
      // Auth enabled - check if authenticated
      api.checkAuth().then(result => {
        setAuthState(result.authenticated ? 'authenticated' : 'unauthenticated');
      });
    });
  }, []);

  if (authState === 'loading') {
    return (
      <div className="min-h-screen flex items-center justify-center bg-norse-night">
        <div className="flex flex-col items-center gap-4">
          <div className="w-12 h-12 border-4 border-nornic-primary border-t-transparent rounded-full animate-spin" />
          <span className="text-norse-silver">Connecting to NornicDB...</span>
        </div>
      </div>
    );
  }

  if (authState === 'unauthenticated') {
    return <Navigate to="/login" replace />;
  }

  return <>{children}</>;
}
