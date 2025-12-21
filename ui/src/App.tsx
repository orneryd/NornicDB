import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Login } from './pages/Login';
import { Browser } from './pages/Browser';
import { Security } from './pages/Security';
import { AdminUsers } from './pages/AdminUsers';
import { ProtectedRoute } from './components/ProtectedRoute';

// Base path from environment variable (set at build time)
// Env: VITE_BASE_PATH (same as NORNICDB_BASE_PATH on server)
const basename = import.meta.env.VITE_BASE_PATH || '';

function App() {
  return (
    <BrowserRouter basename={basename}>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route path="/" element={
          <ProtectedRoute>
            <Browser />
          </ProtectedRoute>
        } />
        <Route path="/security" element={
          <ProtectedRoute>
            <Security />
          </ProtectedRoute>
        } />
        <Route path="/security/admin" element={
          <ProtectedRoute>
            <AdminUsers />
          </ProtectedRoute>
        } />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
