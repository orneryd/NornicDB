import { useEffect } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
  useNavigate,
} from "react-router-dom";
import { Login } from "./pages/Login";
import { Browser } from "./pages/Browser";
import { Security } from "./pages/Security";
import { AdminUsers } from "./pages/AdminUsers";
import { DatabaseAccess } from "./pages/DatabaseAccess";
import { Databases } from "./pages/Databases";
import { LifecycleAdmin } from "./pages/LifecycleAdmin";
import { KnowledgePoliciesAdmin } from "./pages/KnowledgePoliciesAdmin";
import { RetentionAdmin } from "./pages/RetentionAdmin";
import { ProtectedRoute } from "./components/ProtectedRoute";
import { BASE_PATH } from "./utils/basePath";

// Base path from environment variable (set at build time)
// Env: VITE_BASE_PATH (same as NORNICDB_BASE_PATH on server)
const basename = BASE_PATH;

function TrailingSlashCanonicalizer() {
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    const { pathname, search, hash } = location;
    if (!pathname || pathname === "/" || pathname.endsWith("/")) {
      return;
    }
    navigate(`${pathname}/${search}${hash}`, { replace: true });
  }, [location, navigate]);

  return null;
}

function App() {
  return (
    <BrowserRouter basename={basename}>
      <TrailingSlashCanonicalizer />
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <Browser />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security"
          element={
            <ProtectedRoute>
              <Security />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security/admin"
          element={
            <ProtectedRoute>
              <AdminUsers />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security/database-access"
          element={
            <ProtectedRoute>
              <DatabaseAccess />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security/lifecycle"
          element={
            <ProtectedRoute>
              <LifecycleAdmin />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security/retention"
          element={
            <ProtectedRoute>
              <RetentionAdmin />
            </ProtectedRoute>
          }
        />
        <Route
          path="/security/knowledge-policies"
          element={
            <ProtectedRoute>
              <KnowledgePoliciesAdmin />
            </ProtectedRoute>
          }
        />
        <Route
          path="/databases"
          element={
            <ProtectedRoute>
              <Databases />
            </ProtectedRoute>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
