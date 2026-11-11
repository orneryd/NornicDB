import { lazy, Suspense, useEffect } from "react";
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
import {Schema} from "./pages/Schema";
import {Backups} from "./pages/Backups";
import { Security } from "./pages/Security";
import { AdminUsers } from "./pages/AdminUsers";
import { DatabaseAccess } from "./pages/DatabaseAccess";
import { Databases } from "./pages/Databases";
import { LifecycleAdmin } from "./pages/LifecycleAdmin";
import { KnowledgePoliciesAdmin } from "./pages/KnowledgePoliciesAdmin";
import { RetentionAdmin } from "./pages/RetentionAdmin";
import {PivotDemo} from './pages/PivotDemo';
import {Demos} from './pages/Demos';
import { ProtectedRoute } from "./components/ProtectedRoute";
import { BASE_PATH } from "./utils/basePath";
import { AppNavigation } from "./components/common/AppNavigation";
import {UrlStateProvider} from './utils/useUrlState';
import {DemoSeedProvider} from './components/common/DemoSeed';

// /demo pulls in three.js + 3d-force-graph; lazy so the main bundle stays lean.
const Demo = lazy(() => import("./pages/Demo").then((m) => ({ default: m.Demo })));
// /graphify: code-graph explorer over the graphify tree (same heavy 3D deps).
const Graphify = lazy(() =>
  import("./pages/Graphify").then((m) => ({ default: m.Graphify })),
);
// /cyber: tick-driven drone-fleet experimentation with NornicDB as the oracle.
// Same heavy 3D deps as /demo, so the same lazy split applies.
const Cyber = lazy(() => import("./pages/Cyber").then((m) => ({ default: m.Cyber })));

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
      <UrlStateProvider>
      <DemoSeedProvider>
      <TrailingSlashCanonicalizer />
      <div className="application-shell">
      <AppNavigation />
      <div id="workspace" className="min-w-0 flex-1" tabIndex={-1}>
      <Suspense fallback={<p role="status" className="p-6 text-norse-silver">Loading demo…</p>}>
      <Routes>
        <Route path="/demos" element={<Demos />} />
        <Route path="/demos/pivot" element={<ProtectedRoute><PivotDemo /></ProtectedRoute>} />
        <Route path="/demos/pivot/:demoDatabase" element={<ProtectedRoute><PivotDemo /></ProtectedRoute>} />
        <Route path="/demos/galaxy/:demoDatabase?" element={<Demo />} />
        <Route path="/demos/fleet/:demoDatabase?" element={<Cyber />} />
        <Route path="/login" element={<Login />} />
        <Route
          path="/embeddings"
          element={<ProtectedRoute><Browser maintenance /></ProtectedRoute>}
        />
        <Route path="/schema" element={<ProtectedRoute><Schema/></ProtectedRoute>}/>
        <Route path="/schema/:database" element={<ProtectedRoute><Schema/></ProtectedRoute>}/>
        <Route path="/schema/:database/:schemaKind/:definition" element={<ProtectedRoute><Schema/></ProtectedRoute>}/>
        <Route path="/backups" element={<ProtectedRoute><Backups/></ProtectedRoute>}/>
        <Route path="/workspace/:database" element={<ProtectedRoute><Browser /></ProtectedRoute>} />
        <Route path="/workspace/:database/nodes/:nodeId" element={<ProtectedRoute><Browser /></ProtectedRoute>} />
        <Route path="/workspace/:database/nodes/:nodeId/edit" element={<ProtectedRoute><Browser /></ProtectedRoute>} />
        <Route path="/security/admin/new" element={<ProtectedRoute><AdminUsers /></ProtectedRoute>} />
        <Route path="/security/admin/:username/edit" element={<ProtectedRoute><AdminUsers /></ProtectedRoute>} />
        <Route path="/databases/new" element={<ProtectedRoute><Databases /></ProtectedRoute>} />
        <Route path="/databases/:database" element={<ProtectedRoute><Databases /></ProtectedRoute>} />
        <Route path="/databases/:database/edit" element={<ProtectedRoute><Databases /></ProtectedRoute>} />
        <Route path="/security/retention/policies/new" element={<ProtectedRoute><RetentionAdmin /></ProtectedRoute>} />
        <Route path="/security/retention/policies/:policyId/edit" element={<ProtectedRoute><RetentionAdmin /></ProtectedRoute>} />
        <Route path="/security/retention/holds/new" element={<ProtectedRoute><RetentionAdmin /></ProtectedRoute>} />
        <Route path="/security/retention/erasures/new" element={<ProtectedRoute><RetentionAdmin /></ProtectedRoute>} />
        <Route
          path="/demo"
          element={
            <Suspense
              fallback={
                <div className="min-h-screen flex items-center justify-center bg-norse-night text-norse-silver">
                  Loading galaxy...
                </div>
              }
            >
              <Demo />
            </Suspense>
          }
        />
        <Route
          path="/cyber"
          element={
            <Suspense
              fallback={
                <div className="min-h-screen flex items-center justify-center bg-norse-night text-norse-silver">
                  Loading fleet...
                </div>
              }
            >
              <Cyber />
            </Suspense>
          }
        />
        <Route
          path="/graphify"
          element={
            <Suspense
              fallback={
                <div className="min-h-screen flex items-center justify-center bg-norse-night text-norse-silver">
                  Loading code graph...
                </div>
              }
            >
              <Graphify />
            </Suspense>
          }
        />
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
      </Suspense>
      </div>
      </div>
      </DemoSeedProvider>
      </UrlStateProvider>
    </BrowserRouter>
  );
}

export default App;
