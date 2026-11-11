import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  Fragment,
  type ReactNode,
} from "react";
import {
  Link,
  useNavigate,
  useParams,
  useSearchParams,
} from "react-router-dom";
import { api } from "../../utils/api";
import {
  demoDescriptions,
  demoPlan,
  type DemoKind,
} from "../../utils/demoPlans";
import { PageLayout } from "./PageLayout";
import { PageHeader } from "./PageHeader";
import { Button } from "./Button";
import { Alert } from "./Alert";

type Job = {
  kind: DemoKind;
  database: string;
  phase: "creating" | "seeding" | "ready" | "error";
  label: string;
  done: number;
  total: number;
  error?: string;
  created?: boolean;
  resumable?: boolean;
};
const Context = createContext<{
  jobs: Record<string, Job>;
  active: Job | null;
  start: (kind: DemoKind, database?: string) => void;
} | null>(null);
const markerQuery =
  "MATCH (m:DemoDataset {id: $id}) RETURN m.kind AS kind, m.ready AS ready, m.version AS version";

export function DemoSeedProvider({ children }: { children: ReactNode }) {
  const [jobs, setJobs] = useState<Record<string, Job>>({});
  const busy = useRef(false);
  const navigate = useNavigate();
  const start = (kind: DemoKind, resume?: string) => {
    if (busy.current) return;
    busy.current = true;
    const database =
      resume || `nornic_demo_${kind}_${crypto.randomUUID().slice(0, 8)}`;
    let job: Job = {
      kind,
      database,
      phase: "creating",
      label: resume ? "Resuming setup" : "Creating demo database",
      done: 0,
      total: 0,
    };
    const update = (patch: Partial<Job>) => {
      job = { ...job, ...patch };
      setJobs((previous) => ({ ...previous, [database]: job }));
    };
    update({});
    navigate(`/demos/${kind}/${encodeURIComponent(database)}`);
    void (async () => {
      try {
        if (!resume) await api.createDatabase(database);
        update({ created: true });
        const execute = async (
          query: string,
          parameters?: Record<string, unknown>,
        ) => {
          const response = await api.executeCypherOverHttp(
            database,
            query,
            parameters,
          );
          if (response.errors?.length)
            throw Error(response.errors.map((e) => e.message).join("; "));
        };
        await execute(
          "MERGE (m:DemoDataset {id: $id, embedding_skipped: 'demo dataset'}) SET m.kind=$kind,m.ready=false,m.version=1",
          { id: "dataset", kind },
        );
        update({ resumable: true });
        const plan = demoPlan(kind);
        const total = plan.reduce((n, step) => n + (step.rows?.length ?? 0), 0);
        update({ phase: "seeding", total });
        for (const step of plan) {
          update({ label: step.label });
          if (!step.rows) {
            await execute(step.query);
            continue;
          }
          for (let offset = 0; offset < step.rows.length; offset += 400) {
            const rows = step.rows.slice(offset, offset + 400);
            await execute(step.query, { rows });
            update({ done: job.done + rows.length });
          }
        }
        await execute(
          "MATCH (m:DemoDataset {id: $id}) SET m.ready=true RETURN m.id",
          { id: "dataset" },
        );
        update({ phase: "ready", label: "Demo ready" });
      } catch (error) {
        update({
          phase: "error",
          label: "Setup failed",
          error: error instanceof Error ? error.message : "Unable to seed demo",
        });
      } finally {
        busy.current = false;
      }
    })();
  };
  const active =
    Object.values(jobs).find(
      (job) => job.phase === "creating" || job.phase === "seeding",
    ) ?? null;
  return (
    <Context.Provider value={{ jobs, active, start }}>
      {children}
    </Context.Provider>
  );
}

export function DemoGate({
  kind,
  children,
}: {
  kind: DemoKind;
  children: (database: string) => ReactNode;
}) {
  const context = useContext(Context);
  if (!context) throw Error("Demo requires seed provider");
  const route = useParams();
  const [params] = useSearchParams();
  const database = route.demoDatabase || params.get("database") || "";
  const candidate = context.jobs[database];
  const job = candidate?.kind === kind ? candidate : undefined;
  const [checked, setChecked] = useState<{
    database: string;
    ready: boolean;
    error?: string;
    resumable?: boolean;
  } | null>(null);
  useEffect(() => {
    if (!database || job) return;
    let current = true;
    setChecked(null);
    api
      .executeCypherOverHttp(database, markerQuery, { id: "dataset" })
      .then((response) => {
        const result = response.results?.[0];
        const row = result?.data?.[0]?.row;
        const record = Object.fromEntries(
          (result?.columns ?? []).map((name, index) => [name, row?.[index]]),
        );
        if (current)
          setChecked({
            database,
            ready:
              record.kind === kind &&
              record.ready === true &&
              Number(record.version) === 1,
            resumable: record.kind === kind && record.ready === false,
            error: response.errors?.map((e) => e.message).join("; "),
          });
      })
      .catch((error) => {
        if (current)
          setChecked({ database, ready: false, error: error.message });
      });
    return () => {
      current = false;
    };
  }, [database, job, kind]);
  const ready =
    job?.phase === "ready" || (checked?.database === database && checked.ready);
  if (ready) return <Fragment key={database}>{children(database)}</Fragment>;
  const definition = demoDescriptions[kind];
  const pending = job?.phase === "creating" || job?.phase === "seeding";
  return (
    <PageLayout>
      <PageHeader title={definition.title} />
      <div className="max-w-3xl mx-auto p-6 space-y-6">
        <p className="text-norse-silver">{definition.description}</p>
        {pending ? (
          <section
            aria-label="Demo setup progress"
            className="rounded-lg border border-norse-rune bg-norse-shadow p-6 space-y-3"
          >
            <h2 className="text-lg text-white">{job.label}</h2>
            <progress
              aria-label="Seeding progress"
              max={Math.max(job.total, 1)}
              value={job.phase === "creating" ? undefined : job.done}
              className="w-full"
            />
            <p role="status" className="text-sm text-norse-silver">
              {job.phase === "creating"
                ? "Preparing a new demo database"
                : `${job.done.toLocaleString()} / ${job.total.toLocaleString()} records`}{" "}
              · {database}
            </p>
          </section>
        ) : (
          <>
            <section className="rounded-lg border border-norse-rune bg-norse-shadow p-6 space-y-4">
              <h2 className="text-lg text-white">
                Create a disposable demo database
              </h2>
              <p className="text-norse-silver">
                Seed creates a new database containing only generated demo data.
                You can safely delete that demo database at any time through
                Databases. It does not use your application databases.
              </p>
              {database && !checked && !job ? (
                <p role="status" className="text-norse-silver">
                  Checking demo database…
                </p>
              ) : (
                database && (
                  <p className="text-norse-silver">
                    This demo database is unavailable or its setup is
                    incomplete.
                  </p>
                )
              )}
              {(job?.error || checked?.error) && (
                <Alert
                  type="error"
                  message={job?.error || checked?.error || ""}
                />
              )}
              <Button
                disabled={Boolean(context.active)}
                onClick={() => context.start(kind)}
              >
                Seed new demo database
              </Button>
              {(checked?.resumable || job?.resumable) && (
                <Button
                  variant="secondary"
                  disabled={Boolean(context.active)}
                  onClick={() => context.start(kind, database)}
                >
                  Resume setup
                </Button>
              )}
            </section>
            {context.active && (
              <Link
                to={`/demos/${context.active.kind}/${context.active.database}`}
                className="text-nornic-primary underline"
              >
                Return to active setup
              </Link>
            )}
          </>
        )}
        {(job?.created || checked?.resumable || checked?.ready) && (
          <Link
            className="text-nornic-primary underline"
            to={`/databases/${encodeURIComponent(database)}`}
          >
            Manage demo database
          </Link>
        )}
      </div>
    </PageLayout>
  );
}
