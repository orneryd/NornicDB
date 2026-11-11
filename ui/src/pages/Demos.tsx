import { Link } from "react-router-dom";
import { PageLayout } from "../components/common/PageLayout";
import { PageHeader } from "../components/common/PageHeader";
import { demoDescriptions, demoKinds } from "../utils/demoPlans";
export function Demos() {
  return (
    <PageLayout>
      <PageHeader title="Demos" />
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        <p className="text-norse-silver">
          Explore graph traversal, live updates and analytical queries.
          Generated demos start only when you click Seed and use a new,
          disposable database.
        </p>
        <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-6">
          {demoKinds.map((kind) => (
            <section
              key={kind}
              className="rounded-xl border border-norse-rune bg-norse-shadow p-6 flex flex-col gap-4"
            >
              <h2 className="text-lg font-semibold text-white">
                {demoDescriptions[kind].title}
              </h2>
              <p className="text-norse-silver flex-1">
                {demoDescriptions[kind].description}
              </p>
              <Link
                className="text-nornic-primary underline"
                to={`/demos/${kind}`}
              >
                Open {demoDescriptions[kind].title.toLowerCase()} setup
              </Link>
            </section>
          ))}
        </div>
        <section className="rounded-xl border border-norse-rune bg-norse-shadow p-6 space-y-3">
          <h2 className="text-lg font-semibold text-white">Code graph</h2>
          <p className="text-norse-silver">
            Explore an existing Graphify database or import your own artifact.
            This viewer does not create sample data automatically.
          </p>
          <Link className="text-nornic-primary underline" to="/graphify">
            Open code graph
          </Link>
        </section>
      </div>
    </PageLayout>
  );
}
