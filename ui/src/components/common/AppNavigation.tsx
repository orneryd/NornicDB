import { Link, useLocation } from "react-router-dom";
import { useMediaQuery } from "../../utils/useMediaQuery";
import { useUrlState } from "../../utils/useUrlState";

const groups = [
  ["Workspace", [["Query, search & graph", "/"]]],
  [
    "Manage",
    [
      ["Databases", "/databases"],
      ["Schema", "/schema"],
    ],
  ],
  [
    "Operations",
    [
      ["Embeddings", "/embeddings"],
      ["Backup & restore", "/backups"],
      ["Lifecycle", "/security/lifecycle"],
      ["Retention", "/security/retention"],
      ["Knowledge policies", "/security/knowledge-policies"],
    ],
  ],
  [
    "Access",
    [
      ["Account & tokens", "/security"],
      ["Users", "/security/admin"],
      ["Database access", "/security/database-access"],
    ],
  ],
  [
    "Demos",
    [
      ["Overview", "/demos"],
      ["Code graph", "/graphify"],
      ["Galaxy", "/demos/galaxy"],
      ["Fleet", "/demos/fleet"],
      ["Pivot analysis", "/demos/pivot"],
    ],
  ],
] as const;

export function AppNavigation() {
  const { params, patch } = useUrlState();
  const mobile = useMediaQuery("(max-width: 767px)");
  const { pathname, search } = useLocation();
  const workspace = pathname.match(/^\/workspace\/([^/]+)/);
  if (pathname.replace(/\/$/, "") === "/login") return null;
  return (
    <nav aria-label="Application" className="app-navigation">
      <a className="skip-workspace" href="#workspace">
        Skip to workspace
      </a>
      <details
        open={!mobile || params.get("navigation") === "open"}
        onToggle={(event) => {
          if (mobile) {
            const open = event.currentTarget.open;
            if (open !== (params.get("navigation") === "open"))
              patch({ navigation: open ? "open" : null });
          }
        }}
      >
        <summary className="navigation-toggle cursor-pointer px-3 py-2 text-white">
          Navigation
        </summary>
        <Link
          to="/"
          className="block px-3 py-3 text-lg font-semibold text-white"
        >
          NornicDB
        </Link>
        {groups.map(([title, links]) => (
          <section key={title} aria-label={title}>
            <h2 className="px-3 pt-4 pb-1 text-xs uppercase tracking-wide text-norse-fog">
              {title}
            </h2>
            {links.map(([label, to]) => {
              const active =
                to === "/"
                  ? pathname === "/" || Boolean(workspace)
                  : pathname.replace(/\/$/, "") === to ||
                    (to.startsWith("/demos/") &&
                      pathname.startsWith(to + "/")) ||
                    (to === "/databases" &&
                      pathname.startsWith("/databases/")) ||
                    (to === "/schema" && pathname.startsWith("/schema/")) ||
                    (to === "/security/admin" &&
                      pathname.startsWith("/security/admin/")) ||
                    (to === "/security/retention" &&
                      pathname.startsWith("/security/retention/"));
              return (
                <Link
                  key={to}
                  to={
                    to === "/" && workspace
                      ? `/workspace/${workspace[1]}${search}`
                      : to
                  }
                  aria-current={active ? "page" : undefined}
                  className={`block rounded-lg px-3 py-2 text-sm ${active ? "bg-nornic-primary/15 text-nornic-primary" : "text-norse-silver hover:bg-norse-rune hover:text-white"}`}
                >
                  {label}
                </Link>
              );
            })}
          </section>
        ))}
      </details>
    </nav>
  );
}
