import { useState } from "react";
import { PageLayout } from "../components/common/PageLayout";
import { PageHeader } from "../components/common/PageHeader";
import { FormInput } from "../components/common/FormInput";
import { Button } from "../components/common/Button";
import { Alert } from "../components/common/Alert";
import { Modal } from "../components/common/Modal";
import { Disclosure } from "../components/common/Disclosure";
import { useViewState } from "../utils/useUrlState";
import { BASE_PATH, joinBasePath } from "../utils/basePath";
export function Backups() {
  const [operation, setOperation] = useViewState<"backup" | "restore">(
    "operation",
    "backup",
  );
  const [confirm, setConfirm] = useViewState("confirm", false);
  const [path, setPath] = useState(""),
    [typed, setTyped] = useState(""),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [result, setResult] = useState<{
      operation: string;
      path: string;
      status: string;
    } | null>(null);
  const run = async () => {
    setBusy(true);
    setError("");
    setResult(null);
    try {
      const response = await fetch(
        joinBasePath(BASE_PATH, `/admin/${operation}`),
        {
          method: "POST",
          credentials: "include",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ path }),
        },
      );
      const data = await response.json();
      if (!response.ok)
        throw Error(
          data.message || data.error || `Operation failed (${response.status})`,
        );
      setResult({ operation, path, status: data.status });
      setConfirm(false);
      setTyped("");
    } catch (e) {
      setError(
        e instanceof Error ? e.message : "Unable to complete the operation",
      );
    } finally {
      setBusy(false);
    }
  };
  const valid = operation === "backup" || operation === "restore";
  return (
    <PageLayout>
      <PageHeader title="Backup & restore" />
      <div className="max-w-3xl mx-auto p-6 space-y-5">
        <p className="text-norse-silver">
          These operations cover the entire server, including every database and
          system data. They require administrative permission and access to
          every included database.
        </p>
        <form
          className="rounded-xl border border-norse-rune bg-norse-shadow p-6 space-y-4"
          onSubmit={(event) => {
            event.preventDefault();
            setTyped("");
            setConfirm(true);
          }}
        >
          <label className="block text-sm text-norse-silver">
            Operation
            <select
              disabled={busy}
              className="block w-full mt-1 p-2 rounded border border-norse-rune bg-norse-stone text-white"
              value={operation}
              onChange={(e) => {
                setOperation(e.target.value as typeof operation);
                setResult(null);
                setError("");
              }}
            >
              <option value="backup">Create backup</option>
              <option value="restore">Restore backup</option>
            </select>
          </label>
          <FormInput
            label={
              operation === "restore"
                ? "Existing backup file on server"
                : "Destination backup file on server"
            }
            value={path}
            onChange={setPath}
            required
            placeholder="/backups/nornicdb.bin"
          />
          <p className="text-sm text-norse-silver">
            Use a filesystem path accessible to the NornicDB process. In Docker,
            this is a container path; mount a backup directory to retain the
            file. This does not upload or download a file through your browser.
          </p>
          {operation === "restore" && (
            <Alert
              type="warning"
              message="Restore can replace existing server data. Take a backup first and stop application writes before restoring."
            />
          )}
          <Button type="submit" disabled={busy || !path.trim() || !valid}>
            {operation === "restore" ? "Review restore" : "Review backup"}
          </Button>
        </form>
        {error && !confirm && <Alert type="error" message={error} />}
        <p
          role="status"
          aria-label="Backup operation status"
          className="text-sm text-norse-silver"
        >
          {busy
            ? "Operation running. Keep this page open; closing it does not cancel the server operation."
            : result
              ? `${result.status} · ${result.path}`
              : "No operation has been submitted from this page."}
        </p>
        <Disclosure id="backup-guidance" title="Plan a backup or restore">
          <ul className="list-disc pl-5 space-y-2 text-sm text-norse-silver">
            <li>
              Choose a writable destination and keep the backup outside the
              database data directory.
            </li>
            <li>
              Retain a separate copy of the backup. Protect it as database data.
            </li>
            <li>
              For restore, check that you have the correct backup and plan a
              maintenance window. The server reports completion or an error; it
              does not provide a percentage progress API.
            </li>
            <li>
              Native backups replace the stored dataset. Legacy JSON backups
              create or update their records and may retain records not present
              in the file.
            </li>
            <li>
              Restored authentication or configuration data may require signing
              in again.
            </li>
          </ul>
        </Disclosure>
        <Modal
          isOpen={confirm}
          onClose={() => !busy && setConfirm(false)}
          title={
            operation === "restore"
              ? "Restore all server data"
              : "Create server backup"
          }
        >
          <p className="text-norse-silver mb-4">
            {operation === "restore"
              ? "Restore server data from"
              : "Create a backup of all server databases at"}{" "}
            <code className="break-all text-white">
              {path || "(choose a path first)"}
            </code>
            .
          </p>
          {operation === "restore" && (
            <FormInput
              label="Type RESTORE to confirm"
              value={typed}
              onChange={setTyped}
            />
          )}
          {error && <Alert type="error" message={error} />}
          <div className="flex gap-3 mt-4">
            <Button
              data-autofocus
              variant="secondary"
              disabled={busy}
              onClick={() => setConfirm(false)}
            >
              Cancel
            </Button>
            <Button
              variant={operation === "restore" ? "danger" : "primary"}
              disabled={
                busy ||
                !path.trim() ||
                !valid ||
                (operation === "restore" && typed !== "RESTORE")
              }
              onClick={() => void run()}
            >
              {busy
                ? "Running…"
                : operation === "restore"
                  ? "Restore server"
                  : "Create backup"}
            </Button>
          </div>
        </Modal>
      </div>
    </PageLayout>
  );
}
