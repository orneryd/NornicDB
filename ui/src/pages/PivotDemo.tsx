import { useMediaQuery } from "../utils/useMediaQuery";
import { PivotDetails } from "../components/demos/PivotDetails";

import { DemoGate } from "../components/common/DemoSeed";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Link } from "react-router-dom";

import type {
  GridOptions,
  GridColumnDef,
  GridCellTemplateContext,
} from "@ornery/ui-grid-core";

import { StatefulGrid } from "../components/common/StatefulGrid";

import { PageLayout } from "../components/common/PageLayout";

import { PageHeader } from "../components/common/PageHeader";

import { Disclosure } from "../components/common/Disclosure";

import { Button } from "../components/common/Button";

import { Alert } from "../components/common/Alert";

import { useViewState, useUrlState } from "../utils/useUrlState";

import { api } from "../utils/api";

import {
  dimensions,
  pivotQuery,
  projectPivot,
  type Dimension,
  type PivotGroup,
} from "../utils/pivotDemo";

export function PivotDemo() {
  return (
    <DemoGate kind="pivot">
      {(database) => <PivotScene database={database} />}
    </DemoGate>
  );
}

function PivotScene({ database }: { database: string }) {
  const mobile = useMediaQuery("(max-width: 767px)");
  const { patch } = useUrlState();

  const [bars, setBars] = useViewState("bars", true);

  const [row, setRow] = useViewState<Dimension>("rows", "account");

  const [column, setColumn] = useViewState<Dimension>("columns", "quarter");

  const [measure, setMeasure] = useViewState<
    "revenue" | "units" | "orders" | "revenuePerUnit" | "revenueShare"
  >("measure", "revenue");

  const [aggregation, setAggregation] = useViewState<"sum" | "average">(
    "aggregation",
    "sum",
  );

  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");

  const [response, setResponse] = useState<{
    signature: string;
    groups: PivotGroup[];
    elapsed: number;
  } | null>(null);

  const valid =
    dimensions.includes(row) &&
    dimensions.includes(column) &&
    column !== "account" &&
    ["revenue", "units", "orders", "revenuePerUnit", "revenueShare"].includes(
      measure,
    ) &&
    ["sum", "average"].includes(aggregation);

  const signature = JSON.stringify([row, column, measure, database]);

  const current = useRef(signature);
  current.current = signature;

  const calculated = measure === "revenuePerUnit" || measure === "revenueShare";

  const query = valid
    ? pivotQuery(row, column, calculated ? "revenue" : measure)
    : "";

  const returned = response?.signature === signature ? response : null;

  const pivot = useMemo(
    () =>
      projectPivot(
        returned?.groups ?? [],
        aggregation === "average" && measure !== "orders",
        calculated ? measure : "value",
      ),
    [returned, aggregation, measure],
  );

  const format = (value: unknown) =>
    value == null
      ? "—"
      : measure === "revenueShare"
        ? new Intl.NumberFormat("en-US", {
            style: "percent",
            maximumFractionDigits: 2,
          }).format(Number(value))
        : measure === "revenue" || measure === "revenuePerUnit"
          ? new Intl.NumberFormat("en-US", {
              style: "currency",
              currency: "USD",
            }).format(Number(value) / 100)
          : new Intl.NumberFormat("en-US", {
              maximumFractionDigits: aggregation === "average" ? 2 : 0,
            }).format(Number(value));

  const columns = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "dimension",
        displayName: row.charAt(0).toUpperCase() + row.slice(1),
        field: "dimension",
        pinnedLeft: true,
        width: mobile ? "200px" : "220px",
        enableCellEdit: false,
      },

      ...pivot.columns.map((name) => ({
        name,
        field: name,
        displayName: name,
        type: "number" as const,
        width: "160px",
        formatter: format,
        enableCellEdit: false,
      })),

      {
        name: "total",
        field: "total",
        displayName: calculated
          ? measure === "revenueShare"
            ? "Revenue share"
            : "USD / unit"
          : aggregation === "average" && measure !== "orders"
            ? "Weighted average"
            : "Total",
        type: "number",
        pinnedRight: !mobile,
        width: "180px",
        formatter: format,
        enableCellEdit: false,
      },
    ],
    [pivot.columns.join("|"), row, measure, aggregation, mobile],
  );

  const options = useMemo<GridOptions>(
    () => ({
      id: "pivot-demo-grid",
      title: "Sales pivot",
      data: pivot.rows,
      columnDefs: columns,
      rowIdentity: (r) => String(r.__gridId),
      enableSorting: true,
      enableFiltering: true,
      enablePinning: true,
      enableExpandable: true,
      expandableRowHeight: 360,
      enableColumnResizing: true,
      emptyMessage: "No groups returned",
    }),
    [pivot.rows, columns],
  );

  const run = async () => {
    if (!valid || !database || busy) return;

    const requested = signature;
    setBusy(true);
    setError("");
    const start = performance.now();

    attempted.current = requested;

    try {
      const result = await api.executeCypherOverHttp(database, query);

      if (current.current !== requested) return;

      if (result.errors?.length)
        throw Error(result.errors.map((e) => e.message).join("; "));

      const first = result.results?.[0];
      if (!first) throw Error("The database returned no result set.");

      const names = first.columns ?? [];

      const groups = first.data.map((entry) => {
        const r = Object.fromEntries(
          names.map((key, index) => [key, entry.row[index]]),
        );
        if (
          typeof r.rowKey !== "string" ||
          typeof r.columnKey !== "string" ||
          !Number.isFinite(Number(r.amount)) ||
          !Number.isFinite(Number(r.records))
        )
          throw Error("The database returned an unexpected pivot result.");
        return {
          rowKey: r.rowKey,
          columnKey: r.columnKey,
          amount: Number(r.amount),
          records: Number(r.records),
          revenue: Number(r.revenue),
          units: Number(r.units),
        };
      });

      setResponse({
        signature: requested,
        groups,
        elapsed: performance.now() - start,
      });
    } catch (e) {
      if (current.current === requested)
        setError(e instanceof Error ? e.message : "Unable to run the pivot.");
    } finally {
      setBusy(false);
    }
  };

  const runRef = useRef(run);
  runRef.current = run;

  const attempted = useRef<string | null>(null);

  useEffect(() => {
    if (busy || !valid || attempted.current === signature) return;

    const timer = setTimeout(() => {
      attempted.current = signature;
      void runRef.current();
    }, 150);

    return () => clearTimeout(timer);
  }, [signature, busy, valid]);

  const maximums = useMemo(
    () =>
      Object.fromEntries(
        [...pivot.columns, "total"].map((column) => [
          column,
          pivot.rows.reduce(
            (maximum, row) =>
              Math.max(
                maximum,
                Number((row as Record<string, unknown>)[column]) || 0,
              ),
            0,
          ),
        ]),
      ),
    [pivot.rows, pivot.columns],
  );

  const renderNumeric = useCallback(
    (context: GridCellTemplateContext) => (
      <span className="relative block py-1 px-2 text-right font-mono text-sm">
        <span
          aria-hidden="true"
          className="absolute inset-y-0 left-0 rounded bg-nornic-primary/15"
          style={{
            width:
              bars && maximums[context.column.name] > 0
                ? `${(Math.max(0, Number(context.value) || 0) / maximums[context.column.name]) * 100}%`
                : "0%",
          }}
        />
        <span className="relative">{format(context.value)}</span>
      </span>
    ),
    [bars, maximums, measure, aggregation],
  );

  const renderers = useMemo(
    () =>
      Object.fromEntries(
        [...pivot.columns, "total"].map((column) => [column, renderNumeric]),
      ),
    [pivot.columns, renderNumeric],
  );

  const control =
    "block w-full mt-1 rounded-lg border border-norse-rune bg-norse-stone p-2 text-white";

  return (
    <PageLayout>
      <PageHeader title="Pivot analysis" />
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        <p className="text-norse-silver">
          Regroup synthetic orders by account, region, product, quarter or
          channel. Dimensions and measures run in NornicDB; the grid virtualizes
          the resulting rows.
        </p>

        <section
          aria-label="Pivot controls"
          className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4"
        >
          <label className="text-sm text-norse-silver">
            Rows
            <select
              className={control}
              value={row}
              onChange={(e) => setRow(e.target.value as Dimension)}
            >
              {dimensions.map((d) => (
                <option key={d}>{d}</option>
              ))}
            </select>
          </label>

          <label className="text-sm text-norse-silver">
            Columns
            <select
              className={control}
              value={column}
              onChange={(e) => setColumn(e.target.value as Dimension)}
            >
              {dimensions
                .filter((d) => d !== "account")
                .map((d) => (
                  <option key={d}>{d}</option>
                ))}
            </select>
          </label>

          <label className="text-sm text-norse-silver">
            Measure
            <select
              className={control}
              value={measure}
              onChange={(e) => setMeasure(e.target.value as typeof measure)}
            >
              <option value="revenue">Revenue (USD)</option>
              <option value="units">Units</option>
              <option value="orders">Orders</option>
              <option value="revenuePerUnit">Revenue per unit (formula)</option>
              <option value="revenueShare">
                Share of all revenue (formula)
              </option>
            </select>
          </label>

          <label className="text-sm text-norse-silver">
            Aggregation
            <select
              className={control}
              value={measure === "orders" || calculated ? "sum" : aggregation}
              disabled={measure === "orders" || calculated}
              onChange={(e) =>
                setAggregation(e.target.value as typeof aggregation)
              }
            >
              <option value="sum">
                {calculated
                  ? "Calculated"
                  : measure === "orders"
                    ? "Count"
                    : "Sum"}
              </option>
              <option value="average">Average per order</option>
            </select>
          </label>
        </section>

        {!valid && (
          <Alert type="error" message="Choose supported pivot controls." />
        )}

        <div className="flex flex-wrap items-center gap-4">
          <Button
            disabled={busy || !database || !valid}
            onClick={() => void run()}
          >
            {busy ? "Running…" : "Refresh pivot"}
          </Button>
          <Button
            variant="secondary"
            disabled={row === "account"}
            aria-describedby={
              row === "account" ? "pivot-account-axis" : undefined
            }
            onClick={() => patch({ rows: column, columns: row })}
          >
            Swap rows and columns
          </Button>
          <label className="flex items-center gap-2 text-sm text-norse-silver">
            <input
              type="checkbox"
              checked={bars}
              onChange={(e) => setBars(e.target.checked)}
            />
            Show relative values
          </label>
          <Link
            className="text-nornic-primary underline"
            to={`/databases/${encodeURIComponent(database)}`}
          >
            Manage demo database
          </Link>
        </div>

        {row === "account" && (
          <p id="pivot-account-axis" className="text-xs text-norse-silver">
            Accounts stay on rows to keep 1,000 accounts readable. Choose
            another row dimension to swap axes.
          </p>
        )}

        <p role="status" className="text-sm text-norse-silver">
          {returned
            ? `${returned.groups.reduce((total, group) => total + group.records, 0).toLocaleString()} orders · ${pivot.rows.length.toLocaleString()} pivot rows · ${returned.elapsed.toFixed(1)} ms request elapsed`
            : busy
              ? "Calculating pivot…"
              : "Choose dimensions and run the pivot."}
        </p>

        {error && <Alert type="error" message={error} />}

        <p className="text-sm text-norse-silver">
          Expand any row to inspect its orders in a detail grid. You can keep
          several rows open.
        </p>

        {calculated && (
          <p className="text-sm text-norse-silver">
            <strong>Formula: </strong>
            {measure === "revenuePerUnit"
              ? "sum(revenue) ÷ sum(units). Totals recalculate the ratio from all contributing orders."
              : "sum(revenue in group) ÷ sum(revenue in the entire dataset). Totals sum each row’s share."}
          </p>
        )}

        <StatefulGrid height={420}
          options={options}
          cellRenderers={renderers}
          expandableRowRenderer={({ row: record }) => (
            <PivotDetails
              database={database}
              dimension={row}
              group={String(record.dimension)}
              total={Number(record.__records)}
            />
          )}
        />

        {bars && (
          <p className="text-xs text-norse-silver">
            Bars compare values within each column. Exact values remain visible.
          </p>
        )}

        <p className="text-sm text-norse-silver">
          The query reads only this disposable demo database. Request elapsed
          includes network and browser work; it is not a controlled benchmark.
          Average totals are weighted by order count.
        </p>

        <Disclosure id="pivot-query" title="Query and dataset">
          <pre className="overflow-auto text-sm text-norse-silver">{query}</pre>
          <p className="mt-3 text-sm text-norse-silver">
            DemoSale nodes contain deterministic synthetic sales. Pivot queries
            do not modify them.
          </p>
        </Disclosure>
      </div>
    </PageLayout>
  );
}
