import { useEffect, useMemo, useState } from "react";
import type { GridOptions, GridRecord } from "@ornery/ui-grid-core";
import { api } from "../../utils/api";
import { pivotDetailQuery, type Dimension } from "../../utils/pivotDemo";
import { StatefulGrid } from "../common/StatefulGrid";
import { Alert } from "../common/Alert";

const currency = (value: unknown) =>
  value == null
    ? "—"
    : new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
      }).format(Number(value) / 100);
export function PivotDetails({
  database,
  dimension,
  group,
  total,
}: {
  database: string;
  dimension: Dimension;
  group: string;
  total: number;
}) {
  const [rows, setRows] = useState<GridRecord[]>([]),
    [error, setError] = useState(""),
    [loading, setLoading] = useState(true);
  useEffect(() => {
    let current = true;
    setLoading(true);
    setError("");
    api
      .executeCypherOverHttp(database, pivotDetailQuery(dimension), { group })
      .then((response) => {
        if (!current) return;
        if (response.errors?.length)
          throw Error(response.errors.map((e) => e.message).join("; "));
        const result = response.results?.[0];
        if (!result) throw Error("No order details returned.");
        setRows(
          result.data.map((entry) => {
            const record = Object.fromEntries(
              result.columns.map((name, index) => [name, entry.row[index]]),
            );
            return {
              ...record,
              __gridId: String(record.orderId),
              unitPrice:
                Number(record.units) > 0
                  ? Number(record.revenue) / Number(record.units)
                  : null,
            };
          }),
        );
      })
      .catch((error) => {
        if (current) setError(error.message);
      })
      .finally(() => {
        if (current) setLoading(false);
      });
    return () => {
      current = false;
    };
  }, [database, dimension, group]);
  const options = useMemo<GridOptions>(
    () => ({
      id: `pivot-orders-${dimension}-${group}`,
      title: `Orders for ${group}`,
      data: rows,
      rowIdentity: (r) => String(r.__gridId),
      enableSorting: true,
      enableFiltering: false,
      enablePinning: false,
      columnDefs: [
        {
          name: "orderId",
          displayName: "Order",
          pinnedLeft: true,
          width: "150px",
        },
        { name: "account", displayName: "Account", width: "160px" },
        { name: "product", displayName: "Product", width: "150px" },
        { name: "quarter", displayName: "Quarter", width: "100px" },
        { name: "channel", displayName: "Channel", width: "120px" },
        {
          name: "revenue",
          displayName: "Revenue",
          type: "number" as const,
          formatter: currency,
          width: "150px",
        },
        {
          name: "units",
          displayName: "Units",
          type: "number" as const,
          width: "90px",
        },
        {
          name: "unitPrice",
          displayName: "USD / unit",
          type: "number" as const,
          formatter: currency,
          width: "180px",
        },
      ].map((column) => ({ ...column, enableCellEdit: false })),
    }),
    [rows, dimension, group],
  );
  return (
    <section
      aria-label={`Order details for ${group}`}
      className="p-3 space-y-2 overflow-auto h-full"
    >
      <h3 className="text-sm font-semibold text-white">
        {group} · Order details
      </h3>
      <p className="text-xs text-norse-silver">
        {loading
          ? "Loading orders…"
          : `Showing ${rows.length.toLocaleString()} of ${total.toLocaleString()} orders (up to 100).`}{" "}
        Revenue / unit = revenue ÷ units. Values are read-only.
      </p>
      {error ? (
        <Alert type="error" message={error} />
      ) : (
        <StatefulGrid height={240} options={options} />
      )}
    </section>
  );
}
