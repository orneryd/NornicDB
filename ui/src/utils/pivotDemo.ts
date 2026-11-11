export const dimensions = [
  "account",
  "region",
  "product",
  "quarter",
  "channel",
] as const;
export type Dimension = (typeof dimensions)[number];
export type Sale = {
  account: string;
  region: string;
  product: string;
  quarter: string;
  channel: string;
  revenue: number;
  units: number;
};
export type PivotGroup = {
  rowKey: string;
  columnKey: string;
  amount: number;
  records: number;
  revenue?: number;
  units?: number;
};
export function sampleSales(size: number): Sale[] {
  return Array.from({ length: size }, (_, i) => ({
    account: `Account ${String((i % 1000) + 1).padStart(4, "0")}`,
    region: ["Americas", "Europe", "Asia Pacific", "Middle East & Africa"][
      i % 4
    ],
    product: ["Database", "Support", "Training", "Consulting"][
      Math.floor(i / 4) % 4
    ],
    quarter: `Q${(Math.floor(i / 16) % 4) + 1}`,
    channel: Math.floor(i / 64) % 2 ? "Partner" : "Direct",
    revenue: ((i * 7919) % 90000) + 10000,
    units: (i % 7) + 1,
  }));
}
export function pivotQuery(
  row: Dimension,
  column: Dimension,
  measure: "revenue" | "units" | "orders",
) {
  if (!dimensions.includes(row) || !dimensions.includes(column))
    throw Error("Choose supported dimensions");
  const amount =
    measure === "orders"
      ? "count(*)"
      : `sum(sale.${measure === "units" ? "units" : "revenue"})`;
  return `MATCH (sale:DemoSale)\nRETURN sale.${row} AS rowKey, sale.${column} AS columnKey,\n       ${amount} AS amount, count(*) AS records, sum(sale.revenue) AS revenue, sum(sale.units) AS units\nORDER BY rowKey, columnKey`;
}
export function aggregateSales(
  sales: Sale[],
  row: Dimension,
  column: Dimension,
  measure: "revenue" | "units" | "orders",
): PivotGroup[] {
  const groups = new Map<string, PivotGroup>();
  for (const sale of sales) {
    const key = JSON.stringify([sale[row], sale[column]]);
    const group = groups.get(key) ?? {
      rowKey: sale[row],
      columnKey: sale[column],
      amount: 0,
      records: 0,
    };
    group.amount += measure === "orders" ? 1 : sale[measure];
    group.records++;
    groups.set(key, group);
  }
  return [...groups.values()];
}
export function projectPivot(
  groups: PivotGroup[],
  average: boolean,
  calculation: "value" | "revenuePerUnit" | "revenueShare" = "value",
) {
  const grandRevenue = groups.reduce(
    (total, group) => total + (group.revenue ?? group.amount),
    0,
  );
  const calculate = (
    amount: number,
    count: number,
    revenue: number,
    units: number,
  ) =>
    calculation === "revenuePerUnit"
      ? units > 0
        ? revenue / units
        : null
      : calculation === "revenueShare"
        ? grandRevenue > 0
          ? revenue / grandRevenue
          : null
        : average
          ? count > 0
            ? amount / count
            : null
          : amount;
  const columns = [...new Set(groups.map((g) => g.columnKey))].sort();
  const rows = new Map<string, Record<string, unknown>>();
  for (const group of groups) {
    const row = rows.get(group.rowKey) ?? {
      __gridId: group.rowKey,
      dimension: group.rowKey,
      __amount: 0,
      __records: 0,
      __revenue: 0,
      __units: 0,
    };
    row[group.columnKey] = calculate(
      group.amount,
      group.records,
      group.revenue ?? group.amount,
      group.units ?? 0,
    );
    row.__revenue = Number(row.__revenue) + (group.revenue ?? group.amount);
    row.__units = Number(row.__units) + (group.units ?? 0);
    row.__amount = Number(row.__amount) + group.amount;
    row.__records = Number(row.__records) + group.records;
    rows.set(group.rowKey, row);
  }
  return {
    columns,
    rows: [...rows.values()]
      .sort((a, b) => String(a.dimension).localeCompare(String(b.dimension)))
      .map((row) => ({
        ...row,
        total: calculate(
          Number(row.__amount),
          Number(row.__records),
          Number(row.__revenue),
          Number(row.__units),
        ),
      })),
  };
}

export function pivotDetailQuery(dimension: Dimension) {
  if (!dimensions.includes(dimension))
    throw Error("Choose a supported dimension");
  return `MATCH (sale:DemoSale) WHERE sale.${dimension}=$group
RETURN sale.orderId AS orderId,sale.account AS account,sale.product AS product,sale.quarter AS quarter,sale.channel AS channel,sale.revenue AS revenue,sale.units AS units
ORDER BY orderId LIMIT 100`;
}
