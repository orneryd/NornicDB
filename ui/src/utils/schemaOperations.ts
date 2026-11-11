import type { CypherResponse } from "./api";
export const quoteIdentifier = (value: string) => {
  if (!value.trim()) throw Error("Enter a name.");
  return "`" + value.trim().replace(/`/g, "``") + "`";
};
export function schemaStatement({
  kind,
  name,
  entity,
  target,
  properties,
  constraint,
}: {
  kind: "index" | "constraint";
  name: string;
  entity: "node" | "relationship";
  target: string;
  properties: string[];
  constraint: "unique" | "exists";
}) {
  if (!properties.length || properties.some((p) => !p.trim()))
    throw Error("Enter at least one property.");
  const variable = entity === "node" ? "n" : "r",
    pattern =
      entity === "node"
        ? `(n:${quoteIdentifier(target)})`
        : `()-[r:${quoteIdentifier(target)}]-()`;
  const fields = properties
    .map((property) => `${variable}.${quoteIdentifier(property)}`)
    .join(", ");
  if (kind === "index")
    return `CREATE INDEX ${quoteIdentifier(name)} IF NOT EXISTS FOR ${pattern} ON (${fields})`;
  if (constraint === "exists" && properties.length !== 1)
    throw Error("Property existence uses one property.");
  return `CREATE CONSTRAINT ${quoteIdentifier(name)} IF NOT EXISTS FOR ${pattern} REQUIRE ${constraint === "unique" ? `(${fields}) IS UNIQUE` : `${fields} IS NOT NULL`}`;
}
export function resultRecords(response: CypherResponse) {
  if (response.errors?.length)
    throw Error(response.errors.map((e) => e.message).join("; "));
  const result = response.results?.[0];
  if (!result) throw Error("No result set returned.");
  return result.data.map((entry) =>
    Object.fromEntries(
      result.columns.map((column, index) => [column, entry.row[index]]),
    ),
  );
}
