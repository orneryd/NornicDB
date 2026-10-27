import { cypherStatementSpans } from './cypherText.mjs';

export function splitCypherStatements(script: string): string[] {
  return cypherStatementSpans(script)
    .map(([start, end]) => script.slice(start, end).trim())
    .filter(statement => statement.length > 0);
}
