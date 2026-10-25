// Runtime peer of internal/cyphertext.QuoteName. Input is a decoded name:
// whitespace and punctuation are identity, never source trivia.
export function quoteName(name, role = "variable") {
  return "`" + name.replaceAll("`", "``") + "`";
}
