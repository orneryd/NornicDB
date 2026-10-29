"""Runtime peer of internal/cyphertext.QuoteName for decoded Cypher names."""


def quote_name(name: str, role: str = "variable") -> str:
    return "`" + name.replace("`", "``") + "`"
