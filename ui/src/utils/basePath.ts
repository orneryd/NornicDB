// normalizeBasePath converts VITE_BASE_PATH into a stable prefix for API URL joins.
// Examples:
//   "" -> ""
//   "/" -> ""
//   "nornic-db" -> "/nornic-db"
//   "/nornic-db/" -> "/nornic-db"
export function normalizeBasePath(raw?: string): string {
  const value = (raw || "").trim();
  if (value === "" || value === "/") return "";
  const withLeading = value.startsWith("/") ? value : `/${value}`;
  return withLeading.replace(/\/+$/, "");
}

// joinBasePath safely joins a normalized base path with an absolute API path.
// It avoids accidental double slashes when base path already has a trailing slash.
export function joinBasePath(basePath: string, absolutePath: string): string {
  const path = absolutePath.startsWith("/") ? absolutePath : `/${absolutePath}`;
  return `${basePath}${path}`;
}

export const BASE_PATH = normalizeBasePath(import.meta.env.VITE_BASE_PATH || "");
