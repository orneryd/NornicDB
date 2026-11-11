import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useMemo,
  useRef,
} from "react";
import type { Dispatch, ReactNode, SetStateAction } from "react";
import { useLocation, useNavigate } from "react-router-dom";
type Changes = Record<string, string | null>;
type Patch = (
  changes: Changes | ((current: URLSearchParams) => Changes),
  replace?: boolean,
) => void;
const Context = createContext<{ params: URLSearchParams; patch: Patch } | null>(
  null,
);
/** One writer merges non-secret view state into the current route. */
export function UrlStateProvider({ children }: { children: ReactNode }) {
  const location = useLocation(),
    navigate = useNavigate();
  const current = useRef(location);
  current.current = location;
  const params = useMemo(
    () => new URLSearchParams(location.search),
    [location.search],
  );
  const patch: Patch = useCallback(
    (changes, replace = false) => {
      const route = current.current;
      const next = new URLSearchParams(route.search);
      const values = typeof changes === "function" ? changes(next) : changes;
      for (const [key, value] of Object.entries(values))
        if (value === null || value === "") next.delete(key);
        else next.set(key, value);
      const search = next.size ? "?" + next.toString() : "";
      if (search === route.search) return;
      current.current = { ...route, search };
      navigate(
        { pathname: route.pathname, search, hash: route.hash },
        { replace },
      );
    },
    [navigate],
  );
  return createElement(
    Context.Provider,
    { value: { params, patch } },
    children,
  );
}
export function useUrlState() {
  const value = useContext(Context);
  if (!value) throw Error("URL state requires UrlStateProvider");
  return value;
}
export function decodeViewState<T>(raw: string | null, fallback: T): T {
  if (raw === null) return fallback;
  try {
    const decoded = typeof fallback === "string" ? raw : JSON.parse(raw);
    if (
      fallback === null ||
      (Array.isArray(fallback)
        ? Array.isArray(decoded)
        : typeof decoded === typeof fallback)
    )
      return decoded;
  } catch {}
  return fallback;
}
export function useViewState<T>(
  key: string,
  fallback: T,
): [T, Dispatch<SetStateAction<T>>] {
  const { params, patch } = useUrlState();
  const defaultJson = JSON.stringify(fallback);
  const setValue: Dispatch<SetStateAction<T>> = useCallback(
    (next) =>
      patch((current) => {
        const initial = JSON.parse(defaultJson) as T;
        const previous = decodeViewState(current.get(key), initial);
        const resolved =
          typeof next === "function"
            ? (next as (value: T) => T)(previous)
            : next;
        return {
          [key]:
            JSON.stringify(resolved) === defaultJson
              ? null
              : typeof initial === "string"
                ? String(resolved)
                : JSON.stringify(resolved),
        };
      }),
    [patch, key, defaultJson],
  );
  return [decodeViewState(params.get(key), fallback), setValue];
}
