import { withBase } from "vitepress";
export const historical =
  !!import.meta.env.VITE_DOCS_VERSION &&
  import.meta.env.VITE_DOCS_VERSION !== "current";
export const apiUrl = (route: string) =>
  (import.meta.env.VITE_DOCS_API_ORIGIN || "") + withBase("/api" + route);
export const apiFetch = (route: string, options: RequestInit = {}) =>
  historical
    ? Promise.reject(
        new Error("Historical documentation uses browser search only."),
      )
    : fetch(apiUrl(route), { credentials: "omit", ...options });
