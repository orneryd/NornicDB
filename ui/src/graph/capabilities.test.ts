import { describe, expect, it } from "vitest";
import { deriveGraphCapabilities } from "./capabilities";

describe("deriveGraphCapabilities", () => {
  it("uses per-database privileges before global entitlements", () => {
    const capabilities = deriveGraphCapabilities({
      role: "editor",
      database: "analytics",
      privilegesMatrix: [
        { role: "editor", database: "analytics", read: true, write: false },
      ],
      roleEntitlements: {
        editor: ["write"],
      },
    });

    expect(capabilities).toMatchObject({
      accessLevel: "read",
      canAccessGraph: true,
      canInspectNeighborhood: true,
      canExpandGraph: true,
      canTracePaths: true,
      canMutateGraph: false,
    });
  });

  it("falls back to global entitlements when no database privilege exists", () => {
    const capabilities = deriveGraphCapabilities({
      role: "viewer",
      database: "nornic",
      privilegesMatrix: [],
      roleEntitlements: {
        viewer: ["read"],
      },
    });

    expect(capabilities.accessLevel).toBe("read");
    expect(capabilities.canAccessGraph).toBe(true);
    expect(capabilities.canMutateGraph).toBe(false);
  });

  it("enables write-only graph features only for write access with flags", () => {
    const capabilities = deriveGraphCapabilities({
      role: "admin",
      database: "nornic",
      privilegesMatrix: [],
      roleEntitlements: {
        admin: ["admin"],
      },
      featureFlags: {
        temporal: true,
        mutate: true,
      },
    });

    expect(capabilities).toMatchObject({
      accessLevel: "write",
      canViewTemporalHistory: true,
      canMutateGraph: true,
    });
  });

  it("allows plan-level endpoint families to be gated independently", () => {
    const capabilities = deriveGraphCapabilities({
      role: "viewer",
      database: "nornic",
      privilegesMatrix: [],
      roleEntitlements: {
        viewer: ["read"],
      },
      featureFlags: {
        neighborhood: true,
        expand: false,
        path: false,
      },
    });

    expect(capabilities.canInspectNeighborhood).toBe(true);
    expect(capabilities.canExpandGraph).toBe(false);
    expect(capabilities.canTracePaths).toBe(false);
  });

  it("returns no graph access without read or write privileges", () => {
    const capabilities = deriveGraphCapabilities({
      role: "guest",
      database: "nornic",
      privilegesMatrix: [],
      roleEntitlements: {},
    });

    expect(capabilities).toEqual({
      accessLevel: "none",
      canAccessGraph: false,
      canInspectNeighborhood: false,
      canExpandGraph: false,
      canTracePaths: false,
      canViewTemporalHistory: false,
      canMutateGraph: false,
    });
  });
});
