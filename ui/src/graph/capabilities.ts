export interface GraphPrivilegeEntry {
  role: string;
  database: string;
  read: boolean;
  write: boolean;
}

export interface GraphFeatureFlags {
  neighborhood: boolean;
  expand: boolean;
  path: boolean;
  temporal: boolean;
  mutate: boolean;
}

export interface GraphCapabilities {
  accessLevel: "none" | "read" | "write";
  canAccessGraph: boolean;
  canInspectNeighborhood: boolean;
  canExpandGraph: boolean;
  canTracePaths: boolean;
  canViewTemporalHistory: boolean;
  canMutateGraph: boolean;
}

export interface DeriveGraphCapabilitiesInput {
  role: string;
  database: string;
  privilegesMatrix: GraphPrivilegeEntry[];
  roleEntitlements: Record<string, string[]>;
  featureFlags?: Partial<GraphFeatureFlags>;
}

const defaultFeatureFlags: GraphFeatureFlags = {
  neighborhood: true,
  expand: true,
  path: true,
  temporal: false,
  mutate: false,
};

function normalizeKey(value: string): string {
  return value.toLowerCase().trim();
}

function getEntitlementsForRole(
  role: string,
  roleEntitlements: Record<string, string[]>,
): string[] {
  return roleEntitlements[normalizeKey(role)] ?? roleEntitlements[role] ?? [];
}

function resolveAccessLevel(
  role: string,
  database: string,
  privilegesMatrix: GraphPrivilegeEntry[],
  roleEntitlements: Record<string, string[]>,
): "none" | "read" | "write" {
  const roleKey = normalizeKey(role);
  const databaseKey = normalizeKey(database);
  const entries = privilegesMatrix.filter(
    (entry) =>
      normalizeKey(entry.role) === roleKey &&
      normalizeKey(entry.database) === databaseKey,
  );

  if (entries.length > 0) {
    const hasWrite = entries.some((entry) => entry.write);
    const hasRead = hasWrite || entries.some((entry) => entry.read);
    if (hasWrite) {
      return "write";
    }
    if (hasRead) {
      return "read";
    }
    return "none";
  }

  const entitlements = getEntitlementsForRole(role, roleEntitlements);
  if (entitlements.includes("admin") || entitlements.includes("write")) {
    return "write";
  }
  if (entitlements.includes("read")) {
    return "read";
  }
  return "none";
}

export function deriveGraphCapabilities(
  input: DeriveGraphCapabilitiesInput,
): GraphCapabilities {
  const featureFlags = { ...defaultFeatureFlags, ...input.featureFlags };
  const accessLevel = resolveAccessLevel(
    input.role,
    input.database,
    input.privilegesMatrix,
    input.roleEntitlements,
  );
  const canRead = accessLevel === "read" || accessLevel === "write";
  const canWrite = accessLevel === "write";

  return {
    accessLevel,
    canAccessGraph: canRead,
    canInspectNeighborhood: canRead && featureFlags.neighborhood,
    canExpandGraph: canRead && featureFlags.expand,
    canTracePaths: canRead && featureFlags.path,
    canViewTemporalHistory: canRead && featureFlags.temporal,
    canMutateGraph: canWrite && featureFlags.mutate,
  };
}
