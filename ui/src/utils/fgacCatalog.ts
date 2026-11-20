/**
 * Editing model for the fine-grained entitlement (FGAC) catalog served by
 * `GET/PUT /auth/fgac/catalog`.
 *
 * The server owns the policy authority: it returns the action contracts, the
 * stored catalog and its revision, and it rejects an invalid catalog or a
 * stale revision. This module only prepares that document for editing, keeps
 * the draft canonical so a reloaded catalog compares equal to the stored one,
 * and mirrors the server's structural rules so an administrator sees a
 * rejected definition before the request is sent.
 *
 * An empty catalog is returned with `null` definitions and assignments; see
 * {@link normalizeCatalog}.
 */

export type FgacResourceKind = "dbms" | "database" | "node" | "relationship";

/** One operation contract registered by the server. */
export interface FgacAction {
  id: string;
  resourceKind: FgacResourceKind;
  property?: boolean;
  creates?: boolean;
}

export interface FgacPredicate {
  property: string;
  operator: string;
  value?: boolean | string | number | Array<boolean | string | number>;
}

/**
 * Selects resources of one kind. An empty database ID selects every database,
 * empty selector lists select every value in that dimension.
 */
export interface FgacScope {
  kind: FgacResourceKind;
  databaseId?: string;
  types?: string[];
  entityIds?: string[];
  properties?: string[];
  homeDatabase?: boolean;
  predicate?: FgacPredicate;
  propertyPatterns?: string[];
}

/** A named, reusable binding of an action and a scope. */
export interface FgacDefinition {
  id: string;
  action: string;
  scope: FgacScope;
  deny?: boolean;
}

/** One Cypher-managed privilege owned by a role. */
export interface FgacCypherPrivilege {
  key: string;
  role: string;
  clause: string;
  deny?: boolean;
  immutable?: boolean;
}

/** Persisted catalog document; both members are null until a catalog exists. */
export interface FgacCatalog {
  definitions: FgacDefinition[] | null;
  assignments: Record<string, string[]> | null;
  privileges?: FgacCypherPrivilege[];
}

/** `GET /auth/fgac/catalog` response. */
export interface FgacCatalogDocument {
  catalog?: FgacCatalog | null;
  revision?: string;
  actions?: FgacAction[];
  enforced?: boolean;
  roles?: string[];
}

/** Editable draft with the nulls resolved. */
export interface FgacCatalogDraft {
  definitions: FgacDefinition[];
  assignments: Record<string, string[]>;
}

/** Text fields of the definition editor. */
export interface FgacDefinitionInput {
  id: string;
  action: string;
  databaseId: string;
  types: string;
  entityIds: string;
  properties: string;
  deny: boolean;
  predicateProperty?: string;
  predicateOperator?: string;
  predicateValue?: string;
  propertyPatterns?: string[];
}

const DEFINITION_ID_PATTERN = /^[a-z][a-z0-9_.-]*$/;

export function isEntityKind(kind: FgacResourceKind): boolean {
  return kind === "node" || kind === "relationship";
}

/** Splits a comma separated selector field, dropping blanks and repeats. */
export function parseSelectors(value: string): string[] {
  const seen = new Set<string>();
  const selectors: string[] = [];
  for (const part of value.split(",")) {
    const selector = part.trim();
    if (!selector || seen.has(selector)) continue;
    seen.add(selector);
    selectors.push(selector);
  }
  return selectors;
}

export function formatSelectors(values?: readonly string[] | null): string {
  return (values ?? []).join(", ");
}

function cloneScope(scope: FgacScope): FgacScope {
  const clone: FgacScope = { kind: scope.kind };
  if (scope.databaseId) clone.databaseId = scope.databaseId;
  if (scope.types?.length) clone.types = [...scope.types];
  if (scope.entityIds?.length) clone.entityIds = [...scope.entityIds];
  if (scope.properties?.length) clone.properties = [...scope.properties];
  if (scope.homeDatabase) clone.homeDatabase = true;
  if (scope.propertyPatterns?.length) clone.propertyPatterns = [...scope.propertyPatterns];
  if (scope.predicate) {
    clone.predicate = { ...scope.predicate };
    if (Array.isArray(scope.predicate.value)) clone.predicate.value = [...scope.predicate.value];
  }
  return clone;
}

export function cloneDefinition(definition: FgacDefinition): FgacDefinition {
  const clone: FgacDefinition = {
    id: definition.id,
    action: definition.action,
    scope: cloneScope(definition.scope),
  };
  if (definition.deny) clone.deny = true;
  return clone;
}

/** Resolves a nullable stored catalog into an editable draft. */
export function normalizeCatalog(catalog?: FgacCatalog | null): FgacCatalogDraft {
  const definitions = (catalog?.definitions ?? []).map(cloneDefinition);
  const assignments: Record<string, string[]> = {};
  for (const [role, ids] of Object.entries(catalog?.assignments ?? {})) {
    const unique = Array.from(new Set(ids ?? []));
    if (role.trim() && unique.length > 0) assignments[role] = unique;
  }
  return { definitions, assignments };
}

/**
 * Builds the `catalog` member of a PUT body: roles without assignments are
 * omitted and each role's definition references are sorted so an unchanged
 * catalog builds the same document on every render.
 */
export function buildCatalogBody(draft: FgacCatalogDraft): FgacCatalog {
  const assignments: Record<string, string[]> = {};
  for (const [role, ids] of Object.entries(draft.assignments)) {
    const unique = Array.from(new Set(ids)).sort();
    if (role.trim() && unique.length > 0) assignments[role] = unique;
  }
  return {
    definitions: draft.definitions.map(cloneDefinition),
    assignments,
  };
}

/** PUT body for the catalog endpoint: the draft and the revision it was read at. */
export function catalogPutBody(
  draft: FgacCatalogDraft,
  revision: string,
): { catalog: FgacCatalog; revision: string } {
  return { catalog: buildCatalogBody(draft), revision };
}

/** Canonical form of a draft, used to decide whether it differs from the stored one. */
export function catalogSignature(draft: FgacCatalogDraft): string {
  return JSON.stringify(buildCatalogBody(draft));
}

export function definitionInput(definition?: FgacDefinition): FgacDefinitionInput {
  return {
    id: definition?.id ?? "",
    action: definition?.action ?? "",
    databaseId: definition?.scope.homeDatabase ? ":home" : definition?.scope.databaseId ?? "",
    types: formatSelectors(definition?.scope.types),
    entityIds: formatSelectors(definition?.scope.entityIds),
    properties: formatSelectors(definition?.scope.properties),
    deny: definition?.deny === true,
    predicateProperty: definition?.scope.predicate?.property,
    predicateOperator: definition?.scope.predicate?.operator,
    predicateValue: definition?.scope.predicate?.value === undefined ? "" : JSON.stringify(definition.scope.predicate.value),
    propertyPatterns: definition?.scope.propertyPatterns,
  };
}

/**
 * Drops fields the selected action cannot carry, so changing the action never
 * leaves a scope the server rejects.
 */
export function isPredicateAction(action: string): boolean {
  return /^graph\.(node|relationship)\.(traverse|property\.read)$/.test(action);
}

export function inputForAction(
  input: FgacDefinitionInput,
  action?: FgacAction,
): FgacDefinitionInput {
  if (!action) return input;
  const next: FgacDefinitionInput = { ...input, action: action.id };
  if (action.resourceKind === "dbms") next.databaseId = "";
  if (!isEntityKind(action.resourceKind)) {
    next.types = "";
    next.entityIds = "";
  } else if (action.creates || !next.databaseId.trim()) {
    next.entityIds = "";
  }
  if (!action.property) next.properties = "";
  if (input.action !== action.id) next.propertyPatterns = undefined;
  if (!isPredicateAction(action.id)) {
    next.predicateProperty = undefined;
    next.predicateOperator = undefined;
    next.predicateValue = undefined;
  }
  return next;
}

/**
 * Validates one definition editor submission against the action contracts and
 * the definitions already in the draft. Returns the definition to store only
 * when no error was found.
 */
const predicateOperators = new Set(["=", "<>", "<", "<=", ">", ">=", "IS NULL", "IS NOT NULL", "IN", "CONTAINS VALUE"]);

function predicateScalar(value: unknown): value is boolean | string | number {
  return typeof value === "boolean" || typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value)));
}

function validatePredicate(input: FgacDefinitionInput, action: string, errors: string[]): FgacPredicate | undefined {
  const operator = input.predicateOperator;
  if (!operator) return;
  const property = input.predicateProperty?.trim() ?? "";
  if (!property || !isPredicateAction(action) || !predicateOperators.has(operator)) {
    errors.push("Select a supported comparison and enter a property name.");
    return;
  }
  if (operator === "IS NULL" || operator === "IS NOT NULL") return { property, operator };
  try {
    const value: unknown = JSON.parse(input.predicateValue ?? "");
    if (operator === "IN" && Array.isArray(value) && value.every(predicateScalar)) {
      return { property, operator, value };
    }
    if (operator !== "IN" && predicateScalar(value)) return { property, operator, value };
  } catch {
    // Report the field error with the rest of the draft validation errors.
  }
  errors.push("Enter a JSON string, boolean, or safe number, or a list of those values for IN.");
}

export function validateDefinitionInput(
  input: FgacDefinitionInput,
  actions: readonly FgacAction[],
  existing: readonly FgacDefinition[],
  editingId?: string,
): { errors: string[]; definition?: FgacDefinition } {
  const errors: string[] = [];
  const id = input.id.trim();
  const action = actions.find((candidate) => candidate.id === input.action);
  if (!id) {
    errors.push("Enter a definition ID.");
  } else if (!DEFINITION_ID_PATTERN.test(id)) {
    errors.push(
      'Definition IDs must start with a lowercase letter and use only a-z, 0-9, "_", ".", "-".',
    );
  } else if (
    existing.some((definition) => definition.id === id && definition.id !== editingId)
  ) {
    errors.push(`A definition with ID "${id}" already exists.`);
  }
  if (!action) {
    errors.push("Select an action.");
    return { errors };
  }

  const types = parseSelectors(input.types);
  const entityIds = parseSelectors(input.entityIds);
  const properties = parseSelectors(input.properties);
  const homeDatabase = input.databaseId === ":home";
  const databaseId = homeDatabase ? "" : input.databaseId.trim();
  const entityKind = isEntityKind(action.resourceKind);

  if (!entityKind && (types.length > 0 || entityIds.length > 0)) {
    errors.push("Only node and relationship actions select labels, types or entity IDs.");
  }
  if (action.resourceKind === "dbms" && databaseId) {
    errors.push("A DBMS action applies to the whole DBMS and cannot select a database.");
  }
  if (entityIds.length > 0 && !databaseId) {
    errors.push("Entity IDs require a specific database.");
  }
  if (entityIds.length > 0 && action.creates) {
    errors.push("Create actions cannot select entity IDs.");
  }
  if (properties.length > 0 && !action.property) {
    errors.push("Only property actions select properties.");
  }
  const parsedPredicate = validatePredicate(input, action.id, errors);
  if (errors.length > 0) return { errors };

  const scope: FgacScope = { kind: action.resourceKind };
  if (action.resourceKind !== "dbms" && databaseId) scope.databaseId = databaseId;
  if (action.resourceKind !== "dbms" && homeDatabase) scope.homeDatabase = true;
  if (parsedPredicate) scope.predicate = parsedPredicate;
  if (action.resourceKind === "dbms" && action.property && input.propertyPatterns?.length && properties.length === 0) {
    scope.propertyPatterns = [...input.propertyPatterns];
  }
  if (entityKind && types.length > 0) scope.types = types;
  if (entityKind && entityIds.length > 0) scope.entityIds = entityIds;
  if (action.property && properties.length > 0) scope.properties = properties;
  const definition: FgacDefinition = { id, action: action.id, scope };
  if (input.deny) definition.deny = true;
  return { errors, definition };
}

/** Adds a definition, or replaces the stored one with the same ID. */
export function upsertDefinition(
  draft: FgacCatalogDraft,
  definition: FgacDefinition,
): FgacCatalogDraft {
  const index = draft.definitions.findIndex((existing) => existing.id === definition.id);
  const definitions = [...draft.definitions];
  if (index === -1) definitions.push(cloneDefinition(definition));
  else definitions[index] = cloneDefinition(definition);
  return { definitions, assignments: { ...draft.assignments } };
}

/** Removes a definition and every role reference to it. */
export function removeDefinition(draft: FgacCatalogDraft, id: string): FgacCatalogDraft {
  const assignments: Record<string, string[]> = {};
  for (const [role, ids] of Object.entries(draft.assignments)) {
    const kept = ids.filter((assigned) => assigned !== id);
    if (kept.length > 0) assignments[role] = kept;
  }
  return {
    definitions: draft.definitions.filter((definition) => definition.id !== id),
    assignments,
  };
}

/** Adds or removes one definition reference for a role. */
export function toggleRoleAssignment(
  assignments: Record<string, string[]>,
  role: string,
  definitionId: string,
): Record<string, string[]> {
  const current = assignments[role] ?? [];
  const next = current.includes(definitionId)
    ? current.filter((id) => id !== definitionId)
    : [...current, definitionId];
  const updated = { ...assignments };
  if (next.length === 0) delete updated[role];
  else updated[role] = next;
  return updated;
}

function roleKey(role: string): string {
  return role.trim().toLowerCase();
}

/**
 * Validates a whole draft before saving. Roles must be existing role records
 * and every assignment must reference a definition in the draft, because the
 * server rejects a catalog that names an unknown action, definition or role.
 */
export function validateCatalog(
  draft: FgacCatalogDraft,
  actions: readonly FgacAction[],
  roles?: readonly string[],
): string[] {
  const errors: string[] = [];
  const actionIds = new Set(actions.map((action) => action.id));
  const definitionIds = new Set<string>();
  for (const definition of draft.definitions) {
    if (!DEFINITION_ID_PATTERN.test(definition.id)) {
      errors.push(`Definition "${definition.id}" has an invalid ID.`);
    }
    if (definitionIds.has(definition.id)) {
      errors.push(`Definition "${definition.id}" appears more than once.`);
    }
    definitionIds.add(definition.id);
    if (!actionIds.has(definition.action)) {
      errors.push(`Definition "${definition.id}" uses unknown action "${definition.action}".`);
    }
  }
  const knownRoles = roles ? new Set(roles.map(roleKey)) : undefined;
  for (const [role, ids] of Object.entries(draft.assignments)) {
    if (!role.trim()) {
      errors.push("A role assignment has no role name.");
      continue;
    }
    if (knownRoles && !knownRoles.has(roleKey(role))) {
      errors.push(`Role "${role}" does not exist; refresh the role list before saving.`);
    }
    for (const id of ids) {
      if (!definitionIds.has(id)) {
        errors.push(`Role "${role}" references missing definition "${id}".`);
      }
    }
    if (ids.length !== new Set(ids).size) {
      errors.push(`Role "${role}" repeats a definition assignment.`);
    }
  }
  return errors;
}

/** Describes a definition's scope using the database catalog's display name. */
export function describeScope(
  definition: FgacDefinition,
  databaseNames: Record<string, string> = {},
): string {
  const scope = definition.scope;
  const parts: string[] = [];
  if (scope.kind === "dbms") {
    parts.push("DBMS-wide");
  } else {
    parts.push(
      scope.databaseId ? (databaseNames[scope.databaseId] ?? scope.databaseId) : "All databases",
    );
  }
  if (isEntityKind(scope.kind)) {
    const all = scope.kind === "node" ? "all labels" : "all relationship types";
    parts.push(scope.types?.length ? scope.types.join(", ") : all);
    if (scope.entityIds?.length) parts.push(`entities: ${scope.entityIds.join(", ")}`);
  }
  if (scope.properties?.length) parts.push(`properties: ${scope.properties.join(", ")}`);
  if (scope.homeDatabase) parts[0] = "Home database";
  if (scope.predicate) parts.push(`where ${scope.predicate.property} ${scope.predicate.operator}`);
  return parts.join(" · ");
}

/** Display an action in language suitable for the access editor. */
export function describeFgacActionName(id: string): string {
  const graph = /^graph\.(node|relationship)\.(?:(property|label|embedding)\.)?(\w+)$/.exec(id);
  if (graph) {
    const [, entity, field, verb] = graph;
    return [verb, entity, field === "property" ? "properties" : field].filter(Boolean).join(" ");
  }
  return id.replace(/[._]/g, " ");
}

const actionGroups: Record<string, string> = {
  graph: "Graph data",
  database: "Databases",
  embedding: "Managed embeddings",
  heimdall: "Heimdall",
  knowledge: "Knowledge policies",
  dbms: "Administration",
  retention: "Retention",
  schema: "Schema",
};

/** Group registered operations for the action picker, retaining a selected value. */
export function groupFgacActions(actions: FgacAction[], search: string, selected?: string): Map<string, FgacAction[]> {
  const groups = new Map<string, FgacAction[]>();
  const terms = search.toLowerCase().match(/\S+/g) ?? [];
  for (const action of actions) {
    const group = actionGroups[action.id.split(".", 1)[0]] ?? "Other operations";
    const searchable = `${group} ${action.id} ${describeFgacActionName(action.id)}`.toLowerCase();
    if (action.id !== selected && !terms.every((term) => searchable.includes(term))) continue;
    const entries = groups.get(group);
    if (entries) entries.push(action);
    else groups.set(group, [action]);
  }
  return groups;
}

/** Short description of a registered action contract. */
export function describeAction(action: FgacAction): string {
  const detail: string[] = [action.resourceKind];
  if (action.property) detail.push("property");
  if (action.creates) detail.push("creates");
  return detail.join(" · ");
}

/** Effective rules returned for the signed-in account. */
export interface FgacEffectiveRules {
  enforced: boolean;
  rules: FgacDefinition[] | null;
  homeDatabaseId?: string;
}

/** Check a concrete operation for a UI control. Backend decisions are authoritative. */
export function permitsFgacAction(effective: FgacEffectiveRules, action: string, databaseId?: string, property?: string): boolean {
  let granted = false;
  for (const rule of effective.rules ?? []) {
    const scope = rule.scope;
    if (rule.action !== action || scope.kind === "node" || scope.kind === "relationship") continue;
    if (scope.kind === "database" && !databaseId) continue;
    if (scope.databaseId && scope.databaseId !== databaseId) continue;
    if (scope.homeDatabase && effective.homeDatabaseId !== databaseId) continue;
    if (scope.properties?.length && (!property || !scope.properties.includes(property))) continue;
    if (scope.propertyPatterns?.length || scope.predicate || scope.entityIds?.length) continue;
    if (rule.deny) return false;
    granted = true;
  }
  return granted;
}

/** Navigation can advertise a view that contains at least one delegated database operation. */
export function hasFgacActionFamily(effective: FgacEffectiveRules, prefix: string): boolean {
  return (effective.rules ?? []).some(rule => !rule.deny && rule.action.startsWith(prefix));
}
