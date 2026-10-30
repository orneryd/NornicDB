import {
  createContext,
  useContext,
  useEffect,
  useId,
  useMemo,
  useState,
} from "react";
import { UiGrid } from "@ornery/ui-grid-react";
import {
  buildGridPipeline,
  registerRustWasmGridEngine,
} from "@ornery/ui-grid-core";
import type {
  GridCellTemplateContext,
  GridColumnDef,
  GridOptions,
  GridRecord,
  UiGridApi,
} from "@ornery/ui-grid-core";
import type {
  DecayProfileBinding,
  DecayProfileBundle,
  KPPoliciesResponse,
  KPProfilesResponse,
  PromotionPolicyDef,
  PromotionProfileDef,
} from "../../utils/api";
import {
  buildDecayBindingAlter,
  buildProfileOptionAlter,
  buildPromotionPolicyAlter,
  isFiniteNumber,
  isNonNegative,
  isUnitInterval,
  quotePolicyName,
  targetValue,
  validateTarget,
  type PolicyScope,
} from "../../utils/knowledgePolicyEditing";

interface KnowledgePolicyEditorGridsProps {
  section: "decay" | "promotion";
  profiles: KPProfilesResponse | null;
  policies: KPPoliciesResponse | null;
  saving: boolean;
  onAlter: (statement: string, successMessage: string) => Promise<boolean>;
  onValidationError: (message: string) => void;
}

type BundleRow = GridRecord & DecayProfileBundle & { id: string };
type BindingRow = GridRecord &
  DecayProfileBinding & {
    id: string;
    Scope: PolicyScope;
    Target: string;
  };
type PromotionProfileRow = GridRecord & PromotionProfileDef & { id: string };
type PromotionPolicyRow = GridRecord &
  PromotionPolicyDef & {
    id: string;
    Scope: PolicyScope;
    Target: string;
  };

interface PolicyEditorContextValue {
  saving: boolean;
  updateBundleOption: (
    row: BundleRow,
    field: string,
    value: string | boolean,
  ) => void;
  updatePromotionProfileEnabled: (
    row: PromotionProfileRow,
    enabled: boolean,
  ) => void;
  updatePromotionPolicyEnabled: (
    row: PromotionPolicyRow,
    enabled: boolean,
  ) => void;
}

const PolicyEditorContext = createContext<PolicyEditorContextValue | null>(
  null,
);

const gridLabels = {
  validateError: "Invalid policy value",
  validateRequired: "A value is required",
};

// ui-grid 5 has no per-grid WASM opt-out. Register its TypeScript pipeline
// before mounting so the vanilla adapter does not attempt WASM initialization.
registerRustWasmGridEngine({ buildPipeline: buildGridPipeline });

function editorSelectClass(): string {
  return "w-full min-w-24 rounded border border-norse-rune bg-norse-stone px-2 py-1 text-xs text-white focus:outline-none focus:ring-2 focus:ring-nornic-primary";
}

function EditorToggle({
  checked,
  disabled,
  label,
  onChange,
}: {
  checked: boolean;
  disabled: boolean;
  label: string;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="inline-flex items-center gap-2 py-1 text-xs text-norse-silver">
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        aria-label={label}
        onChange={(event) => onChange(event.target.checked)}
        className="h-4 w-4 rounded border-norse-rune bg-norse-stone text-nornic-primary focus:ring-nornic-primary"
      />
      <span>{checked ? "Enabled" : "Disabled"}</span>
    </label>
  );
}

function usePolicyEditor(): PolicyEditorContextValue {
  const context = useContext(PolicyEditorContext);
  if (!context) {
    throw new Error(
      "Knowledge policy cell renderer requires an editor context",
    );
  }
  return context;
}

function BundleFunctionCell({ row }: GridCellTemplateContext) {
  const bundle = row as BundleRow;
  const { saving, updateBundleOption } = usePolicyEditor();
  return (
    <select
      value={bundle.Function}
      disabled={saving}
      aria-label={`Decay function for ${bundle.Name}`}
      onClick={(event) => event.stopPropagation()}
      onChange={(event) =>
        updateBundleOption(bundle, "Function", event.target.value)
      }
      className={editorSelectClass()}
    >
      {["exponential", "linear", "step", "none"].map((value) => (
        <option key={value} value={value}>
          {value}
        </option>
      ))}
    </select>
  );
}

function BundleScoreFromCell({ row }: GridCellTemplateContext) {
  const bundle = row as BundleRow;
  const { saving, updateBundleOption } = usePolicyEditor();
  return (
    <select
      value={bundle.ScoreFrom}
      disabled={saving}
      aria-label={`Score source for ${bundle.Name}`}
      onClick={(event) => event.stopPropagation()}
      onChange={(event) =>
        updateBundleOption(bundle, "ScoreFrom", event.target.value)
      }
      className={editorSelectClass()}
    >
      {["CREATED", "VERSION", "CUSTOM", "LAST_ACCESSED"].map((value) => (
        <option key={value} value={value}>
          {value}
        </option>
      ))}
    </select>
  );
}

function BundleDecayEnabledCell({ row }: GridCellTemplateContext) {
  const bundle = row as BundleRow;
  const { saving, updateBundleOption } = usePolicyEditor();
  return (
    <EditorToggle
      checked={bundle.DecayEnabled}
      disabled={saving}
      label={`Toggle decay for ${bundle.Name}`}
      onChange={(checked) =>
        updateBundleOption(bundle, "DecayEnabled", checked)
      }
    />
  );
}

function BundleEnabledCell({ row }: GridCellTemplateContext) {
  const bundle = row as BundleRow;
  const { saving, updateBundleOption } = usePolicyEditor();
  return (
    <EditorToggle
      checked={bundle.Enabled}
      disabled={saving}
      label={`Toggle ${bundle.Name}`}
      onChange={(checked) => updateBundleOption(bundle, "Enabled", checked)}
    />
  );
}

function PromotionProfileEnabledCell({ row }: GridCellTemplateContext) {
  const profile = row as PromotionProfileRow;
  const { saving, updatePromotionProfileEnabled } = usePolicyEditor();
  return (
    <EditorToggle
      checked={profile.Enabled}
      disabled={saving}
      label={`Toggle ${profile.Name}`}
      onChange={(checked) => updatePromotionProfileEnabled(profile, checked)}
    />
  );
}

function PromotionPolicyEnabledCell({ row }: GridCellTemplateContext) {
  const policy = row as PromotionPolicyRow;
  const { saving, updatePromotionPolicyEnabled } = usePolicyEditor();
  return (
    <EditorToggle
      checked={policy.Enabled}
      disabled={saving}
      label={`Toggle ${policy.Name}`}
      onChange={(checked) => updatePromotionPolicyEnabled(policy, checked)}
    />
  );
}

const bundleRenderers = {
  Function: BundleFunctionCell,
  ScoreFrom: BundleScoreFromCell,
  DecayEnabled: BundleDecayEnabledCell,
  Enabled: BundleEnabledCell,
};

const promotionProfileRenderers = {
  Enabled: PromotionProfileEnabledCell,
};

const promotionPolicyRenderers = {
  Enabled: PromotionPolicyEnabledCell,
};

function makeOptions(
  id: string,
  data: readonly GridRecord[],
  columnDefs: readonly GridColumnDef[],
  emptyMessage: string,
  editable: boolean,
): GridOptions {
  return {
    id,
    data,
    columnDefs,
    labels: gridLabels,
    rowIdentity: (row) => String(row.id),
    enableSorting: true,
    enableFiltering: true,
    enableColumnResizing: true,
    enableCellEdit: editable,
    enableCellEditOnFocus: true,
    emptyMessage,
  };
}

function gridHeight(rowCount: number): number {
  return Math.min(460, Math.max(190, rowCount * 44 + 96));
}

async function submitAlter(
  onAlter: (statement: string, successMessage: string) => Promise<boolean>,
  onValidationError: (message: string) => void,
  buildStatement: () => string,
  successMessage: string,
): Promise<boolean> {
  try {
    return await onAlter(buildStatement(), successMessage);
  } catch (error) {
    onValidationError(
      error instanceof Error
        ? error.message
        : "Failed to generate the policy update.",
    );
    return false;
  }
}

function registerPolicyValidators(api: UiGridApi): void {
  api.validate.setValidator(
    "finiteNumber",
    () => (_oldValue, newValue) => isFiniteNumber(newValue),
    () => "Enter a finite number",
  );
  api.validate.setValidator(
    "unitInterval",
    () => (_oldValue, newValue) => isUnitInterval(newValue),
    () => "Enter a value from 0 through 1",
  );
  api.validate.setValidator(
    "nonNegative",
    () => (_oldValue, newValue) => isNonNegative(newValue),
    () => "Enter zero or a positive number",
  );
  api.validate.setValidator(
    "policyTarget",
    () => (_oldValue, newValue, row) =>
      validateTarget(newValue, String(row?.Scope ?? "NODE") as PolicyScope),
    () =>
      "Use a valid label, edge type, colon-separated labels, or * for nodes",
  );
}

async function validateEdit(
  api: UiGridApi,
  row: GridRecord,
  column: GridColumnDef,
  newValue: unknown,
  oldValue: unknown,
  onValidationError: (message: string) => void,
): Promise<boolean> {
  const failures = await api.validate.runValidators(
    row,
    column,
    newValue,
    oldValue,
  );
  if (failures.length === 0) return true;
  const messages = api.validate.getErrorMessages(row, column);
  onValidationError(messages.join(". ") || "The edited value is invalid.");
  return false;
}

async function restoreEditedCell(
  api: UiGridApi,
  row: GridRecord,
  column: GridColumnDef,
  oldValue: unknown,
  rejectedValue: unknown,
): Promise<void> {
  row[column.field ?? column.name] = oldValue;
  await api.validate.runValidators(row, column, oldValue, rejectedValue);
  api.core.refreshRows();
}

export function KnowledgePolicyEditorGrids({
  section,
  profiles,
  policies,
  saving,
  onAlter,
  onValidationError,
}: KnowledgePolicyEditorGridsProps) {
  const [bundleApi, setBundleApi] = useState<UiGridApi | null>(null);
  const [bindingApi, setBindingApi] = useState<UiGridApi | null>(null);
  const [promotionProfileApi, setPromotionProfileApi] =
    useState<UiGridApi | null>(null);
  const [promotionPolicyApi, setPromotionPolicyApi] =
    useState<UiGridApi | null>(null);

  const bundleRows = useMemo<BundleRow[]>(
    () =>
      (profiles?.bundles ?? []).map((bundle) => ({
        ...bundle,
        id: bundle.Name,
      })),
    [profiles],
  );
  const bindingRows = useMemo<BindingRow[]>(
    () =>
      (profiles?.bindings ?? []).map((binding) => ({
        ...binding,
        id: binding.Name,
        Scope: binding.IsEdge ? "EDGE" : "NODE",
        Target: targetValue(
          binding.TargetLabels,
          binding.TargetEdgeType,
          binding.IsWildcard,
        ),
      })),
    [profiles],
  );
  const promotionProfileRows = useMemo<PromotionProfileRow[]>(
    () =>
      (policies?.promotion_profiles ?? []).map((profile) => ({
        ...profile,
        id: profile.Name,
      })),
    [policies],
  );
  const promotionPolicyRows = useMemo<PromotionPolicyRow[]>(
    () =>
      (policies?.promotion_policies ?? []).map((policy) => ({
        ...policy,
        id: policy.Name,
        Scope: policy.IsEdge ? "EDGE" : "NODE",
        Target: targetValue(
          policy.TargetLabels,
          policy.TargetEdgeType,
          policy.IsWildcard,
        ),
      })),
    [policies],
  );

  const bundleColumns = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "Name",
        displayName: "Name",
        field: "Name",
        width: "minmax(12rem, 1.2fr)",
      },
      {
        name: "Function",
        displayName: "Function",
        field: "Function",
        width: "140px",
        enableSorting: false,
      },
      {
        name: "HalfLifeSeconds",
        displayName: "Half-life (seconds)",
        field: "HalfLifeSeconds",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { finiteNumber: true },
      },
      {
        name: "VisibilityThreshold",
        displayName: "Threshold",
        field: "VisibilityThreshold",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { unitInterval: true },
      },
      {
        name: "ScoreFloor",
        displayName: "Score floor",
        field: "ScoreFloor",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { unitInterval: true },
      },
      { name: "Scope", displayName: "Scope", field: "Scope", width: "100px" },
      {
        name: "ScoreFrom",
        displayName: "Score from",
        field: "ScoreFrom",
        width: "160px",
        enableSorting: false,
      },
      {
        name: "ScoreFromProperty",
        displayName: "Anchor property",
        field: "ScoreFromProperty",
        enableCellEdit: true,
        cellEditableCondition: ({ row }) => row.ScoreFrom === "CUSTOM",
        validators: { requiredForCustom: true },
      },
      {
        name: "DecayEnabled",
        displayName: "Decay",
        field: "DecayEnabled",
        width: "120px",
        enableSorting: false,
      },
      {
        name: "Enabled",
        displayName: "Status",
        field: "Enabled",
        width: "120px",
        enableSorting: false,
      },
    ],
    [],
  );

  const bindingColumns = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "Name",
        displayName: "Name",
        field: "Name",
        width: "minmax(12rem, 1fr)",
      },
      { name: "Scope", displayName: "Scope", field: "Scope", width: "90px" },
      {
        name: "Target",
        displayName: "Target",
        field: "Target",
        width: "minmax(14rem, 1.1fr)",
        enableCellEdit: true,
        validators: { required: true, policyTarget: true },
      },
      {
        name: "Apply",
        displayName: "APPLY directives",
        field: "Apply",
        width: "minmax(28rem, 2.5fr)",
        enableCellEdit: true,
        validators: { required: true },
      },
      {
        name: "Order",
        displayName: "Order",
        field: "Order",
        width: "90px",
        align: "end",
      },
    ],
    [],
  );

  const promotionProfileColumns = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "Name",
        displayName: "Name",
        field: "Name",
        width: "minmax(12rem, 1.2fr)",
      },
      { name: "Scope", displayName: "Scope", field: "Scope", width: "100px" },
      {
        name: "Multiplier",
        displayName: "Multiplier",
        field: "Multiplier",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { nonNegative: true },
      },
      {
        name: "ScoreFloor",
        displayName: "Score floor",
        field: "ScoreFloor",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { unitInterval: true },
      },
      {
        name: "ScoreCap",
        displayName: "Score cap",
        field: "ScoreCap",
        type: "number",
        align: "end",
        enableCellEdit: true,
        validators: { unitInterval: true },
      },
      {
        name: "Enabled",
        displayName: "Status",
        field: "Enabled",
        width: "120px",
        enableSorting: false,
      },
    ],
    [],
  );

  const promotionPolicyColumns = useMemo<GridColumnDef[]>(
    () => [
      {
        name: "Name",
        displayName: "Name",
        field: "Name",
        width: "minmax(12rem, 1fr)",
      },
      { name: "Scope", displayName: "Scope", field: "Scope", width: "90px" },
      {
        name: "Target",
        displayName: "Target",
        field: "Target",
        width: "minmax(14rem, 1.1fr)",
        enableCellEdit: true,
        validators: { required: true, policyTarget: true },
      },
      {
        name: "Apply",
        displayName: "APPLY directives",
        field: "Apply",
        width: "minmax(30rem, 2.5fr)",
        enableCellEdit: true,
        validators: { required: true },
      },
      {
        name: "Enabled",
        displayName: "Status",
        field: "Enabled",
        width: "120px",
        enableSorting: false,
      },
    ],
    [],
  );

  const bundleOptions = useMemo(
    () =>
      makeOptions(
        "knowledge-decay-bundles",
        bundleRows,
        bundleColumns,
        "No decay profile bundles defined",
        !saving,
      ),
    [bundleColumns, bundleRows, saving],
  );
  const bindingOptions = useMemo(
    () =>
      makeOptions(
        "knowledge-decay-bindings",
        bindingRows,
        bindingColumns,
        "No decay profile bindings defined",
        !saving,
      ),
    [bindingColumns, bindingRows, saving],
  );
  const promotionProfileOptions = useMemo(
    () =>
      makeOptions(
        "knowledge-promotion-profiles",
        promotionProfileRows,
        promotionProfileColumns,
        "No promotion profiles defined",
        !saving,
      ),
    [promotionProfileColumns, promotionProfileRows, saving],
  );
  const promotionPolicyOptions = useMemo(
    () =>
      makeOptions(
        "knowledge-promotion-policies",
        promotionPolicyRows,
        promotionPolicyColumns,
        "No promotion policies defined",
        !saving,
      ),
    [promotionPolicyColumns, promotionPolicyRows, saving],
  );

  useEffect(() => {
    if (!bundleApi) return;
    registerPolicyValidators(bundleApi);
    bundleApi.validate.setValidator(
      "requiredForCustom",
      () => (_oldValue, newValue, row) =>
        row?.ScoreFrom !== "CUSTOM" || String(newValue ?? "").trim().length > 0,
      () => "An anchor property is required when score source is CUSTOM",
    );
    return bundleApi.edit.on.afterCellEdit(
      (row, column, newValue, oldValue) => {
        if (newValue === oldValue) return;
        void (async () => {
          if (
            !(await validateEdit(
              bundleApi,
              row,
              column,
              newValue,
              oldValue,
              onValidationError,
            ))
          ) {
            await restoreEditedCell(bundleApi, row, column, oldValue, newValue);
            return;
          }
          const numericFields = new Set([
            "HalfLifeSeconds",
            "VisibilityThreshold",
            "ScoreFloor",
          ]);
          const value = numericFields.has(column.name)
            ? Number(newValue)
            : String(newValue ?? "");
          const saved = await submitAlter(
            onAlter,
            onValidationError,
            () =>
              buildProfileOptionAlter(
                "DECAY",
                String(row.Name),
                column.name.charAt(0).toLowerCase() + column.name.slice(1),
                value,
              ),
            `Updated decay profile ${String(row.Name)}`,
          );
          if (!saved) {
            await restoreEditedCell(bundleApi, row, column, oldValue, newValue);
          }
        })();
      },
    );
  }, [bundleApi, onAlter, onValidationError]);

  useEffect(() => {
    if (!promotionProfileApi) return;
    registerPolicyValidators(promotionProfileApi);
    return promotionProfileApi.edit.on.afterCellEdit(
      (row, column, newValue, oldValue) => {
        if (newValue === oldValue) return;
        void (async () => {
          if (
            !(await validateEdit(
              promotionProfileApi,
              row,
              column,
              newValue,
              oldValue,
              onValidationError,
            ))
          ) {
            await restoreEditedCell(
              promotionProfileApi,
              row,
              column,
              oldValue,
              newValue,
            );
            return;
          }
          const saved = await submitAlter(
            onAlter,
            onValidationError,
            () =>
              buildProfileOptionAlter(
                "PROMOTION",
                String(row.Name),
                column.name.charAt(0).toLowerCase() + column.name.slice(1),
                Number(newValue),
              ),
            `Updated promotion profile ${String(row.Name)}`,
          );
          if (!saved) {
            await restoreEditedCell(
              promotionProfileApi,
              row,
              column,
              oldValue,
              newValue,
            );
          }
        })();
      },
    );
  }, [onAlter, onValidationError, promotionProfileApi]);

  useEffect(() => {
    if (!bindingApi) return;
    registerPolicyValidators(bindingApi);
    return bindingApi.edit.on.afterCellEdit(
      (row, column, newValue, oldValue) => {
        if (newValue === oldValue) return;
        void (async () => {
          if (
            !(await validateEdit(
              bindingApi,
              row,
              column,
              newValue,
              oldValue,
              onValidationError,
            ))
          ) {
            await restoreEditedCell(
              bindingApi,
              row,
              column,
              oldValue,
              newValue,
            );
            return;
          }
          const updatedRow = {
            ...row,
            [column.field ?? column.name]: newValue,
          } as BindingRow;
          const saved = await submitAlter(
            onAlter,
            onValidationError,
            () => buildDecayBindingAlter(updatedRow),
            `Updated decay binding ${String(row.Name)}`,
          );
          if (!saved) {
            await restoreEditedCell(
              bindingApi,
              row,
              column,
              oldValue,
              newValue,
            );
          }
        })();
      },
    );
  }, [bindingApi, onAlter, onValidationError]);

  useEffect(() => {
    if (!promotionPolicyApi) return;
    registerPolicyValidators(promotionPolicyApi);
    return promotionPolicyApi.edit.on.afterCellEdit(
      (row, column, newValue, oldValue) => {
        if (newValue === oldValue) return;
        void (async () => {
          if (
            !(await validateEdit(
              promotionPolicyApi,
              row,
              column,
              newValue,
              oldValue,
              onValidationError,
            ))
          ) {
            await restoreEditedCell(
              promotionPolicyApi,
              row,
              column,
              oldValue,
              newValue,
            );
            return;
          }
          const updatedRow = {
            ...row,
            [column.field ?? column.name]: newValue,
          } as PromotionPolicyRow;
          const saved = await submitAlter(
            onAlter,
            onValidationError,
            () => buildPromotionPolicyAlter(updatedRow),
            `Updated promotion policy ${String(row.Name)}`,
          );
          if (!saved) {
            await restoreEditedCell(
              promotionPolicyApi,
              row,
              column,
              oldValue,
              newValue,
            );
          }
        })();
      },
    );
  }, [onAlter, onValidationError, promotionPolicyApi]);

  const updateBundleOption = (
    row: BundleRow,
    field: string,
    value: string | boolean,
  ) => {
    void submitAlter(
      onAlter,
      onValidationError,
      () =>
        buildProfileOptionAlter(
          "DECAY",
          row.Name,
          field.charAt(0).toLowerCase() + field.slice(1),
          value,
        ),
      `Updated decay profile ${row.Name}`,
    );
  };

  const updatePromotionProfileEnabled = (
    row: PromotionProfileRow,
    enabled: boolean,
  ) => {
    void submitAlter(
      onAlter,
      onValidationError,
      () => buildProfileOptionAlter("PROMOTION", row.Name, "enabled", enabled),
      `Updated promotion profile ${row.Name}`,
    );
  };

  const updatePromotionPolicyEnabled = (
    row: PromotionPolicyRow,
    enabled: boolean,
  ) => {
    void submitAlter(
      onAlter,
      onValidationError,
      () =>
        `ALTER PROMOTION POLICY ${quotePolicyName(row.Name)} ${enabled ? "ENABLE" : "DISABLE"}`,
      `${enabled ? "Enabled" : "Disabled"} promotion policy ${row.Name}`,
    );
  };

  const decayBundlesHeadingId = useId();
  const decayBindingsHeadingId = useId();
  const promotionPoliciesHeadingId = useId();
  const promotionProfilesHeadingId = useId();

  const editorContext: PolicyEditorContextValue = {
    saving,
    updateBundleOption,
    updatePromotionProfileEnabled,
    updatePromotionPolicyEnabled,
  };

  return (
    <PolicyEditorContext.Provider value={editorContext}>
      <div className="min-w-0 space-y-6">
        {section === "decay" && (
          <section
            aria-labelledby={decayBundlesHeadingId}
            className="min-w-0 overflow-hidden bg-norse-shadow border border-norse-rune rounded-lg p-4 sm:p-6 space-y-4"
          >
            <div>
              <h2
                id={decayBundlesHeadingId}
                className="text-lg font-semibold text-white"
              >
                Decay profile bundles
              </h2>
              <p className="mt-1 text-sm text-norse-silver">
                Select a constrained value or focus a numeric cell to edit it.
                Press Enter to commit and Escape to cancel.
              </p>
            </div>
            <div
              className="nornic-grid w-full min-w-0 max-w-full overflow-x-auto"
              style={{ height: gridHeight(bundleRows.length) }}
              aria-busy={saving}
            >
              <UiGrid
                options={bundleOptions}
                onRegisterApi={setBundleApi}
                cellRenderers={bundleRenderers}
              />
            </div>
          </section>
        )}

        {section === "decay" && (
          <section
            aria-labelledby={decayBindingsHeadingId}
            className="min-w-0 overflow-hidden bg-norse-shadow border border-norse-rune rounded-lg p-4 sm:p-6 space-y-4"
          >
            <div>
              <h2
                id={decayBindingsHeadingId}
                className="text-lg font-semibold text-white"
              >
                Decay profile bindings
              </h2>
              <p className="mt-1 text-sm text-norse-silver">
                Edit a target or the canonical APPLY directives in place.
                Targets accept colon-separated node labels, or one edge type.
              </p>
            </div>
            <div
              className="nornic-grid w-full min-w-0 max-w-full overflow-x-auto"
              style={{ height: gridHeight(bindingRows.length) }}
              aria-busy={saving}
            >
              <UiGrid options={bindingOptions} onRegisterApi={setBindingApi} />
            </div>
          </section>
        )}

        {section === "promotion" && (
          <section
            aria-labelledby={promotionPoliciesHeadingId}
            className="min-w-0 overflow-hidden bg-norse-shadow border border-norse-rune rounded-lg p-4 sm:p-6 space-y-4"
          >
            <div>
              <h2
                id={promotionPoliciesHeadingId}
                className="text-lg font-semibold text-white"
              >
                Promotion policies
              </h2>
              <p className="mt-1 text-sm text-norse-silver">
                Targets and complete APPLY directives are editable. Enablement
                remains an independent switch.
              </p>
            </div>
            <div
              className="nornic-grid w-full min-w-0 max-w-full overflow-x-auto"
              style={{ height: gridHeight(promotionPolicyRows.length) }}
              aria-busy={saving}
            >
              <UiGrid
                options={promotionPolicyOptions}
                onRegisterApi={setPromotionPolicyApi}
                cellRenderers={promotionPolicyRenderers}
              />
            </div>
          </section>
        )}

        {section === "promotion" && (
          <section
            aria-labelledby={promotionProfilesHeadingId}
            className="min-w-0 overflow-hidden bg-norse-shadow border border-norse-rune rounded-lg p-4 sm:p-6 space-y-4"
          >
            <div>
              <h2
                id={promotionProfilesHeadingId}
                className="text-lg font-semibold text-white"
              >
                Promotion profiles
              </h2>
              <p className="mt-1 text-sm text-norse-silver">
                Edit multiplier, floor, and cap values directly; invalid ranges
                are marked before submission.
              </p>
            </div>
            <div
              className="nornic-grid w-full min-w-0 max-w-full overflow-x-auto"
              style={{ height: gridHeight(promotionProfileRows.length) }}
              aria-busy={saving}
            >
              <UiGrid
                options={promotionProfileOptions}
                onRegisterApi={setPromotionProfileApi}
                cellRenderers={promotionProfileRenderers}
              />
            </div>
          </section>
        )}
      </div>
    </PolicyEditorContext.Provider>
  );
}
