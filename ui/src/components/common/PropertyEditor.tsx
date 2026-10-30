/**
 * PropertyEditor - Component for editing node properties
 * Extracted from Browser.tsx for reusability
 */

import { useState } from "react";
import { ChevronRight, X, Check, Plus } from "lucide-react";
import { JsonPreview } from "./JsonPreview";
import { isReadOnlyProperty } from "../../utils/nodeUtils";

interface PropertyEditorProps {
  properties: Record<string, unknown>;
  onSave: (props: Record<string, unknown>) => Promise<void>;
  onCancel: () => void;
}

export function PropertyEditor({
  properties,
  onSave,
  onCancel,
}: PropertyEditorProps) {
  const [editedProps, setEditedProps] = useState<
    Record<string, { key: string; value: string; type: string }>
  >(() => {
    const props: Record<
      string,
      { key: string; value: string; type: string }
    > = {};
    for (const [key, value] of Object.entries(properties)) {
      if (isReadOnlyProperty(key)) continue;
      const valueType =
          typeof value === "string"
            ? "string"
            : typeof value === "number"
            ? "number"
            : typeof value === "boolean"
            ? "boolean"
          : "json";
      // For string type, use value directly (don't JSON.stringify to avoid double quotes)
      // For other types, use JSON.stringify for proper display
      const displayValue =
        valueType === "string" ? (value as string) : JSON.stringify(value);
      props[key] = {
        key,
        value: displayValue,
        type: valueType,
      };
    }
    return props;
  });
  const [newPropKey, setNewPropKey] = useState("");
  const [newPropValue, setNewPropValue] = useState("");
  const [newPropType, setNewPropType] = useState<
    "string" | "number" | "boolean" | "json"
  >("string");
  const [saving, setSaving] = useState(false);

  const addNewProperty = () => {
    if (!newPropKey.trim()) return;

    let parsedValue: unknown;
    try {
      if (newPropType === "json") {
        parsedValue = JSON.parse(newPropValue);
      } else if (newPropType === "number") {
        parsedValue = parseFloat(newPropValue);
        if (Number.isNaN(parsedValue)) {
          alert("Invalid number");
          return;
        }
      } else if (newPropType === "boolean") {
        parsedValue = newPropValue.toLowerCase() === "true";
      } else {
        parsedValue = newPropValue;
      }
    } catch (err) {
      alert(
        `Invalid ${newPropType}: ${err instanceof Error ? err.message : "Parse error"}`
      );
      return;
    }

    setEditedProps({
      ...editedProps,
      [newPropKey]: {
        key: newPropKey,
        value:
          newPropType === "string"
            ? newPropValue
            : JSON.stringify(parsedValue),
        type: newPropType,
      },
    });
    setNewPropKey("");
    setNewPropValue("");
    setNewPropType("string");
  };

  const removeProperty = (key: string) => {
    const newProps = { ...editedProps };
    delete newProps[key];
    setEditedProps(newProps);
  };

  const updateProperty = (
    oldKey: string,
    newKey: string,
    value: string,
    type: string
  ) => {
    const newProps = { ...editedProps };
    if (oldKey !== newKey) {
      delete newProps[oldKey];
    }
    newProps[newKey] = { key: newKey, value, type };
    setEditedProps(newProps);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const propsToSave: Record<string, unknown> = {};

      for (const prop of Object.values(editedProps)) {
        try {
          let parsedValue: unknown;
          if (prop.type === "json") {
            parsedValue = JSON.parse(prop.value);
          } else if (prop.type === "number") {
            parsedValue = parseFloat(prop.value);
            if (Number.isNaN(parsedValue)) {
              throw new Error(`Invalid number: ${prop.value}`);
            }
          } else if (prop.type === "boolean") {
            parsedValue = prop.value.toLowerCase() === "true";
          } else {
            parsedValue = prop.value;
          }
          propsToSave[prop.key] = parsedValue;
        } catch (err) {
          alert(
            `Invalid value for ${prop.key}: ${err instanceof Error ? err.message : "Parse error"}`
          );
          setSaving(false);
          return;
        }
      }

      await onSave(propsToSave);
    } finally {
      setSaving(false);
    }
  };

  // Separate read-only and editable properties
  const readOnlyProps = Object.entries(properties)
    .filter(([key]) => isReadOnlyProperty(key))
    .map(([key, value]) => ({ key, value }));

  return (
    <div className="space-y-3">
      {/* Read-only properties display */}
      {readOnlyProps.length > 0 && (
        <div className="mb-4">
          <h4 className="text-xs font-medium text-norse-silver mb-2">
            READ-ONLY PROPERTIES
          </h4>
          <div className="space-y-2">
            {readOnlyProps.map(({ key, value }) => (
              <div
                key={key}
                className="bg-norse-shadow/50 rounded-lg p-3 opacity-75"
              >
                <div className="flex items-center gap-2 mb-1">
                  <ChevronRight className="w-3 h-3 text-norse-fog" />
                  <span className="text-sm text-frost-ice font-medium">
                    {key}
                  </span>
                  <span className="text-xs text-norse-fog italic">
                    (read-only)
                  </span>
                </div>
                <div className="pl-5">
                  <JsonPreview data={value} expanded />
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Editable properties */}
      <div>
        <h4 className="text-xs font-medium text-norse-silver mb-2">
          EDITABLE PROPERTIES
        </h4>
        {Object.values(editedProps).map((prop) => (
          <div key={prop.key} className="bg-norse-stone rounded-lg p-3 mb-2">
            <div className="flex items-center gap-2 mb-2">
              <input
                type="text"
                value={prop.key}
                onChange={(e) =>
                  updateProperty(prop.key, e.target.value, prop.value, prop.type)
                }
                className="flex-1 px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white"
                placeholder="Property key"
              />
              <select
                value={prop.type}
                onChange={(e) =>
                  updateProperty(
                    prop.key,
                    prop.key,
                    prop.value,
                    e.target.value as "string" | "number" | "boolean" | "json"
                  )
                }
                className="px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white"
              >
                <option value="string">String</option>
                <option value="number">Number</option>
                <option value="boolean">Boolean</option>
                <option value="json">JSON</option>
              </select>
              <button
                type="button"
                onClick={() => removeProperty(prop.key)}
                className="p-1 hover:bg-red-500/20 rounded"
              >
                <X className="w-4 h-4 text-red-400" />
              </button>
            </div>
            <textarea
              value={prop.value}
              onChange={(e) =>
                updateProperty(prop.key, prop.key, e.target.value, prop.type)
              }
              className="w-full px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white font-mono"
              rows={prop.type === "json" ? 4 : 1}
              placeholder={`Enter ${prop.type} value`}
            />
          </div>
        ))}
      </div>

      {/* Add new property */}
      <div className="bg-norse-stone rounded-lg p-3 border-2 border-dashed border-norse-rune">
        <div className="flex items-center gap-2 mb-2">
          <input
            type="text"
            value={newPropKey}
            onChange={(e) => setNewPropKey(e.target.value)}
            className="flex-1 px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white"
            placeholder="New property key"
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                addNewProperty();
              }
            }}
          />
          <select
            value={newPropType}
            onChange={(e) =>
              setNewPropType(
                e.target.value as "string" | "number" | "boolean" | "json"
              )
            }
            className="px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white"
          >
            <option value="string">String</option>
            <option value="number">Number</option>
            <option value="boolean">Boolean</option>
            <option value="json">JSON</option>
          </select>
          <button
            type="button"
            onClick={addNewProperty}
            className="px-2 py-1 bg-nornic-primary/20 hover:bg-nornic-primary/30 text-nornic-primary rounded text-sm"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>
        <textarea
          value={newPropValue}
          onChange={(e) => setNewPropValue(e.target.value)}
          className="w-full px-2 py-1 bg-norse-shadow border border-norse-rune rounded text-sm text-white font-mono"
          rows={newPropType === "json" ? 4 : 1}
          placeholder={`Enter ${newPropType} value`}
          onKeyDown={(e) => {
            if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
              e.preventDefault();
              addNewProperty();
            }
          }}
        />
      </div>

      {/* Save/Cancel buttons */}
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={handleSave}
          disabled={saving}
          className="flex items-center gap-1 px-4 py-2 bg-nornic-primary text-white rounded hover:bg-nornic-secondary disabled:opacity-50"
        >
          <Check className="w-4 h-4" />
          {saving ? "Saving..." : "Save"}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-4 py-2 bg-norse-rune text-norse-silver rounded hover:bg-norse-fog"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
