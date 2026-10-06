import { useState, type ReactElement } from "react";
import type { GraphPropertyFilter } from "../utils/api";

const inputClass =
  "w-full rounded border border-norse-rune bg-norse-night px-1.5 py-1 text-[11px] text-norse-silver placeholder:text-norse-silver/30 focus:outline-none focus:border-sky-400";

function Chip(props: {
  text: string;
  onRemove: () => void;
}): ReactElement {
  return (
    <span className="inline-flex items-center gap-1 rounded bg-norse-rune/40 border border-norse-rune px-1.5 py-0.5 text-[10px] font-mono text-norse-silver">
      {props.text}
      <button
        type="button"
        aria-label={`remove ${props.text}`}
        onClick={props.onRemove}
        className="text-norse-silver/60 hover:text-red-300"
      >
        ×
      </button>
    </span>
  );
}

// FilterChipList edits a list of plain string entries (labels, edge types,
// symbol names): chips with remove buttons plus an input with an add button.
export function FilterChipList(props: {
  label: string;
  entries: string[];
  onAdd: (entries: string[]) => void;
  onRemove: (entry: string) => void;
  placeholder?: string;
}): ReactElement {
  const [draft, setDraft] = useState("");
  const add = () => {
    const added = draft
      .split(/[\s,]+/)
      .map((entry) => entry.trim())
      .filter((entry) => entry !== "" && !props.entries.includes(entry));
    if (added.length > 0) {
      props.onAdd([...props.entries, ...added]);
    }
    setDraft("");
  };
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[10px] text-norse-silver/60">{props.label}</span>
      {props.entries.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {props.entries.map((entry) => (
            <Chip
              key={entry}
              text={entry}
              onRemove={() => props.onRemove(entry)}
            />
          ))}
        </div>
      )}
      <div className="flex items-center gap-1">
        <input
          type="text"
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
          placeholder={props.placeholder}
          spellCheck={false}
          className={inputClass}
        />
        <button
          type="button"
          onClick={add}
          aria-label={`add ${props.label}`}
          className="rounded border border-norse-rune bg-norse-night px-1.5 py-1 text-[11px] text-norse-silver/70 hover:border-sky-400"
        >
          +
        </button>
      </div>
    </div>
  );
}

function propertyFilterText(entry: GraphPropertyFilter): string {
  const scoped = entry.scope ? `${entry.scope}.` : "";
  const valued = entry.value ? `=${entry.value}` : "";
  return `${scoped}${entry.property}${valued}`;
}

// PropertyFilterList edits structured property filters with separate,
// explicitly labeled inputs: property key, value, and an optional scope
// (node label or edge type). No string encoding is applied client-side.
export function PropertyFilterList(props: {
  label: string;
  entries: GraphPropertyFilter[];
  onAdd: (entries: GraphPropertyFilter[]) => void;
  onRemove: (entry: GraphPropertyFilter) => void;
}): ReactElement {
  const [propertyDraft, setPropertyDraft] = useState("");
  const [valueDraft, setValueDraft] = useState("");
  const [scopeDraft, setScopeDraft] = useState("");
  const add = () => {
    const property = propertyDraft.trim();
    if (property === "") {
      return;
    }
    const value = valueDraft.trim();
    const scope = scopeDraft.trim();
    const entry: GraphPropertyFilter = {
      property,
      ...(scope !== "" ? { scope } : {}),
      ...(value !== "" ? { value } : {}),
    };
    const exists = props.entries.some(
      (item) =>
        item.property === entry.property &&
        (item.value ?? undefined) === (entry.value ?? undefined) &&
        (item.scope ?? undefined) === (entry.scope ?? undefined),
    );
    if (!exists) {
      props.onAdd([...props.entries, entry]);
    }
    setPropertyDraft("");
    setValueDraft("");
    setScopeDraft("");
  };
  return (
    <div className="flex flex-col gap-1">
      <span className="text-[10px] text-norse-silver/60">{props.label}</span>
      {props.entries.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {props.entries.map((entry) => (
            <Chip
              key={propertyFilterText(entry)}
              text={propertyFilterText(entry)}
              onRemove={() => props.onRemove(entry)}
            />
          ))}
        </div>
      )}
      <div className="flex items-center gap-1">
        <input
          type="text"
          value={propertyDraft}
          onChange={(event) => setPropertyDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
          placeholder="property"
          aria-label={`${props.label} property`}
          spellCheck={false}
          className={inputClass}
        />
        <input
          type="text"
          value={valueDraft}
          onChange={(event) => setValueDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              add();
            }
          }}
          placeholder="value"
          aria-label={`${props.label} value`}
          spellCheck={false}
          className={inputClass}
        />
        <button
          type="button"
          onClick={add}
          aria-label={`add ${props.label}`}
          className="rounded border border-norse-rune bg-norse-night px-1.5 py-1 text-[11px] text-norse-silver/70 hover:border-sky-400"
        >
          +
        </button>
      </div>
      <input
        type="text"
        value={scopeDraft}
        onChange={(event) => setScopeDraft(event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            add();
          }
        }}
        placeholder="scope (label/type, optional)"
        aria-label={`${props.label} scope`}
        spellCheck={false}
        className={inputClass}
      />
    </div>
  );
}
