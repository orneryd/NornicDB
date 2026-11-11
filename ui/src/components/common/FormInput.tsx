/**
 * FormInput - Reusable form input component with icon support
 */

import type { LucideIcon } from "lucide-react";
import { useId } from "react";

interface FormInputProps {
  id?: string;
  type?: string;
  label?: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  icon?: LucideIcon;
  error?: string;
  className?: string;
  description?: string;
  min?: number;
  max?: number;
  step?: number | string;
}

export function FormInput({
  id,
  type = "text",
  label,
  value,
  onChange,
  placeholder,
  required = false,
  disabled = false,
  icon: Icon,
  error,
  className = "",
  description,
  min,
  max,
  step,
}: FormInputProps) {
  const generatedId = useId();
  const inputId = id || generatedId;
  const inputClasses = `w-full ${
    Icon ? "pl-10" : "pl-4"
  } pr-4 py-2 bg-norse-stone border ${
    error ? "border-red-500/50" : "border-norse-rune"
  } rounded-lg text-white placeholder-norse-fog focus:outline-none focus:border-nornic-primary disabled:opacity-50 ${className}`;

  return (
    <div>
      {label && (
        <label
          htmlFor={inputId}
          className="block text-sm font-medium text-norse-silver mb-2"
        >
          {label}
          {required && (
            <span aria-hidden="true" className="text-red-400 ml-1">
              *
            </span>
          )}
        </label>
      )}
      <div className="relative">
        {Icon && (
          <Icon className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-norse-fog" />
        )}
        <input
          id={inputId}
          aria-invalid={Boolean(error)}
          aria-describedby={
            [
              description ? `${inputId}-description` : null,
              error ? `${inputId}-error` : null,
            ]
              .filter(Boolean)
              .join(" ") || undefined
          }
          type={type}
          min={min}
          max={max}
          step={step}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          required={required}
          disabled={disabled}
          className={inputClasses}
        />
      </div>
      {description && (
        <p
          id={`${inputId}-description`}
          className="mt-1 text-xs text-norse-silver"
        >
          {description}
        </p>
      )}
      {error && (
        <p
          id={`${inputId}-error`}
          role="alert"
          className="mt-1 text-sm text-red-400"
        >
          {error}
        </p>
      )}
    </div>
  );
}
