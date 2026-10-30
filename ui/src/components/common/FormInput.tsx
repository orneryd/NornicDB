/**
 * FormInput - Reusable form input component with icon support
 */

import type { LucideIcon } from "lucide-react";

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
}: FormInputProps) {
  const inputClasses = `w-full ${
    Icon ? "pl-10" : "pl-4"
  } pr-4 py-2 bg-norse-stone border ${
    error ? "border-red-500/50" : "border-norse-rune"
  } rounded-lg text-white placeholder-norse-fog focus:outline-none focus:ring-2 focus:ring-nornic-primary focus:border-transparent disabled:opacity-50 ${className}`;

  return (
    <div>
      {label && (
        <label
          htmlFor={id}
          className="block text-sm font-medium text-norse-silver mb-2"
        >
          {label}
          {required && <span className="text-red-400 ml-1">*</span>}
        </label>
      )}
      <div className="relative">
        {Icon && (
          <Icon className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-norse-fog" />
        )}
        <input
          id={id}
          type={type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          required={required}
          disabled={disabled}
          className={inputClasses}
        />
      </div>
      {error && (
        <p className="mt-1 text-sm text-red-400">{error}</p>
      )}
    </div>
  );
}

