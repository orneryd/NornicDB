/**
 * Modal - Reusable modal component
 */

import { useId } from "react";
import { X } from "lucide-react";

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title?: string;
  children: React.ReactNode;
  size?: "sm" | "md" | "lg" | "xl";
  className?: string;
}

export function Modal({
  isOpen,
  onClose,
  title,
  children,
  size = "md",
  className = "",
}: ModalProps) {
  const titleId = useId();

  if (!isOpen) return null;

  const sizeClasses = {
    sm: "max-w-sm",
    md: "max-w-md",
    lg: "max-w-lg",
    xl: "max-w-xl",
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby={title ? titleId : undefined}
    >
      {/* Backdrop */}
      <button
        type="button"
        className="absolute inset-0"
        onClick={onClose}
        onKeyDown={(e) => {
          if (e.key === "Escape") {
            onClose();
          }
        }}
        aria-label="Close modal"
      />
      {/* Modal Content */}
      <div
        className={`relative bg-norse-deep border border-norse-rune rounded-xl p-6 ${sizeClasses[size]} mx-4 shadow-2xl w-full ${className}`}
        role="document"
      >
        {title && (
          <div className="flex items-center justify-between mb-4">
            <h3 id={titleId} className="text-lg font-semibold text-white">{title}</h3>
            <button
              type="button"
              onClick={onClose}
              className="p-1 hover:bg-norse-rune rounded transition-colors"
              aria-label="Close"
            >
              <X className="w-5 h-5 text-norse-silver" />
            </button>
          </div>
        )}
        {children}
      </div>
    </div>
  );
}

