/**
 * Alert - Reusable alert/notification component
 */

import { AlertCircle, CheckCircle, Info, X } from "lucide-react";
import { useState } from "react";

interface AlertProps {
  type?: "error" | "success" | "info" | "warning";
  title?: string;
  message: string;
  dismissible?: boolean;
  onDismiss?: () => void;
  className?: string;
}

export function Alert({
  type = "info",
  title,
  message,
  dismissible = false,
  onDismiss,
  className = "",
}: AlertProps) {
  const [dismissed, setDismissed] = useState(false);

  if (dismissed) return null;

  const typeConfig = {
    error: {
      bg: "bg-red-500/10",
      border: "border-red-500/30",
      text: "text-red-400",
      icon: AlertCircle,
    },
    success: {
      bg: "bg-green-500/10",
      border: "border-green-500/30",
      text: "text-green-400",
      icon: CheckCircle,
    },
    info: {
      bg: "bg-blue-500/10",
      border: "border-blue-500/30",
      text: "text-blue-400",
      icon: Info,
    },
    warning: {
      bg: "bg-yellow-500/10",
      border: "border-yellow-500/30",
      text: "text-yellow-400",
      icon: AlertCircle,
    },
  };

  const config = typeConfig[type];
  const Icon = config.icon;

  const handleDismiss = () => {
    setDismissed(true);
    onDismiss?.();
  };

  return (
    <div
      className={`${config.bg} ${config.border} border rounded-lg p-3 ${className}`}
    >
      <div className="flex items-start gap-3">
        <Icon className={`w-5 h-5 ${config.text} flex-shrink-0 mt-0.5`} />
        <div className="flex-1 min-w-0">
          {title && (
            <h4 className={`${config.text} font-medium mb-1`}>{title}</h4>
          )}
          <p className={`${config.text} text-sm break-words`}>{message}</p>
        </div>
        {dismissible && (
          <button
            type="button"
            onClick={handleDismiss}
            className={`${config.text} hover:opacity-70 transition-opacity flex-shrink-0`}
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>
    </div>
  );
}

