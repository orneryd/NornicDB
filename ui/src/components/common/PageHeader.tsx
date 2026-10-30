/**
 * PageHeader - Simple page header with back button and title
 */

import { useNavigate } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { Button } from "./Button";

interface PageHeaderProps {
  title: string;
  backTo?: string;
  backLabel?: string;
  actions?: React.ReactNode;
}

export function PageHeader({
  title,
  backTo,
  backLabel = "Back",
  actions,
}: PageHeaderProps) {
  const navigate = useNavigate();

  return (
    <header className="bg-norse-shadow border-b border-norse-rune px-4 py-3">
      <div className="max-w-6xl mx-auto flex flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 items-center gap-4">
          {backTo && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => navigate(backTo)}
              icon={ArrowLeft}
            >
              {backLabel}
            </Button>
          )}
          <h1 className="min-w-0 text-xl font-semibold text-white">{title}</h1>
        </div>
        {actions && (
          <div className="flex items-center gap-2 sm:shrink-0">{actions}</div>
        )}
      </div>
    </header>
  );
}
