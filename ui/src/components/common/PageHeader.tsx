/**
 * PageHeader - Simple page header with back button and title
 */

import { useNavigate } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { Button } from "./Button";
import { ServerStatus } from "./ServerStatus";

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
      <div className="w-full mx-auto flex flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between">
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
        <div className="flex flex-wrap items-center gap-3 sm:shrink-0 sm:justify-end">
          {actions}
          <ServerStatus />
        </div>
      </div>
    </header>
  );
}
