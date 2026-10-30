/**
 * PageLayout - Shared layout component for all pages
 * Provides consistent page structure with optional header
 */

import type { ReactNode } from "react";

interface PageLayoutProps {
  children: ReactNode;
  showHeader?: boolean;
  header?: ReactNode;
  className?: string;
}

export function PageLayout({
  children,
  showHeader = false,
  header,
  className = "",
}: PageLayoutProps) {
  return (
    <div className={`min-h-screen bg-norse-night flex flex-col ${className}`}>
      {showHeader && header && <header>{header}</header>}
      <main className="flex-1">{children}</main>
    </div>
  );
}

