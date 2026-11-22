/**
 * Header - Header component with logo, stats, and action buttons
 * Extracted from Browser.tsx for reusability
 */

import { useEffect, useState } from "react";
import {
  LogOut,
} from "lucide-react";
import { useAppStore } from "../../store/appStore";
import { api } from "../../utils/api";
import type { DatabaseStats } from "../../utils/api";
import { BASE_PATH, joinBasePath } from "../../utils/basePath";
import {Button} from "../common/Button";
import {MessageCircle} from "lucide-react";
import {useAssistantAvailability} from "../common/AssistantAvailability";
import {ServerStatus} from "../common/ServerStatus";

interface HeaderProps {
  workspace?: boolean;
  stats: DatabaseStats | null;
  onAIChatClick: () => void;
}

export function Header({
  workspace = false,
  stats,
  onAIChatClick,
}: HeaderProps) {
  const {available} = useAssistantAvailability();
  const logout = useAppStore((state) => state.logout);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [logoutError, setLogoutError] = useState(false);
  const [loggingOut, setLoggingOut] = useState(false);

  useEffect(() => {
    api.getAuthConfig().then((config) => setAuthEnabled(config.securityEnabled));
  }, []);

  const handleLogout = async () => {
    setLogoutError(false);
    setLoggingOut(true);
    try {
      await logout();
      window.location.replace(joinBasePath(BASE_PATH, "/login"));
    } catch {
      setLogoutError(true);
      setLoggingOut(false);
    }
  };
  return (
    <header className="workspace-header bg-norse-shadow border-b border-norse-rune px-4 py-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex shrink-0 items-center gap-3">
          {/* NornicDB Logo - Interwoven threads with gold nexus */}
          <svg
            viewBox="0 0 200 180"
            width="44"
            height="40"
            className="flex-shrink-0"
            role="img"
            aria-hidden="true"
          >
            {/* Three interwoven threads */}
            <path
              d="M 40 140 Q 30 100 50 70 Q 70 40 100 35 Q 130 30 145 55 Q 155 75 140 90"
              fill="none"
              stroke="#4a9eff"
              strokeWidth="12"
              strokeLinecap="round"
              opacity="0.9"
            />
            <path
              d="M 100 25 Q 100 50 85 75 Q 70 100 85 120 Q 100 140 100 165"
              fill="none"
              stroke="#4a9eff"
              strokeWidth="12"
              strokeLinecap="round"
            />
            <path
              d="M 160 140 Q 170 100 150 70 Q 130 40 100 35 Q 70 30 55 55 Q 45 75 60 90"
              fill="none"
              stroke="#4a9eff"
              strokeWidth="12"
              strokeLinecap="round"
              opacity="0.9"
            />
            {/* Central nexus - solid gold colors without gradient for simplicity */}
            <circle cx="100" cy="85" r="12" fill="#d4af37" />
            <circle cx="100" cy="85" r="8" fill="#141824" />
            <circle cx="100" cy="85" r="5" fill="#d4af37" />
            {/* Destiny nodes */}
            <circle cx="55" cy="65" r="5" fill="#d4af37" opacity="0.8" />
            <circle cx="145" cy="65" r="5" fill="#d4af37" opacity="0.8" />
            <circle cx="100" cy="140" r="5" fill="#d4af37" opacity="0.8" />
            {/* Connecting lines */}
            <line
              x1="60"
              y1="67"
              x2="93"
              y2="82"
              stroke="#d4af37"
              strokeWidth="1.5"
              opacity="0.4"
            />
            <line
              x1="140"
              y1="67"
              x2="107"
              y2="82"
              stroke="#d4af37"
              strokeWidth="1.5"
              opacity="0.4"
            />
            <line
              x1="100"
              y1="135"
              x2="100"
              y2="92"
              stroke="#d4af37"
              strokeWidth="1.5"
              opacity="0.4"
            />
          </svg>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="text-lg font-semibold text-white">{workspace ? 'Workspace' : 'NornicDB'}</h1>
              {stats?.server?.version ? (
                <span className="rounded-full border border-valhalla-gold/30 bg-valhalla-gold/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-[0.18em] text-valhalla-gold">
                  v{stats.server.version}
                </span>
              ) : null}
            </div>
            <p className="text-xs text-norse-silver">
              {workspace ? 'Query, search and inspect' : 'The Graph Database That Learns'}
            </p>
            <p className="text-xs text-norse-silver">
              Finding NornicDB useful?{' '}
              <a className="underline hover:text-white" href="https://github.com/orneryd/NornicDB">
                Star us on GitHub
              </a>
            </p>
          </div>
        </div>

        {/* Connection Status */}
        <div className="flex min-w-0 flex-1 basis-auto flex-wrap items-center justify-end gap-2">
          <Button variant="secondary" size="sm" icon={MessageCircle}
            onClick={onAIChatClick} disabled={!available}
            className="assistant-header-launcher"
            title={available ? 'Open AI Assistant' : 'AI assistant unavailable'}>
            AI Assistant
          </Button>

          {authEnabled && (
            <button
              type="button"
              onClick={handleLogout}
              disabled={loggingOut}
              aria-label="Log out"
              className="flex shrink-0 items-center rounded-lg border border-norse-rune bg-norse-shadow px-2.5 py-1.5 text-norse-silver transition-all hover:bg-norse-rune hover:text-white"
              title="Sign out of NornicDB"
            >
              <LogOut className="w-4 h-4" />
            </button>
          )}

          <ServerStatus />
        </div>
      </div>
      {logoutError && (
        <div role="alert" className="absolute top-16 right-4 bg-norse-shadow border border-red-500 rounded-lg px-4 py-2 text-sm text-red-400 shadow-lg">
          Sign out failed. Please try again.
        </div>
      )}
    </header>
  );
}
