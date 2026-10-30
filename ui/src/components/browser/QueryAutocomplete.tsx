/**
 * QueryAutocomplete - AI-powered Cypher query autocomplete using Bifrost
 * Provides intelligent query suggestions as the user types
 */

import { useState, useEffect, useRef, useCallback } from "react";
import { Sparkles, Loader2 } from "lucide-react";
import { BASE_PATH, joinBasePath } from "../../utils/basePath";

interface QueryAutocompleteProps {
  query: string;
  onSuggestionSelect: (suggestion: string) => void;
  enabled?: boolean;
  textareaRef?: React.RefObject<HTMLTextAreaElement | null>;
}

interface Suggestion {
  text: string;
  confidence?: number;
}

export function QueryAutocomplete({
  query,
  onSuggestionSelect,
  enabled = true,
  textareaRef,
}: QueryAutocompleteProps) {
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const abortControllerRef = useRef<AbortController | null>(null);
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Debounced function to fetch suggestions
  const fetchSuggestions = useCallback(
    async (currentQuery: string) => {
      // Don't suggest for empty or very short queries
      if (!currentQuery.trim() || currentQuery.trim().length < 3) {
        setSuggestions([]);
        setShowSuggestions(false);
        return;
      }

      // Cancel previous request
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      setIsLoading(true);
      abortControllerRef.current = new AbortController();

      try {
        // Use the new autocomplete endpoint which provides database-aware suggestions
        const response = await fetch(joinBasePath(BASE_PATH, "/api/bifrost/autocomplete"), {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          credentials: "include",
          body: JSON.stringify({
            query: currentQuery,
          }),
          signal: abortControllerRef.current.signal,
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        const suggestionText = data.suggestion?.trim() || "";

        // If we got schema info but no suggestion, the backend will have tried to generate one
        // If still empty, skip showing suggestion
        if (!suggestionText) {
          setSuggestions([]);
          setShowSuggestions(false);
          return;
        }

        // Clean up the suggestion (remove markdown code blocks if present)
        const cleanSuggestion = suggestionText
          .replace(/^```(?:cypher|sql)?\n?/i, "")
          .replace(/\n?```$/i, "")
          .trim();

        // If the suggestion is significantly different or longer, offer it
        if (cleanSuggestion && cleanSuggestion !== currentQuery.trim() && cleanSuggestion.length > currentQuery.trim().length) {
          setSuggestions([
            {
              text: cleanSuggestion,
              confidence: 0.8,
            },
          ]);
          setShowSuggestions(true);
          setSelectedIndex(-1);
        } else {
          setSuggestions([]);
          setShowSuggestions(false);
        }
      } catch (err) {
        // Ignore abort errors
        if (err instanceof Error && err.name === "AbortError") {
          return;
        }
        // Silently fail - autocomplete is a nice-to-have feature
        setSuggestions([]);
        setShowSuggestions(false);
      } finally {
        setIsLoading(false);
      }
    },
    []
  );

  // Track previous enabled state to detect when it becomes true again
  const prevEnabledRef = useRef(enabled);

  // Debounce query changes
  useEffect(() => {
    if (!enabled) {
      setSuggestions([]);
      setShowSuggestions(false);
      // Cancel any pending requests when disabled
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
      prevEnabledRef.current = enabled;
      return;
    }

    // If enabled just became true (was false before), immediately check if we should fetch
    const justEnabled = !prevEnabledRef.current && enabled;
    prevEnabledRef.current = enabled;

    // Clear previous timer
    if (debounceTimerRef.current) {
      clearTimeout(debounceTimerRef.current);
    }

    // If just enabled and query is valid, fetch immediately (shorter debounce)
    // Otherwise, use normal debounce
    const debounceDelay = justEnabled ? 200 : 800;
    
    debounceTimerRef.current = setTimeout(() => {
      fetchSuggestions(query);
    }, debounceDelay);

    return () => {
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, [query, enabled, fetchSuggestions]);

  // Handle keyboard navigation on textarea
  useEffect(() => {
    const textarea = textareaRef?.current;
    if (!textarea) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (!showSuggestions || suggestions.length === 0) return;

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          setSelectedIndex((prev) =>
            prev < suggestions.length - 1 ? prev + 1 : prev
          );
          break;
        case "ArrowUp":
          e.preventDefault();
          setSelectedIndex((prev) => (prev > 0 ? prev - 1 : -1));
          break;
        case "Enter":
          if (selectedIndex >= 0 && selectedIndex < suggestions.length) {
            e.preventDefault();
            onSuggestionSelect(suggestions[selectedIndex].text);
            setShowSuggestions(false);
            setSuggestions([]);
          }
          break;
        case "Escape":
          e.preventDefault();
          setShowSuggestions(false);
          setSelectedIndex(-1);
          break;
      }
    };

    textarea.addEventListener("keydown", handleKeyDown);
    return () => {
      textarea.removeEventListener("keydown", handleKeyDown);
    };
  }, [showSuggestions, suggestions, selectedIndex, onSuggestionSelect, textareaRef]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }
      if (debounceTimerRef.current) {
        clearTimeout(debounceTimerRef.current);
      }
    };
  }, []);

  if (!enabled || (!isLoading && suggestions.length === 0 && !showSuggestions)) {
    return null;
  }

  return (
    <div className="relative">
      {/* Loading indicator */}
      {isLoading && (
        <div className="absolute top-2 right-2 flex items-center gap-2 text-norse-silver text-xs">
          <Loader2 className="w-3 h-3 animate-spin" />
          <span>AI suggesting...</span>
        </div>
      )}

      {/* Suggestions dropdown */}
      {showSuggestions && suggestions.length > 0 && (
        <div className="absolute top-full left-0 right-0 mt-1 bg-norse-shadow border border-norse-rune rounded-lg shadow-xl z-50 max-h-60 overflow-y-auto">
          <div className="p-2">
            <div className="flex items-center gap-2 px-2 py-1 text-xs text-norse-silver mb-1">
              <Sparkles className="w-3 h-3" />
              <span>AI Suggestion</span>
            </div>
            {suggestions.map((suggestion, index) => (
              <button
                key={`suggestion-${index}-${suggestion.text.substring(0, 20)}`}
                type="button"
                onClick={() => {
                  onSuggestionSelect(suggestion.text);
                  setShowSuggestions(false);
                  setSuggestions([]);
                }}
                className={`w-full text-left px-3 py-2 rounded text-sm font-mono transition-colors ${
                  index === selectedIndex
                    ? "bg-nornic-primary/20 text-white"
                    : "text-norse-silver hover:bg-norse-rune hover:text-white"
                }`}
                onMouseEnter={() => setSelectedIndex(index)}
              >
                <div className="break-words whitespace-pre-wrap">
                  {suggestion.text}
                </div>
              </button>
            ))}
            <div className="px-2 py-1 text-xs text-norse-fog mt-1 border-t border-norse-rune">
              Press Enter to accept, Esc to dismiss
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

