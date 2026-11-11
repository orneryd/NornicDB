import type { ReactNode } from "react";
import { useViewState } from "../../utils/useUrlState";

/** Secondary controls stay discoverable by their visible, keyboard-operable label. */
export function Disclosure({
  id,
  title,
  children,
  defaultOpen = false,
  className = "",
  compact = false,
}: {
  id: string;
  title: string;
  children: ReactNode;
  defaultOpen?: boolean;
  className?: string;
  compact?: boolean;
}) {
  const [open, setOpen] = useViewState(`section.${id}`, defaultOpen);
  return (
    <details
      open={open}
      onToggle={(event) => {
        if (event.currentTarget.open !== open)
          setOpen(event.currentTarget.open);
      }}
      className={`rounded-lg border border-norse-rune bg-norse-shadow ${compact ? "p-2" : "p-4"} ${className}`}
    >
      <summary className="cursor-pointer text-white font-semibold">
        {title}
      </summary>
      <div className={compact ? "mt-2" : "mt-4"}>{children}</div>
    </details>
  );
}
