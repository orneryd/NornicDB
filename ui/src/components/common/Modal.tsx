/**
 * Modal - Reusable modal component
 */

import { useId, useEffect, useRef } from "react";
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
  const dialog = useRef<HTMLDialogElement>(null);
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;
  useEffect(()=>{
    const element=dialog.current;
    if(!element)return;
    if(isOpen && !element.open){element.showModal();element.querySelector<HTMLElement>('[data-autofocus]')?.focus();}
    if(!isOpen && element.open)element.close();
    return ()=>{if(element.open)element.close()};
  },[isOpen]);

  const sizeClasses = {
    sm: "max-w-sm",
    md: "max-w-md",
    lg: "max-w-lg",
    xl: "max-w-xl",
  };

  return (
    <dialog
      ref={dialog}
      onCancel={event=>{event.preventDefault();onCloseRef.current()}}
      className="bg-transparent text-white p-0 max-w-none max-h-[90vh] backdrop:bg-black/60 backdrop:backdrop-blur-sm"
      aria-labelledby={title ? titleId : undefined}
    >
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
    </dialog>
  );
}

