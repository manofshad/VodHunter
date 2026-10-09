import { RefObject, useEffect, useRef } from "react";
import { Download, X } from "lucide-react";

interface ShortcutDialogProps {
  open: boolean;
  returnFocusRef: RefObject<HTMLButtonElement>;
  onClose: () => void;
}

export function ShortcutDialog({ open, returnFocusRef, onClose }: ShortcutDialogProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const wasOpenRef = useRef(false);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) {
      return;
    }

    if (!open) {
      if (dialog.open) {
        dialog.close();
      }
      if (wasOpenRef.current) {
        wasOpenRef.current = false;
        returnFocusRef.current?.focus();
      }
      return;
    }

    wasOpenRef.current = true;
    if (!dialog.open) {
      dialog.showModal();
    }
    closeButtonRef.current?.focus();
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = previousOverflow;
    };
  }, [open, returnFocusRef]);

  return (
    <dialog
      ref={dialogRef}
      id="vodhunter-shortcut-dialog"
      aria-labelledby="vodhunter-shortcut-title"
      aria-describedby="vodhunter-shortcut-description"
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
      onClose={() => {
        if (open) {
          onClose();
        }
      }}
      onClick={(event) => {
        if (event.target !== event.currentTarget) {
          return;
        }
        const bounds = event.currentTarget.getBoundingClientRect();
        if (
          event.clientX < bounds.left || event.clientX > bounds.right ||
          event.clientY < bounds.top || event.clientY > bounds.bottom
        ) {
          onClose();
        }
      }}
      className="m-auto max-h-[calc(100dvh-2rem)] w-[calc(100%-2rem)] max-w-[440px] overflow-y-auto rounded-2xl border border-gray-700 bg-gray-900 p-0 text-white shadow-2xl backdrop:bg-black/70"
    >
      <div className="p-5 sm:p-6">
        <div className="flex items-start justify-between gap-3">
          <h2 id="vodhunter-shortcut-title" className="flex min-h-11 items-center text-xl font-bold sm:text-2xl">Search from TikTok</h2>
          <button
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
            aria-label="Close shortcut instructions"
            className="flex size-11 shrink-0 items-center justify-center rounded-lg text-gray-400 transition hover:bg-gray-800 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#fb2844]"
          >
            <X aria-hidden="true" className="size-5" />
          </button>
        </div>

        <p id="vodhunter-shortcut-description" className="mt-2 text-sm leading-6 text-gray-300 sm:text-base">
          Find the original Twitch VOD from TikTok.
        </p>

        <a
          href="/shortcuts/vodhunter-search.shortcut"
          download="vodhunter-search.shortcut"
          className="mt-5 flex min-h-12 items-center justify-center gap-2 rounded-xl bg-[#fb2844] px-4 py-3 text-sm font-bold text-white transition hover:bg-[#e9233e] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white focus-visible:ring-offset-2 focus-visible:ring-offset-gray-900 sm:text-base"
        >
          <Download aria-hidden="true" className="size-5" />
          Download shortcut
        </a>
        <ol role="list" className="mt-5 list-none space-y-4 text-sm leading-6 text-gray-300 sm:text-base">
          <li className="flex items-start gap-3">
            <span aria-hidden="true" className="flex size-8 shrink-0 items-center justify-center rounded-full bg-gray-800 font-semibold text-gray-200">1</span>
            <p className="min-w-0 flex-1 pt-1">Open the file. Tap <strong className="font-semibold text-white">Add Shortcut</strong>.</p>
          </li>
          <li className="flex items-start gap-3">
            <span aria-hidden="true" className="flex size-8 shrink-0 items-center justify-center rounded-full bg-gray-800 font-semibold text-gray-200">2</span>
            <p className="min-w-0 flex-1 pt-1">TikTok → Share → More → <strong className="font-semibold text-white">Search with VodHunter</strong></p>
          </li>
        </ol>
      </div>
    </dialog>
  );
}
