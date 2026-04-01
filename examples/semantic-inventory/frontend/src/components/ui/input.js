import { jsx as _jsx } from "react/jsx-runtime";
import { cn } from "@/lib/utils";
export function Input({ className, ...props }) {
    return (_jsx("input", { className: cn("h-10 w-full rounded-md border border-border bg-white px-3 py-2 text-sm outline-none ring-primary placeholder:text-neutral-500 focus:ring-2", className), ...props }));
}
