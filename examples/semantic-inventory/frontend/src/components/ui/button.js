import { jsx as _jsx } from "react/jsx-runtime";
import { cn } from "@/lib/utils";
export function Button({ className, variant = "default", ...props }) {
    return (_jsx("button", { className: cn("inline-flex items-center justify-center rounded-md px-4 py-2 text-sm font-medium transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary disabled:opacity-50", variant === "default" && "bg-primary text-white hover:brightness-110", variant === "outline" && "border border-border bg-card hover:bg-muted", className), ...props }));
}
