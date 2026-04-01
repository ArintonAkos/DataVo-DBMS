import { jsx as _jsx } from "react/jsx-runtime";
import { cn } from "@/lib/utils";
export function Card({ className, ...props }) {
    return _jsx("div", { className: cn("rounded-lg border border-border bg-card p-4 shadow-sm", className), ...props });
}
export function CardTitle({ className, ...props }) {
    return _jsx("h3", { className: cn("text-lg font-semibold", className), ...props });
}
