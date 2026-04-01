import * as React from "react";
import { cn } from "@/lib/utils";

export function Input({ className, ...props }: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      className={cn(
        "h-10 w-full rounded-md border border-border bg-white px-3 py-2 text-sm outline-none ring-primary placeholder:text-neutral-500 focus:ring-2",
        className
      )}
      {...props}
    />
  );
}
