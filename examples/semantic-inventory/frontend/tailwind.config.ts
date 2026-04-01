import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        background: "hsl(48 33% 97%)",
        foreground: "hsl(25 22% 10%)",
        card: "hsl(0 0% 100%)",
        border: "hsl(38 22% 86%)",
        primary: "hsl(173 78% 28%)",
        muted: "hsl(42 24% 92%)"
      }
    }
  },
  plugins: []
} satisfies Config;
