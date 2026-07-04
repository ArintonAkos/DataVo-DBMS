<template>
  <div v-if="page" class="copy-ai">
    <button class="copy-ai__button" type="button" @click="copyPage">
      {{ buttonText }}
    </button>
    <a class="copy-ai__link" :href="page.aiPath" target="_blank" rel="noopener">
      Open Markdown
    </a>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRoute } from "vitepress";

interface AiPage {
  title: string;
  route: string;
  source: string;
  aiPath: string;
}

const route = useRoute();
const pages = ref<AiPage[]>([]);
const state = ref<"idle" | "copied" | "failed">("idle");

const currentRoute = computed(() => normalizeRoute(route.path));
const page = computed(() => pages.value.find((candidate) => normalizeRoute(candidate.route) === currentRoute.value));
const buttonText = computed(() => {
  if (state.value === "copied") {
    return "Copied";
  }

  if (state.value === "failed") {
    return "Copy failed";
  }

  return "Copy page for AI";
});

onMounted(async () => {
  try {
    const response = await fetch("/ai/index.json");
    if (!response.ok) {
      return;
    }

    const index = await response.json() as { pages?: AiPage[] };
    pages.value = Array.isArray(index.pages) ? index.pages : [];
  } catch {
    pages.value = [];
  }
});

async function copyPage(): Promise<void> {
  if (!page.value) {
    return;
  }

  try {
    const response = await fetch(page.value.aiPath);
    if (!response.ok) {
      throw new Error(`Unable to fetch ${page.value.aiPath}`);
    }

    const markdown = await response.text();
    await navigator.clipboard.writeText(markdown);
    state.value = "copied";
    window.setTimeout(() => {
      state.value = "idle";
    }, 1800);
  } catch {
    state.value = "failed";
    window.setTimeout(() => {
      state.value = "idle";
    }, 2200);
  }
}

function normalizeRoute(value: string): string {
  const routePath = value.split("#")[0].split("?")[0].replace(/\.html$/u, "");
  if (routePath === "" || routePath === "/") {
    return "/";
  }

  return routePath.replace(/\/$/u, "");
}
</script>

<style scoped>
.copy-ai {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 0 0 22px;
  padding: 10px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg-soft);
}

.copy-ai__button {
  border: 1px solid var(--vp-c-brand-1);
  border-radius: 6px;
  padding: 6px 10px;
  color: var(--vp-c-bg);
  background: var(--vp-c-brand-1);
  font-size: 13px;
  font-weight: 700;
  line-height: 1.2;
  cursor: pointer;
}

.copy-ai__button:hover,
.copy-ai__button:focus {
  background: var(--vp-c-brand-2);
}

.copy-ai__link {
  color: var(--vp-c-brand-1);
  font-size: 13px;
  font-weight: 600;
}

@media (max-width: 640px) {
  .copy-ai {
    align-items: stretch;
    flex-direction: column;
  }
}
</style>
