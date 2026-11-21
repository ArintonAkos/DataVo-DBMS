<script setup lang="ts">
import { onMounted, ref } from 'vue';

const REPO_URL = 'https://github.com/ArintonAkos/DataVo-DBMS';
const API_URL = 'https://api.github.com/repos/ArintonAkos/DataVo-DBMS';
const CACHE_KEY = 'datavo:github-stars';
const CACHE_TTL_MS = 2 * 60 * 60 * 1000;
const minStarsToShowCount = 50;

const starCount = ref<number | null>(null);

interface CachedStars {
  count: number;
  ts: number;
}

function readCachedStars(): CachedStars | null {
  try {
    const raw = window.localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<CachedStars>;
    if (typeof parsed.count !== 'number' || typeof parsed.ts !== 'number') {
      return null;
    }
    return { count: parsed.count, ts: parsed.ts };
  } catch {
    return null;
  }
}

function writeCachedStars(count: number): void {
  try {
    const payload: CachedStars = { count, ts: Date.now() };
    window.localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
  } catch {
    // Ignore storage quota / private-browsing failures cleanly.
  }
}

onMounted(async () => {
  const cached = readCachedStars();
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    starCount.value = cached.count;
    return;
  }

  try {
    const response = await fetch(API_URL, {
      headers: { Accept: 'application/vnd.github+json' },
    });
    if (!response.ok) return;
    const data = (await response.json()) as { stargazers_count?: unknown };
    if (typeof data.stargazers_count === 'number') {
      starCount.value = data.stargazers_count;
      writeCachedStars(data.stargazers_count);
    }
  } catch {
    // Offline or rate-limited: fall back cleanly to the count-free pill.
  }
});
</script>

<template>
  <a
    class="gh-star-pill"
    :href="REPO_URL"
    target="_blank"
    rel="noopener noreferrer"
    aria-label="Star ArintonAkos/DataVo-DBMS on GitHub"
  >
    <svg
      class="gh-star-icon"
      viewBox="0 0 16 16"
      width="14"
      height="14"
      aria-hidden="true"
      fill="currentColor"
    >
      <path
        d="M8 .25a.75.75 0 0 1 .673.418l1.882 3.815 4.21.612a.75.75 0 0 1 .416 1.279l-3.046 2.97.719 4.192a.751.751 0 0 1-1.088.791L8 12.347l-3.766 1.98a.75.75 0 0 1-1.088-.79l.72-4.194L.818 6.374a.75.75 0 0 1 .416-1.28l4.21-.611L7.327.668A.75.75 0 0 1 8 .25Z"
      />
    </svg>
    <span class="gh-star-label">Star</span>
    <span
      v-if="starCount !== null && starCount >= minStarsToShowCount"
      class="gh-star-count"
    >
      {{ starCount.toLocaleString('en-US') }}
    </span>
  </a>
</template>

<style scoped>
.gh-star-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  margin-left: 12px;
  padding: 0 10px;
  border: 1px solid var(--vp-c-border);
  border-radius: 999px;
  background-color: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-size: 12.5px;
  font-weight: 500;
  line-height: 1;
  text-decoration: none;
  white-space: nowrap;
  transition:
    border-color 0.2s ease,
    background-color 0.2s ease,
    color 0.2s ease;
}

.gh-star-pill:hover {
  border-color: var(--vp-c-text-2);
  background-color: var(--vp-c-default-soft);
}

.gh-star-icon {
  flex-shrink: 0;
  color: var(--vp-c-text-2);
  transition: color 0.2s ease;
}

.gh-star-pill:hover .gh-star-icon {
  color: var(--vp-c-text-1);
}

.gh-star-count {
  padding-left: 6px;
  border-left: 1px solid var(--vp-c-divider);
  font-family: var(--vp-font-family-mono);
  font-size: 11.5px;
  font-weight: 600;
  color: var(--vp-c-text-2);
}

@media (max-width: 767px) {
  .gh-star-pill {
    display: none;
  }
}
</style>
