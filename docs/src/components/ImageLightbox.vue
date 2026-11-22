<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue';

type LightboxPhase = 'idle' | 'entering' | 'open' | 'closing';

interface BoxMetrics {
  cx: number;
  cy: number;
  w: number;
  h: number;
}

const IOS_EASING = 'cubic-bezier(0.32, 0.72, 0, 1)';
const OPEN_DURATION = 340;
const CLOSE_DURATION = 300;
const MIN_SCALE = 0.5;
const MAX_SCALE = 5;
const CLICK_ZOOM_SCALE = 1.75;

const isOpen = ref(false);
const isCardReady = ref(false);
const phase = ref<LightboxPhase>('idle');

const imgSrc = ref('');
const imgAlt = ref('');
const isSketch = ref(false);
const imgAspect = ref(16 / 9);
const imgMaxWidth = ref(1200);
const currentIndex = ref(-1);
const totalImages = ref(0);

const scale = ref(1);
const translateX = ref(0);
const translateY = ref(0);
const isDragging = ref(false);
const isAnimatingZoom = ref(false);

const rootEl = ref<HTMLElement | null>(null);
const scrimEl = ref<HTMLElement | null>(null);
const cardEl = ref<HTMLElement | null>(null);
const chromeEl = ref<HTMLElement | null>(null);
const previewImgEl = ref<HTMLImageElement | null>(null);

let dragStartX = 0;
let dragStartY = 0;
let startTranslateX = 0;
let startTranslateY = 0;
let didDrag = false;
let pageImages: HTMLImageElement[] = [];

let originImg: HTMLImageElement | null = null;
let originPrevVisibility = '';
let previousActiveElement: HTMLElement | null = null;

let isScrollLocked = false;
let lockedScrollX = 0;
let lockedScrollY = 0;

let openMeasuredBox: BoxMetrics | null = null;
let animRunId = 0;
let activeCardAnim: Animation | null = null;
let activeScrimAnim: Animation | null = null;
let activeChromeAnim: Animation | null = null;

const cardStyle = computed(() => ({
  opacity: isCardReady.value ? 1 : 0,
  transform: `translate3d(${translateX.value}px, ${translateY.value}px, 0) scale(${scale.value})`,
}));

const imgStyle = computed(() => ({
  '--dv-aspect': String(imgAspect.value),
  '--dv-max-w': `${imgMaxWidth.value}px`,
}));

function cancelWaapi() {
  if (activeCardAnim) {
    activeCardAnim.cancel();
    activeCardAnim = null;
  }
  if (activeScrimAnim) {
    activeScrimAnim.cancel();
    activeScrimAnim = null;
  }
  if (activeChromeAnim) {
    activeChromeAnim.cancel();
    activeChromeAnim = null;
  }
}

function onWindowScroll() {
  if (!isScrollLocked) return;
  if (window.scrollX !== lockedScrollX || window.scrollY !== lockedScrollY) {
    window.scrollTo({ left: lockedScrollX, top: lockedScrollY, behavior: 'instant' as ScrollBehavior });
  }
}

function preventTouchMove(e: TouchEvent) {
  if (!isScrollLocked) return;
  e.preventDefault();
}

function lockScroll() {
  if (isScrollLocked) return;
  lockedScrollX = window.scrollX;
  lockedScrollY = window.scrollY;
  isScrollLocked = true;
  window.addEventListener('scroll', onWindowScroll, { passive: true });
  window.addEventListener('touchmove', preventTouchMove, { passive: false });
}

function unlockScroll() {
  if (!isScrollLocked) return;
  isScrollLocked = false;
  window.removeEventListener('scroll', onWindowScroll);
  window.removeEventListener('touchmove', preventTouchMove);
}

function restoreOriginVisibility() {
  if (!originImg) return;
  originImg.style.visibility = originPrevVisibility;
  originImg = null;
  originPrevVisibility = '';
}

function setHiddenOrigin(target: HTMLImageElement) {
  if (originImg === target) return;
  restoreOriginVisibility();
  originImg = target;
  originPrevVisibility = target.style.visibility;
  target.style.visibility = 'hidden';
}

function collectPageImages(): HTMLImageElement[] {
  return Array.from(document.querySelectorAll<HTMLImageElement>('.vp-doc img')).filter((img) => {
    if (img.closest('a')) return false;
    const style = window.getComputedStyle(img);
    if (style.display === 'none') return false;
    const sketchWrapper = img.closest('.light-only, .dark-only');
    if (sketchWrapper && window.getComputedStyle(sketchWrapper).display === 'none') return false;
    return true;
  });
}

function resetTransform(animate = true) {
  isAnimatingZoom.value = animate;
  scale.value = 1;
  translateX.value = 0;
  translateY.value = 0;
}

function loadFromImage(target: HTMLImageElement) {
  const rect = target.getBoundingClientRect();
  const natW = target.naturalWidth || rect.width || 780;
  const natH = target.naturalHeight || rect.height || 440;
  const sketch = Boolean(target.closest('.sketch-diagram'));

  imgSrc.value = target.currentSrc || target.src;
  imgAlt.value = target.alt || '';
  isSketch.value = sketch;
  imgAspect.value = natW > 0 && natH > 0 ? natW / natH : 16 / 9;
  imgMaxWidth.value = sketch ? 1200 : Math.max(natW, Math.round(rect.width * 1.6), 960);
  resetTransform(false);
}

async function openLightbox(target: HTMLImageElement) {
  const originRect = target.getBoundingClientRect();
  if (originRect.width <= 0 || originRect.height <= 0) return;

  cancelWaapi();
  const runId = ++animRunId;

  pageImages = collectPageImages();
  currentIndex.value = pageImages.indexOf(target);
  totalImages.value = pageImages.length;
  loadFromImage(target);

  previousActiveElement =
    document.activeElement instanceof HTMLElement &&
    document.activeElement !== document.body &&
    document.activeElement !== document.documentElement
      ? document.activeElement
      : null;
  lockScroll();

  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  phase.value = 'entering';
  isCardReady.value = false;
  isOpen.value = true;

  await nextTick();
  if (runId !== animRunId || !isOpen.value || !previewImgEl.value || !cardEl.value) return;

  // Ensure the bitmap / SVG is decoded in memory before swapping out the inline thumbnail.
  if (typeof previewImgEl.value.decode === 'function') {
    await previewImgEl.value.decode().catch(() => {});
  }
  if (runId !== animRunId || !isOpen.value || !previewImgEl.value || !cardEl.value) return;

  const targetRect = previewImgEl.value.getBoundingClientRect();
  const targetCx = targetRect.left + targetRect.width / 2;
  const targetCy = targetRect.top + targetRect.height / 2;
  openMeasuredBox = {
    cx: targetCx,
    cy: targetCy,
    w: targetRect.width,
    h: targetRect.height,
  };

  if (prefersReducedMotion || targetRect.width <= 0 || targetRect.height <= 0) {
    isCardReady.value = true;
    setHiddenOrigin(target);
    rootEl.value?.focus({ preventScroll: true });
    if (scrimEl.value) {
      activeScrimAnim = scrimEl.value.animate([{ opacity: 0 }, { opacity: 1 }], {
        duration: 120,
        easing: 'ease-out',
        fill: 'forwards',
      });
    }
    if (chromeEl.value) {
      activeChromeAnim = chromeEl.value.animate([{ opacity: 0 }, { opacity: 1 }], {
        duration: 120,
        easing: 'ease-out',
        fill: 'forwards',
      });
    }
    phase.value = 'open';
    return;
  }

  const originCx = originRect.left + originRect.width / 2;
  const originCy = originRect.top + originRect.height / 2;
  const dx = originCx - targetCx;
  const dy = originCy - targetCy;
  const sx = originRect.width / targetRect.width;
  const sy = originRect.height / targetRect.height;

  // Start compositor WAAPI transform from originRect BEFORE revealing card & hiding inline thumbnail.
  if (scrimEl.value) {
    activeScrimAnim = scrimEl.value.animate([{ opacity: 0 }, { opacity: 1 }], {
      duration: 280,
      easing: IOS_EASING,
      fill: 'forwards',
    });
  }
  if (chromeEl.value) {
    activeChromeAnim = chromeEl.value.animate([{ opacity: 0 }, { opacity: 1 }], {
      duration: 220,
      delay: 40,
      easing: IOS_EASING,
      fill: 'both',
    });
  }

  const cardAnim = cardEl.value.animate(
    [
      {
        transform: `translate3d(${dx}px, ${dy}px, 0) scale(${sx}, ${sy})`,
        boxShadow: '0 0 0 rgba(0, 0, 0, 0)',
      },
      {
        transform: 'translate3d(0px, 0px, 0) scale(1, 1)',
        boxShadow: '0 24px 72px rgba(0, 0, 0, 0.55)',
      },
    ],
    {
      duration: OPEN_DURATION,
      easing: IOS_EASING,
      fill: 'forwards',
    },
  );
  activeCardAnim = cardAnim;
  isCardReady.value = true;
  setHiddenOrigin(target);
  rootEl.value?.focus({ preventScroll: true });

  try {
    await cardAnim.finished;
  } catch {
    return;
  }
  if (runId !== animRunId || phase.value !== 'entering') return;

  cardAnim.cancel();
  if (activeCardAnim === cardAnim) activeCardAnim = null;
  phase.value = 'open';
}

function finalizeClose() {
  onPointerUp();
  restoreOriginVisibility();
  isOpen.value = false;
  isCardReady.value = false;
  phase.value = 'idle';
  openMeasuredBox = null;
  resetTransform(false);
  unlockScroll();
  if (previousActiveElement && previousActiveElement.isConnected) {
    previousActiveElement.focus({ preventScroll: true });
  }
  previousActiveElement = null;
  activeCardAnim = null;
  activeScrimAnim = null;
  activeChromeAnim = null;
}

async function closeLightbox() {
  if (!isOpen.value || phase.value === 'idle' || phase.value === 'closing') return;

  onPointerUp();
  const runId = ++animRunId;
  const wasEntering = phase.value === 'entering';
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const currentOrigin = originImg;

  let liveTransform = `translate3d(${translateX.value}px, ${translateY.value}px, 0) scale(${scale.value})`;
  if (wasEntering && cardEl.value) {
    const computedTransform = window.getComputedStyle(cardEl.value).transform;
    if (computedTransform && computedTransform !== 'none') {
      liveTransform = computedTransform;
    }
  }
  const liveScrimOpacity = scrimEl.value
    ? Number(window.getComputedStyle(scrimEl.value).opacity) || 1
    : 1;
  const liveChromeOpacity = chromeEl.value
    ? Number(window.getComputedStyle(chromeEl.value).opacity) || 1
    : 1;

  cancelWaapi();
  phase.value = 'closing';

  if (chromeEl.value) {
    activeChromeAnim = chromeEl.value.animate([{ opacity: liveChromeOpacity }, { opacity: 0 }], {
      duration: prefersReducedMotion ? 100 : 140,
      easing: 'ease-out',
      fill: 'forwards',
    });
  }

  if (prefersReducedMotion || !currentOrigin || !previewImgEl.value || !cardEl.value) {
    if (scrimEl.value) {
      activeScrimAnim = scrimEl.value.animate([{ opacity: liveScrimOpacity }, { opacity: 0 }], {
        duration: prefersReducedMotion ? 110 : 200,
        easing: 'ease-out',
        fill: 'forwards',
      });
    }
    const fadeAnim = cardEl.value?.animate([{ opacity: 1 }, { opacity: 0 }], {
      duration: prefersReducedMotion ? 110 : 200,
      easing: 'ease-out',
      fill: 'forwards',
    });
    activeCardAnim = fadeAnim ?? null;
    try {
      await fadeAnim?.finished;
    } catch {
      return;
    }
    if (runId === animRunId) finalizeClose();
    return;
  }

  const originRect = currentOrigin.getBoundingClientRect();
  const inViewport =
    originRect.width > 0 &&
    originRect.height > 0 &&
    originRect.bottom > 0 &&
    originRect.top < window.innerHeight &&
    originRect.right > 0 &&
    originRect.left < window.innerWidth;

  if (scrimEl.value) {
    activeScrimAnim = scrimEl.value.animate([{ opacity: liveScrimOpacity }, { opacity: 0 }], {
      duration: 240,
      easing: IOS_EASING,
      fill: 'forwards',
    });
  }

  if (!inViewport) {
    const offscreenAnim = cardEl.value.animate(
      [
        { transform: liveTransform, opacity: 1 },
        { transform: `translate3d(${translateX.value}px, ${translateY.value}px, 0) scale(${scale.value * 0.94})`, opacity: 0 },
      ],
      { duration: 220, easing: IOS_EASING, fill: 'forwards' },
    );
    activeCardAnim = offscreenAnim;
    try {
      await offscreenAnim.finished;
    } catch {
      return;
    }
    if (runId === animRunId) finalizeClose();
    return;
  }

  let baseBox: BoxMetrics;
  if (wasEntering && openMeasuredBox) {
    baseBox = openMeasuredBox;
  } else {
    const rectNow = previewImgEl.value.getBoundingClientRect();
    const curScale = scale.value || 1;
    baseBox = {
      w: rectNow.width / curScale,
      h: rectNow.height / curScale,
      cx: rectNow.left + rectNow.width / 2 - translateX.value,
      cy: rectNow.top + rectNow.height / 2 - translateY.value,
    };
  }

  const destCx = originRect.left + originRect.width / 2;
  const destCy = originRect.top + originRect.height / 2;
  const targetDx = destCx - baseBox.cx;
  const targetDy = destCy - baseBox.cy;
  const targetSx = originRect.width / baseBox.w;
  const targetSy = originRect.height / baseBox.h;

  isAnimatingZoom.value = false;
  const closeAnim = cardEl.value.animate(
    [
      {
        transform: liveTransform,
        boxShadow: '0 24px 72px rgba(0, 0, 0, 0.55)',
      },
      {
        transform: `translate3d(${targetDx}px, ${targetDy}px, 0) scale(${targetSx}, ${targetSy})`,
        boxShadow: '0 0 0 rgba(0, 0, 0, 0)',
      },
    ],
    {
      duration: CLOSE_DURATION,
      easing: IOS_EASING,
      fill: 'forwards',
    },
  );
  activeCardAnim = closeAnim;

  try {
    await closeAnim.finished;
  } catch {
    return;
  }
  if (runId !== animRunId || phase.value !== 'closing') return;

  finalizeClose();
}

async function stepImage(dir: -1 | 1) {
  if (pageImages.length <= 1 || currentIndex.value < 0 || phase.value !== 'open') return;
  const nextIdx = (currentIndex.value + dir + pageImages.length) % pageImages.length;
  currentIndex.value = nextIdx;
  const nextTarget = pageImages[nextIdx];
  loadFromImage(nextTarget);
  setHiddenOrigin(nextTarget);
  await nextTick();
  if (previewImgEl.value) {
    if (typeof previewImgEl.value.decode === 'function') {
      await previewImgEl.value.decode().catch(() => {});
    }
    const r = previewImgEl.value.getBoundingClientRect();
    openMeasuredBox = {
      cx: r.left + r.width / 2,
      cy: r.top + r.height / 2,
      w: r.width,
      h: r.height,
    };
  }
}

function zoomAt(nextScaleRaw: number, clientX?: number, clientY?: number, animate = false) {
  if (phase.value !== 'open') return;
  const nextScale = Math.min(MAX_SCALE, Math.max(MIN_SCALE, Number(nextScaleRaw.toFixed(3))));
  const prevScale = scale.value;
  if (nextScale === prevScale) return;

  isAnimatingZoom.value = animate;

  if (nextScale <= 1 && Math.abs(nextScale - 1) < 0.01) {
    scale.value = 1;
    translateX.value = 0;
    translateY.value = 0;
    return;
  }

  if (clientX !== undefined && clientY !== undefined) {
    const cx = window.innerWidth / 2;
    const cy = window.innerHeight / 2;
    const ratio = nextScale / prevScale;
    translateX.value = clientX - cx - ratio * (clientX - cx - translateX.value);
    translateY.value = clientY - cy - ratio * (clientY - cy - translateY.value);
  }

  scale.value = nextScale;
}

function onCardClick(e: MouseEvent) {
  if (phase.value !== 'open') return;
  if (didDrag) return;
  e.stopPropagation();
  if (scale.value > 1.05) {
    resetTransform(true);
  } else {
    zoomAt(CLICK_ZOOM_SCALE, e.clientX, e.clientY, true);
  }
}

function onWheel(e: WheelEvent) {
  e.preventDefault();
  if (phase.value !== 'open') return;
  const sensitivity = e.ctrlKey ? 0.01 : 0.0025;
  const factor = Math.exp(-e.deltaY * sensitivity);
  zoomAt(scale.value * factor, e.clientX, e.clientY, false);
}

function onPointerDown(e: PointerEvent) {
  if (e.button !== 0 || phase.value !== 'open') return;
  if ((e.target as HTMLElement)?.closest('.dv-lb-hud, .dv-lb-close-top, .dv-lb-nav')) return;
  didDrag = false;
  if (scale.value <= 1.05) return;
  isDragging.value = true;
  isAnimatingZoom.value = false;
  dragStartX = e.clientX;
  dragStartY = e.clientY;
  startTranslateX = translateX.value;
  startTranslateY = translateY.value;
  window.addEventListener('pointermove', onPointerMove);
  window.addEventListener('pointerup', onPointerUp);
  window.addEventListener('pointercancel', onPointerUp);
}

function onPointerMove(e: PointerEvent) {
  if (!isDragging.value) return;
  const dx = e.clientX - dragStartX;
  const dy = e.clientY - dragStartY;
  if (Math.hypot(dx, dy) > 4) didDrag = true;
  translateX.value = startTranslateX + dx;
  translateY.value = startTranslateY + dy;
}

function onPointerUp() {
  isDragging.value = false;
  window.removeEventListener('pointermove', onPointerMove);
  window.removeEventListener('pointerup', onPointerUp);
  window.removeEventListener('pointercancel', onPointerUp);
  if (didDrag) {
    window.setTimeout(() => {
      didDrag = false;
    }, 0);
  }
}

function onCanvasClick(e: MouseEvent) {
  if (didDrag) {
    didDrag = false;
    return;
  }
  const target = e.target as HTMLElement | null;
  if (!target || target.closest('.dv-lb-card')) return;
  if (target.classList.contains('dv-lb-canvas')) {
    closeLightbox();
  }
}

function onDocumentClick(e: MouseEvent) {
  const target = e.target as HTMLElement | null;
  if (!target || target.tagName !== 'IMG') return;
  if (!target.closest('.vp-doc')) return;
  if (target.closest('a')) return;
  e.preventDefault();
  if (isOpen.value) {
    finalizeClose();
  }
  openLightbox(target as HTMLImageElement);
}

function onKeyDown(e: KeyboardEvent) {
  if (!isOpen.value) return;
  if (e.key === 'Escape') {
    e.preventDefault();
    closeLightbox();
  } else if (e.key === '+' || e.key === '=') {
    zoomAt(scale.value * 1.25, undefined, undefined, true);
  } else if (e.key === '-') {
    zoomAt(scale.value / 1.25, undefined, undefined, true);
  } else if (e.key === '0') {
    resetTransform(true);
  } else if (e.key === 'ArrowRight') {
    stepImage(1);
  } else if (e.key === 'ArrowLeft') {
    stepImage(-1);
  } else if (
    e.key === ' ' ||
    e.key === 'PageUp' ||
    e.key === 'PageDown' ||
    e.key === 'Home' ||
    e.key === 'End' ||
    e.key === 'ArrowUp' ||
    e.key === 'ArrowDown'
  ) {
    e.preventDefault();
  }
}

onMounted(() => {
  document.addEventListener('click', onDocumentClick);
  document.addEventListener('keydown', onKeyDown);
});

onUnmounted(() => {
  document.removeEventListener('click', onDocumentClick);
  document.removeEventListener('keydown', onKeyDown);
  finalizeClose();
});
</script>

<template>
  <Teleport to="body">
    <div
      v-if="isOpen"
      ref="rootEl"
      class="dv-lb-root"
      role="dialog"
      aria-modal="true"
      :aria-label="imgAlt || 'Diagram viewer'"
      tabindex="-1"
      @wheel.prevent="onWheel"
    >
      <div
        ref="scrimEl"
        class="dv-lb-scrim"
        aria-hidden="true"
      />

      <div
        class="dv-lb-canvas"
        :class="{ 'is-zoomed': scale > 1.05, 'is-dragging': isDragging }"
        @click="onCanvasClick"
        @pointerdown="onPointerDown"
      >
        <div
          ref="cardEl"
          class="dv-lb-card"
          :class="{
            'is-sketch': isSketch,
            'is-animating-zoom': isAnimatingZoom,
            'is-zoomed': scale > 1.05,
          }"
          :style="cardStyle"
          @click="onCardClick"
        >
          <img
            ref="previewImgEl"
            :src="imgSrc"
            :alt="imgAlt"
            class="dv-lb-img"
            :class="{ 'is-sketch': isSketch }"
            :style="imgStyle"
            draggable="false"
          />
        </div>
      </div>

      <div ref="chromeEl" class="dv-lb-chrome">
        <button
          type="button"
          class="dv-lb-close-top"
          aria-label="Close"
          @click="closeLightbox"
        >
          ✕
        </button>

        <button
          v-if="totalImages > 1"
          type="button"
          class="dv-lb-nav dv-lb-prev"
          aria-label="Previous diagram"
          @click="stepImage(-1)"
        >
          ‹
        </button>
        <button
          v-if="totalImages > 1"
          type="button"
          class="dv-lb-nav dv-lb-next"
          aria-label="Next diagram"
          @click="stepImage(1)"
        >
          ›
        </button>

        <div class="dv-lb-hud">
          <div class="dv-lb-controls">
            <button
              type="button"
              class="dv-lb-btn"
              aria-label="Zoom out"
              @click="zoomAt(scale / 1.25, undefined, undefined, true)"
            >
              −
            </button>
            <button
              type="button"
              class="dv-lb-btn dv-lb-pct"
              title="Reset zoom (0)"
              @click="resetTransform(true)"
            >
              {{ Math.round(scale * 100) }}%
            </button>
            <button
              type="button"
              class="dv-lb-btn"
              aria-label="Zoom in"
              @click="zoomAt(scale * 1.25, undefined, undefined, true)"
            >
              +
            </button>
          </div>
          <span v-if="imgAlt" class="dv-lb-sep" aria-hidden="true"></span>
          <span v-if="imgAlt" class="dv-lb-caption">{{ imgAlt }}</span>
          <span v-if="totalImages > 1" class="dv-lb-count">{{ currentIndex + 1 }} / {{ totalImages }}</span>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.dv-lb-root {
  position: fixed;
  inset: 0;
  z-index: 9999;
  overflow: hidden;
  user-select: none;
  outline: none;
  overscroll-behavior: contain;
}

.dv-lb-scrim {
  position: fixed;
  inset: 0;
  background: rgba(10, 10, 14, 0.9);
  pointer-events: none;
  will-change: opacity;
}

.dv-lb-canvas {
  position: fixed;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
  cursor: zoom-out;
  touch-action: none;
}

.dv-lb-canvas.is-zoomed {
  cursor: grab;
}

.dv-lb-canvas.is-dragging {
  cursor: grabbing;
}

.dv-lb-card {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  max-width: 92vw;
  max-height: 88vh;
  padding: 0;
  background: #ffffff;
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 24px 72px rgba(0, 0, 0, 0.55);
  transform-origin: center center;
  will-change: transform;
  cursor: zoom-in;
}

.dv-lb-card.is-zoomed {
  cursor: zoom-out;
}

.dv-lb-canvas.is-dragging .dv-lb-card {
  cursor: grabbing;
}

.dv-lb-card.is-sketch {
  background: var(--vp-c-bg);
}

.dv-lb-card.is-animating-zoom {
  transition: transform 240ms cubic-bezier(0.32, 0.72, 0, 1);
}

.dv-lb-img {
  display: block;
  width: min(
    92vw,
    var(--dv-max-w, 1280px),
    calc(88vh * var(--dv-aspect))
  );
  height: auto;
  aspect-ratio: var(--dv-aspect);
  max-width: 92vw;
  max-height: 88vh;
  object-fit: contain;
  pointer-events: none;
}

.dv-lb-chrome {
  opacity: 0;
  pointer-events: none;
}

.dv-lb-close-top,
.dv-lb-nav,
.dv-lb-hud {
  pointer-events: auto;
}

.dv-lb-close-top {
  position: fixed;
  top: 16px;
  right: 18px;
  z-index: 10001;
  width: 36px;
  height: 36px;
  border-radius: 50%;
  border: 1px solid rgba(255, 255, 255, 0.18);
  background: rgba(24, 24, 28, 0.78);
  color: #f4f4f5;
  font-size: 16px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background-color 0.15s ease, transform 0.15s ease;
}

.dv-lb-close-top:hover {
  background: rgba(255, 255, 255, 0.2);
  transform: scale(1.06);
}

.dv-lb-nav {
  position: fixed;
  top: 50%;
  transform: translateY(-50%);
  z-index: 10001;
  width: 40px;
  height: 52px;
  border-radius: 8px;
  border: 1px solid rgba(255, 255, 255, 0.14);
  background: rgba(24, 24, 28, 0.72);
  color: #f4f4f5;
  font-size: 26px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background-color 0.15s ease;
}

.dv-lb-nav:hover {
  background: rgba(255, 255, 255, 0.18);
}

.dv-lb-prev {
  left: 14px;
}

.dv-lb-next {
  right: 14px;
}

.dv-lb-hud {
  position: fixed;
  bottom: 16px;
  left: 50%;
  transform: translateX(-50%);
  z-index: 10001;
  display: flex;
  align-items: center;
  gap: 10px;
  max-width: 92vw;
  padding: 6px 12px;
  border-radius: 999px;
  background: rgba(24, 24, 28, 0.86);
  border: 1px solid rgba(255, 255, 255, 0.14);
  color: #e4e4e7;
  box-shadow: 0 8px 28px rgba(0, 0, 0, 0.4);
}

.dv-lb-controls {
  display: flex;
  align-items: center;
  gap: 4px;
  flex-shrink: 0;
}

.dv-lb-btn {
  appearance: none;
  border: 1px solid rgba(255, 255, 255, 0.14);
  background: rgba(255, 255, 255, 0.08);
  color: #f4f4f5;
  font-size: 13px;
  font-weight: 500;
  height: 26px;
  padding: 0 9px;
  border-radius: 999px;
  cursor: pointer;
  transition: background-color 0.12s ease;
}

.dv-lb-btn:hover {
  background: rgba(255, 255, 255, 0.2);
}

.dv-lb-pct {
  font-variant-numeric: tabular-nums;
  min-width: 52px;
}

.dv-lb-sep {
  width: 1px;
  height: 16px;
  background: rgba(255, 255, 255, 0.18);
  flex-shrink: 0;
}

.dv-lb-caption {
  font-size: 12.5px;
  color: #d4d4d8;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.dv-lb-count {
  font-size: 11.5px;
  color: #a1a1aa;
  font-variant-numeric: tabular-nums;
  flex-shrink: 0;
}

@media (prefers-reduced-motion: reduce) {
  .dv-lb-card.is-animating-zoom,
  .dv-lb-chrome {
    transition: none;
  }
}

@media (max-width: 640px) {
  .dv-lb-canvas {
    padding: 12px;
  }
  .dv-lb-nav {
    display: none;
  }
  .dv-lb-caption {
    display: none;
  }
  .dv-lb-sep {
    display: none;
  }
}
</style>
