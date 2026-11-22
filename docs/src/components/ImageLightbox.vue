<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue';

type LightboxPhase = 'idle' | 'entering' | 'open' | 'closing';

interface BoxMetrics {
  cx: number;
  cy: number;
  w: number;
  h: number;
}

const isOpen = ref(false);
const phase = ref<LightboxPhase>('idle');
const isBackdropVisible = ref(false);

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

const flipTx = ref(0);
const flipTy = ref(0);
const flipSx = ref(1);
const flipSy = ref(1);
const isFlipTransition = ref(false);

const backdropEl = ref<HTMLElement | null>(null);
const cardEl = ref<HTMLElement | null>(null);
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
let previousOverflow = '';
let previousPaddingRight = '';

let openMeasuredBox: BoxMetrics | null = null;
let animRunId = 0;
let enterTimer: number | undefined;
let closeTimer: number | undefined;

const MIN_SCALE = 0.5;
const MAX_SCALE = 5;
const CLICK_ZOOM_SCALE = 1.75;

const cardStyle = computed(() => {
  if (phase.value === 'entering' || phase.value === 'closing') {
    return {
      transform: `translate3d(${flipTx.value}px, ${flipTy.value}px, 0) scale(${flipSx.value}, ${flipSy.value})`,
    };
  }
  return {
    transform: `translate3d(${translateX.value}px, ${translateY.value}px, 0) scale(${scale.value})`,
  };
});

const imgStyle = computed(() => ({
  '--dv-aspect': String(imgAspect.value),
  '--dv-max-w': `${imgMaxWidth.value}px`,
}));

function clearAnimTimers() {
  if (enterTimer !== undefined) {
    window.clearTimeout(enterTimer);
    enterTimer = undefined;
  }
  if (closeTimer !== undefined) {
    window.clearTimeout(closeTimer);
    closeTimer = undefined;
  }
}

function lockScroll() {
  if (isScrollLocked) return;
  const scrollbarWidth = window.innerWidth - document.documentElement.clientWidth;
  const prevClientWidth = document.documentElement.clientWidth;
  previousOverflow = document.documentElement.style.overflow;
  previousPaddingRight = document.documentElement.style.paddingRight;
  document.documentElement.style.overflow = 'hidden';
  if (scrollbarWidth > 0 && document.documentElement.clientWidth > prevClientWidth) {
    document.documentElement.style.paddingRight = `${scrollbarWidth}px`;
  }
  isScrollLocked = true;
}

function unlockScroll() {
  if (!isScrollLocked) return;
  document.documentElement.style.overflow = previousOverflow;
  document.documentElement.style.paddingRight = previousPaddingRight;
  isScrollLocked = false;
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

  clearAnimTimers();
  const runId = ++animRunId;

  pageImages = collectPageImages();
  currentIndex.value = pageImages.indexOf(target);
  totalImages.value = pageImages.length;
  loadFromImage(target);

  previousActiveElement =
    document.activeElement instanceof HTMLElement ? document.activeElement : null;
  lockScroll();

  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  phase.value = 'entering';
  isFlipTransition.value = false;
  isBackdropVisible.value = false;
  flipTx.value = 0;
  flipTy.value = 0;
  flipSx.value = 1;
  flipSy.value = 1;
  isOpen.value = true;

  await nextTick();
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

  setHiddenOrigin(target);
  backdropEl.value?.focus({ preventScroll: true });

  if (prefersReducedMotion || targetRect.width <= 0 || targetRect.height <= 0) {
    requestAnimationFrame(() => {
      if (runId !== animRunId || phase.value !== 'entering') return;
      isBackdropVisible.value = true;
      phase.value = 'open';
    });
    return;
  }

  const originCx = originRect.left + originRect.width / 2;
  const originCy = originRect.top + originRect.height / 2;

  flipTx.value = originCx - targetCx;
  flipTy.value = originCy - targetCy;
  flipSx.value = originRect.width / targetRect.width;
  flipSy.value = originRect.height / targetRect.height;

  await nextTick();
  if (runId !== animRunId || !isOpen.value || !cardEl.value) return;

  // Commit inverted starting frame before enabling CSS FLIP transition.
  cardEl.value.getBoundingClientRect();

  requestAnimationFrame(() => {
    if (runId !== animRunId || phase.value !== 'entering') return;
    isFlipTransition.value = true;
    isBackdropVisible.value = true;
    flipTx.value = 0;
    flipTy.value = 0;
    flipSx.value = 1;
    flipSy.value = 1;

    enterTimer = window.setTimeout(() => {
      if (runId === animRunId && phase.value === 'entering') {
        isFlipTransition.value = false;
        phase.value = 'open';
      }
    }, 310);
  });
}

function finalizeClose() {
  clearAnimTimers();
  onPointerUp();
  isOpen.value = false;
  phase.value = 'idle';
  isBackdropVisible.value = false;
  isFlipTransition.value = false;
  openMeasuredBox = null;
  resetTransform(false);
  restoreOriginVisibility();
  unlockScroll();
  if (previousActiveElement && previousActiveElement.isConnected) {
    previousActiveElement.focus({ preventScroll: true });
  }
  previousActiveElement = null;
}

async function closeLightbox() {
  if (!isOpen.value || phase.value === 'idle' || phase.value === 'closing') return;

  clearAnimTimers();
  onPointerUp();
  const runId = ++animRunId;
  const wasEntering = phase.value === 'entering';
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const currentOrigin = originImg;

  if (prefersReducedMotion || !currentOrigin || !previewImgEl.value || !cardEl.value) {
    phase.value = 'closing';
    isBackdropVisible.value = false;
    closeTimer = window.setTimeout(() => {
      if (runId === animRunId) finalizeClose();
    }, prefersReducedMotion ? 110 : 220);
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

  if (!inViewport) {
    phase.value = 'closing';
    isFlipTransition.value = true;
    flipTx.value = wasEntering ? 0 : translateX.value;
    flipTy.value = wasEntering ? 0 : translateY.value;
    flipSx.value = (wasEntering ? 1 : scale.value) * 0.94;
    flipSy.value = (wasEntering ? 1 : scale.value) * 0.94;
    isBackdropVisible.value = false;
    closeTimer = window.setTimeout(() => {
      if (runId === animRunId) finalizeClose();
    }, 240);
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

  if (wasEntering) {
    phase.value = 'closing';
    isFlipTransition.value = true;
    isBackdropVisible.value = false;
    flipTx.value = targetDx;
    flipTy.value = targetDy;
    flipSx.value = targetSx;
    flipSy.value = targetSy;
    closeTimer = window.setTimeout(() => {
      if (runId === animRunId && phase.value === 'closing') finalizeClose();
    }, 310);
    return;
  }

  // Seed close start frame with exact interactive zoom/pan transform before animating back.
  flipTx.value = translateX.value;
  flipTy.value = translateY.value;
  flipSx.value = scale.value;
  flipSy.value = scale.value;
  isFlipTransition.value = false;
  isAnimatingZoom.value = false;
  phase.value = 'closing';

  await nextTick();
  if (runId !== animRunId || !cardEl.value) return;
  cardEl.value.getBoundingClientRect();

  requestAnimationFrame(() => {
    if (runId !== animRunId || phase.value !== 'closing') return;
    isFlipTransition.value = true;
    isBackdropVisible.value = false;
    flipTx.value = targetDx;
    flipTy.value = targetDy;
    flipSx.value = targetSx;
    flipSy.value = targetSy;

    closeTimer = window.setTimeout(() => {
      if (runId === animRunId && phase.value === 'closing') finalizeClose();
    }, 310);
  });
}

function onCardTransitionEnd(e: TransitionEvent) {
  if (e.target !== cardEl.value || e.propertyName !== 'transform') return;
  if (phase.value === 'entering') {
    clearAnimTimers();
    isFlipTransition.value = false;
    phase.value = 'open';
  } else if (phase.value === 'closing') {
    finalizeClose();
  }
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
  const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
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
  if (e.key === 'Escape') closeLightbox();
  else if (e.key === '+' || e.key === '=') zoomAt(scale.value * 1.25, undefined, undefined, true);
  else if (e.key === '-') zoomAt(scale.value / 1.25, undefined, undefined, true);
  else if (e.key === '0') resetTransform(true);
  else if (e.key === 'ArrowRight') stepImage(1);
  else if (e.key === 'ArrowLeft') stepImage(-1);
}

onMounted(() => {
  document.addEventListener('click', onDocumentClick);
  document.addEventListener('keydown', onKeyDown);
});

onUnmounted(() => {
  clearAnimTimers();
  document.removeEventListener('click', onDocumentClick);
  document.removeEventListener('keydown', onKeyDown);
  onPointerUp();
  restoreOriginVisibility();
  unlockScroll();
});
</script>

<template>
  <Teleport to="body">
    <div
      v-if="isOpen"
      ref="backdropEl"
      class="dv-lb-backdrop"
      :class="{ 'is-visible': isBackdropVisible }"
      role="dialog"
      aria-modal="true"
      :aria-label="imgAlt || 'Diagram viewer'"
      tabindex="-1"
      @wheel="onWheel"
    >
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
            'is-flipping': isFlipTransition,
            'is-animating-zoom': isAnimatingZoom,
            'is-closing': phase === 'closing',
            'is-zoomed': scale > 1.05,
          }"
          :style="cardStyle"
          @click="onCardClick"
          @transitionend="onCardTransitionEnd"
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
  </Teleport>
</template>

<style scoped>
.dv-lb-backdrop {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background: rgba(10, 10, 14, 0.88);
  backdrop-filter: blur(8px);
  overflow: hidden;
  user-select: none;
  outline: none;
  opacity: 0;
  transition: opacity 220ms ease;
}

.dv-lb-backdrop.is-visible {
  opacity: 1;
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
  --dv-card-pad: 16px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  max-width: 92vw;
  max-height: 88vh;
  padding: var(--dv-card-pad);
  box-sizing: border-box;
  background: #ffffff;
  border-radius: 10px;
  box-shadow: 0 24px 72px rgba(0, 0, 0, 0.55);
  transform-origin: center center;
  will-change: transform;
  cursor: zoom-in;
  transition: box-shadow 220ms ease, border-color 220ms ease;
}

.dv-lb-card.is-zoomed {
  cursor: zoom-out;
}

.dv-lb-canvas.is-dragging .dv-lb-card {
  cursor: grabbing;
}

.dv-lb-card.is-sketch {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
}

.dv-lb-card.is-flipping {
  transition:
    transform 280ms cubic-bezier(0.22, 1, 0.36, 1),
    box-shadow 220ms ease,
    border-color 220ms ease;
}

.dv-lb-card.is-animating-zoom {
  transition: transform 200ms cubic-bezier(0.16, 1, 0.3, 1);
}

.dv-lb-card.is-closing {
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.15);
  border-color: transparent;
}

.dv-lb-img {
  display: block;
  width: min(
    calc(92vw - 2 * var(--dv-card-pad)),
    var(--dv-max-w, 1280px),
    calc((88vh - 2 * var(--dv-card-pad)) * var(--dv-aspect))
  );
  height: auto;
  aspect-ratio: var(--dv-aspect);
  max-width: calc(92vw - 2 * var(--dv-card-pad));
  max-height: calc(88vh - 2 * var(--dv-card-pad));
  object-fit: contain;
  pointer-events: none;
}

.dv-lb-close-top,
.dv-lb-nav,
.dv-lb-hud {
  opacity: 0;
  transition: opacity 180ms ease;
}

.dv-lb-backdrop.is-visible .dv-lb-close-top,
.dv-lb-backdrop.is-visible .dv-lb-nav,
.dv-lb-backdrop.is-visible .dv-lb-hud {
  opacity: 1;
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
  transition: opacity 180ms ease, background-color 0.15s ease, transform 0.15s ease;
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
  transition: opacity 180ms ease, background-color 0.15s ease;
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
  .dv-lb-backdrop {
    transition-duration: 100ms;
  }
  .dv-lb-card,
  .dv-lb-card.is-flipping,
  .dv-lb-card.is-animating-zoom {
    transition: none;
  }
}

@media (max-width: 640px) {
  .dv-lb-canvas {
    padding: 12px;
  }
  .dv-lb-card {
    --dv-card-pad: 10px;
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
