<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';

const isOpen = ref(false);
const imgSrc = ref('');
const imgAlt = ref('');
const isSketch = ref(false);
const currentIndex = ref(-1);
const totalImages = ref(0);

const scale = ref(1);
const translateX = ref(0);
const translateY = ref(0);
const isDragging = ref(false);
const isAnimatingZoom = ref(false);

let dragStartX = 0;
let dragStartY = 0;
let startTranslateX = 0;
let startTranslateY = 0;
let didDrag = false;
let pageImages: HTMLImageElement[] = [];

const MIN_SCALE = 0.5;
const MAX_SCALE = 5;

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
  imgSrc.value = target.currentSrc || target.src;
  imgAlt.value = target.alt || '';
  isSketch.value = Boolean(target.closest('.sketch-diagram'));
  resetTransform(false);
}

function openLightbox(target: HTMLImageElement) {
  pageImages = collectPageImages();
  currentIndex.value = pageImages.indexOf(target);
  totalImages.value = pageImages.length;
  loadFromImage(target);
  isOpen.value = true;
  document.documentElement.style.overflow = 'hidden';
}

function closeLightbox() {
  isOpen.value = false;
  isDragging.value = false;
  document.documentElement.style.overflow = '';
}

function stepImage(dir: -1 | 1) {
  if (pageImages.length <= 1 || currentIndex.value < 0) return;
  const nextIdx = (currentIndex.value + dir + pageImages.length) % pageImages.length;
  currentIndex.value = nextIdx;
  loadFromImage(pageImages[nextIdx]);
}

function zoomAt(nextScaleRaw: number, clientX?: number, clientY?: number, animate = false) {
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
    translateX.value = (clientX - cx) - ratio * ((clientX - cx) - translateX.value);
    translateY.value = (clientY - cy) - ratio * ((clientY - cy) - translateY.value);
  }

  scale.value = nextScale;
}

function toggleDoubleClick(e: MouseEvent) {
  if (scale.value > 1.05) {
    resetTransform(true);
  } else {
    zoomAt(2.25, e.clientX, e.clientY, true);
  }
}

function onWheel(e: WheelEvent) {
  e.preventDefault();
  const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
  zoomAt(scale.value * factor, e.clientX, e.clientY, false);
}

function onPointerDown(e: PointerEvent) {
  if (e.button !== 0) return;
  if ((e.target as HTMLElement)?.closest('.dv-lb-hud, .dv-lb-close-top, .dv-lb-nav')) return;
  didDrag = false;
  if (scale.value <= 1) return;
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
  document.removeEventListener('click', onDocumentClick);
  document.removeEventListener('keydown', onKeyDown);
  onPointerUp();
  document.documentElement.style.overflow = '';
});
</script>

<template>
  <Teleport to="body">
    <Transition name="dv-lb">
      <div
        v-if="isOpen"
        class="dv-lb-backdrop"
        role="dialog"
        aria-modal="true"
        :aria-label="imgAlt || 'Diagram viewer'"
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
          :class="{ 'is-zoomed': scale > 1, 'is-dragging': isDragging }"
          @click="onCanvasClick"
          @dblclick="toggleDoubleClick"
          @pointerdown="onPointerDown"
        >
          <div
            class="dv-lb-card"
            :class="{ 'is-sketch': isSketch, 'is-animating': isAnimatingZoom }"
            :style="{
              transform: `translate3d(${translateX}px, ${translateY}px, 0) scale(${scale})`
            }"
          >
            <img
              :src="imgSrc"
              :alt="imgAlt"
              :class="{ 'is-sketch': isSketch }"
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
    </Transition>
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
}

.dv-lb-canvas {
  position: fixed;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 28px 64px 72px;
  cursor: zoom-in;
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
  background: #ffffff;
  border-radius: 10px;
  padding: 16px;
  box-shadow: 0 24px 72px rgba(0, 0, 0, 0.55);
  transform-origin: center center;
}

.dv-lb-card.is-sketch {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
}

.dv-lb-card.is-animating {
  transition: transform 0.2s cubic-bezier(0.16, 1, 0.3, 1);
}

.dv-lb-card img {
  display: block;
  max-width: 90vw;
  max-height: 82vh;
  width: auto;
  height: auto;
  object-fit: contain;
  pointer-events: none;
}

.dv-lb-card img.is-sketch {
  width: min(82vw, 1120px);
  max-height: 80vh;
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

/* Smooth open and close transitions */
.dv-lb-enter-active,
.dv-lb-leave-active {
  transition: opacity 0.22s cubic-bezier(0.16, 1, 0.3, 1);
}

.dv-lb-enter-active .dv-lb-card,
.dv-lb-leave-active .dv-lb-card {
  transition: transform 0.24s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.22s ease;
}

.dv-lb-enter-from,
.dv-lb-leave-to {
  opacity: 0;
}

.dv-lb-enter-from .dv-lb-card,
.dv-lb-leave-to .dv-lb-card {
  transform: scale(0.93) !important;
  opacity: 0;
}

@media (max-width: 640px) {
  .dv-lb-canvas {
    padding: 16px 12px 72px;
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
