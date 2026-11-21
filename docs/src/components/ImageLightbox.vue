<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';

const isOpen = ref(false);
const imgSrc = ref('');
const imgAlt = ref('');
const isSketch = ref(false);

const scale = ref(1);
const translateX = ref(0);
const translateY = ref(0);
const isDragging = ref(false);
let dragStartX = 0;
let dragStartY = 0;
let startTranslateX = 0;
let startTranslateY = 0;

function resetTransform() {
  scale.value = 1;
  translateX.value = 0;
  translateY.value = 0;
}

function openLightbox(target: HTMLImageElement) {
  imgSrc.value = target.currentSrc || target.src;
  imgAlt.value = target.alt || '';
  isSketch.value = Boolean(target.closest('.sketch-diagram'));
  resetTransform();
  isOpen.value = true;
  document.documentElement.style.overflow = 'hidden';
}

function closeLightbox() {
  isOpen.value = false;
  document.documentElement.style.overflow = '';
}

function zoomBy(delta: number) {
  const next = Math.min(4, Math.max(0.5, Number((scale.value + delta).toFixed(2))));
  scale.value = next;
  if (next <= 1) {
    translateX.value = 0;
    translateY.value = 0;
  }
}

function toggleDoubleClick() {
  if (scale.value > 1.05) {
    resetTransform();
  } else {
    scale.value = 2;
  }
}

function onWheel(e: WheelEvent) {
  e.preventDefault();
  zoomBy(e.deltaY < 0 ? 0.2 : -0.2);
}

function onPointerDown(e: PointerEvent) {
  if (scale.value <= 1) return;
  isDragging.value = true;
  dragStartX = e.clientX;
  dragStartY = e.clientY;
  startTranslateX = translateX.value;
  startTranslateY = translateY.value;
  (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
}

function onPointerMove(e: PointerEvent) {
  if (!isDragging.value) return;
  translateX.value = startTranslateX + (e.clientX - dragStartX);
  translateY.value = startTranslateY + (e.clientY - dragStartY);
}

function onPointerUp() {
  isDragging.value = false;
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
  else if (e.key === '+' || e.key === '=') zoomBy(0.25);
  else if (e.key === '-') zoomBy(-0.25);
  else if (e.key === '0') resetTransform();
}

onMounted(() => {
  document.addEventListener('click', onDocumentClick);
  document.addEventListener('keydown', onKeyDown);
});

onUnmounted(() => {
  document.removeEventListener('click', onDocumentClick);
  document.removeEventListener('keydown', onKeyDown);
  document.documentElement.style.overflow = '';
});
</script>

<template>
  <Teleport to="body">
    <div
      v-if="isOpen"
      class="dv-lightbox-backdrop"
      role="dialog"
      aria-modal="true"
      :aria-label="imgAlt || 'Diagram viewer'"
      @click.self="closeLightbox"
      @wheel="onWheel"
    >
      <div class="dv-lightbox-toolbar">
        <span class="dv-lightbox-zoom-label">{{ Math.round(scale * 100) }}%</span>
        <button type="button" class="dv-lb-btn" aria-label="Zoom out" @click="zoomBy(-0.25)">−</button>
        <button type="button" class="dv-lb-btn" aria-label="Reset zoom" @click="resetTransform">Reset</button>
        <button type="button" class="dv-lb-btn" aria-label="Zoom in" @click="zoomBy(0.25)">+</button>
        <button type="button" class="dv-lb-btn dv-lb-close" aria-label="Close" @click="closeLightbox">✕</button>
      </div>

      <div
        class="dv-lightbox-stage"
        :class="{ 'is-sketch': isSketch, 'is-zoomed': scale > 1, 'is-dragging': isDragging }"
        @click.self="closeLightbox"
        @dblclick="toggleDoubleClick"
        @pointerdown="onPointerDown"
        @pointermove="onPointerMove"
        @pointerup="onPointerUp"
        @pointercancel="onPointerUp"
      >
        <img
          :src="imgSrc"
          :alt="imgAlt"
          draggable="false"
          :style="{
            transform: `translate(${translateX}px, ${translateY}px) scale(${scale})`
          }"
        />
      </div>

      <div v-if="imgAlt" class="dv-lightbox-caption">
        {{ imgAlt }} · Scroll or double-click to zoom, drag to pan, Esc to close
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.dv-lightbox-backdrop {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background: rgba(10, 10, 12, 0.86);
  backdrop-filter: blur(4px);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 56px 24px 48px;
}

.dv-lightbox-toolbar {
  position: fixed;
  top: 14px;
  right: 18px;
  z-index: 10000;
  display: flex;
  align-items: center;
  gap: 6px;
  background: rgba(24, 24, 28, 0.92);
  border: 1px solid rgba(255, 255, 255, 0.14);
  border-radius: 8px;
  padding: 5px 8px;
}

.dv-lightbox-zoom-label {
  color: #d4d4d8;
  font-size: 12px;
  font-variant-numeric: tabular-nums;
  min-width: 40px;
  text-align: center;
}

.dv-lb-btn {
  appearance: none;
  border: 1px solid rgba(255, 255, 255, 0.16);
  background: rgba(255, 255, 255, 0.08);
  color: #f4f4f5;
  font-size: 12.5px;
  font-weight: 500;
  padding: 4px 10px;
  border-radius: 5px;
  cursor: pointer;
}

.dv-lb-btn:hover {
  background: rgba(255, 255, 255, 0.18);
}

.dv-lb-close {
  margin-left: 4px;
}

.dv-lightbox-stage {
  max-width: 94vw;
  max-height: 82vh;
  overflow: hidden;
  border-radius: 8px;
  background: #ffffff;
  padding: 14px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: zoom-in;
  touch-action: none;
}

.dv-lightbox-stage.is-sketch {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
}

.dv-lightbox-stage.is-zoomed {
  cursor: grab;
}

.dv-lightbox-stage.is-dragging {
  cursor: grabbing;
}

.dv-lightbox-stage img {
  max-width: 88vw;
  max-height: 76vh;
  width: auto;
  height: auto;
  object-fit: contain;
  user-select: none;
  transition: transform 0.08s ease-out;
}

.dv-lightbox-stage.is-dragging img {
  transition: none;
}

.dv-lightbox-caption {
  position: fixed;
  bottom: 14px;
  left: 50%;
  transform: translateX(-50%);
  color: #e4e4e7;
  background: rgba(24, 24, 28, 0.85);
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: 6px;
  padding: 5px 12px;
  font-size: 12.5px;
  max-width: 90vw;
  text-align: center;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
