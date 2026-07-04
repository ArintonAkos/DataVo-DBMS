<template>
  <figure class="benchmark-chart">
    <figcaption class="benchmark-chart__caption">
      <span class="benchmark-chart__title">{{ title }}</span>
      <span v-if="subtitle" class="benchmark-chart__subtitle">{{ subtitle }}</span>
      <span class="benchmark-chart__scale">{{ scaleLabel }}</span>
    </figcaption>

    <div class="benchmark-chart__plot" @mouseleave="clearActive">
      <svg
        :viewBox="`0 0 ${width} ${height}`"
        role="img"
        :aria-label="ariaLabel"
        class="benchmark-chart__svg"
      >
        <line
          v-for="tick in yTicks"
          :key="`grid-${tick.value}`"
          class="benchmark-chart__grid"
          :x1="plot.left"
          :x2="plot.right"
          :y1="tick.y"
          :y2="tick.y"
        />
        <text
          v-for="tick in yTicks"
          :key="`label-${tick.value}`"
          class="benchmark-chart__axis-label"
          :x="plot.left - 10"
          :y="tick.y + 4"
          text-anchor="end"
        >
          {{ formatCompact(tick.value) }}
        </text>

        <line class="benchmark-chart__axis" :x1="plot.left" :x2="plot.right" :y1="plot.bottom" :y2="plot.bottom" />
        <line class="benchmark-chart__axis" :x1="plot.left" :x2="plot.left" :y1="plot.top" :y2="plot.bottom" />
        <text class="benchmark-chart__axis-title benchmark-chart__axis-title--y" :x="plot.left" :y="24" text-anchor="start">
          {{ yAxisLabel || unit }}
        </text>
        <text class="benchmark-chart__axis-title benchmark-chart__axis-title--x" :x="(plot.left + plot.right) / 2" :y="height - 12" text-anchor="middle">
          {{ xAxisLabel }}
        </text>

        <g v-if="kind === 'line'">
          <path
            v-for="(path, seriesIndex) in linePaths"
            :key="`line-${seriesIndex}`"
            class="benchmark-chart__line"
            :d="path"
            :stroke="seriesColor(seriesIndex)"
          />
          <template v-for="(series, seriesIndex) in normalizedSeries" :key="`line-points-${seriesIndex}`">
            <circle
              v-for="(point, pointIndex) in series.points"
              :key="`point-${seriesIndex}-${pointIndex}`"
              class="benchmark-chart__point"
              :class="{ 'benchmark-chart__point--active': isActive(seriesIndex, pointIndex) }"
              :cx="point.x"
              :cy="point.y"
              r="5"
              :fill="seriesColor(seriesIndex)"
              tabindex="0"
              role="button"
              :aria-label="pointAriaLabel(seriesIndex, pointIndex)"
              @mouseenter="setActive(seriesIndex, pointIndex)"
              @focus="setActive(seriesIndex, pointIndex)"
              @blur="clearActive"
            />
          </template>
        </g>

        <g v-else>
          <template v-for="(series, seriesIndex) in normalizedSeries" :key="`bars-${seriesIndex}`">
            <line
              v-for="(point, pointIndex) in series.points"
              :key="`bar-cap-${seriesIndex}-${pointIndex}`"
              class="benchmark-chart__bar-cap"
              :class="{ 'benchmark-chart__bar-cap--active': isActive(seriesIndex, pointIndex) }"
              :x1="barRect(seriesIndex, pointIndex).x"
              :x2="barRect(seriesIndex, pointIndex).x + barRect(seriesIndex, pointIndex).width"
              :y1="barRect(seriesIndex, pointIndex).y"
              :y2="barRect(seriesIndex, pointIndex).y"
              :stroke="seriesColor(seriesIndex)"
            />
            <rect
              v-for="(point, pointIndex) in series.points"
              :key="`bar-${seriesIndex}-${pointIndex}`"
              class="benchmark-chart__bar"
              :class="{ 'benchmark-chart__bar--active': isActive(seriesIndex, pointIndex) }"
              :x="barRect(seriesIndex, pointIndex).x"
              :y="barRect(seriesIndex, pointIndex).y"
              :width="barRect(seriesIndex, pointIndex).width"
              :height="barRect(seriesIndex, pointIndex).height"
              :fill="seriesColor(seriesIndex)"
              tabindex="0"
              role="button"
              :aria-label="pointAriaLabel(seriesIndex, pointIndex)"
              @mouseenter="setActive(seriesIndex, pointIndex)"
              @focus="setActive(seriesIndex, pointIndex)"
              @blur="clearActive"
            />
          </template>
        </g>

        <text
          v-for="(label, index) in xLabels"
          :key="`x-${label}-${index}`"
          class="benchmark-chart__axis-label benchmark-chart__x-label"
          :x="xPosition(index)"
          :y="plot.bottom + 46"
          text-anchor="middle"
        >
          {{ label }}
        </text>

        <template v-if="showSeriesLabels">
          <text
            v-for="label in seriesLabels"
            :key="`series-label-${label.seriesIndex}-${label.pointIndex}`"
            class="benchmark-chart__axis-label benchmark-chart__series-label"
            :x="label.x"
            :y="plot.bottom + 24"
            text-anchor="middle"
          >
            {{ label.text }}
          </text>
        </template>

        <g v-if="activePoint" class="benchmark-chart__tooltip">
          <rect
            :x="tooltipBox.x"
            :y="tooltipBox.y"
            :width="tooltipBox.width"
            :height="tooltipBox.height"
            rx="6"
          />
          <text :x="tooltipBox.x + 10" :y="tooltipBox.y + 18">
            <tspan class="benchmark-chart__tooltip-title">{{ activePoint.seriesName }}</tspan>
            <tspan :x="tooltipBox.x + 10" dy="17">{{ activePoint.xLabel }}: {{ formatValue(activePoint.value) }} {{ unit }}</tspan>
            <tspan v-if="activePoint.detail" :x="tooltipBox.x + 10" dy="17">{{ activePoint.detail }}</tspan>
          </text>
        </g>
      </svg>
    </div>

    <div class="benchmark-chart__legend" aria-hidden="true">
      <span v-for="(series, index) in normalizedSeries" :key="series.name" class="benchmark-chart__legend-item">
        <span class="benchmark-chart__swatch" :style="{ backgroundColor: seriesColor(index) }"></span>
        {{ series.name }}
      </span>
    </div>

    <p v-if="activePoint" class="benchmark-chart__details" aria-live="polite">
      <strong>{{ activePoint.seriesName }}</strong>,
      {{ activePoint.xLabel }}:
      {{ formatValue(activePoint.value) }} {{ unit }}.
      <span v-if="activePoint.detail">{{ activePoint.detail }}</span>
    </p>
    <p v-else-if="note" class="benchmark-chart__details">{{ note }}</p>
  </figure>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";

type ChartKind = "bar" | "line";
type ScaleKind = "linear" | "log";

interface BenchmarkSeries {
  name: string;
  values: number[];
  color?: string;
  details?: string[];
}

const props = withDefaults(
  defineProps<{
    title: string;
    subtitle?: string;
    kind?: ChartKind;
    scale?: ScaleKind;
    unit: string;
    xAxisLabel?: string;
    yAxisLabel?: string;
    xLabels: string[];
    series: BenchmarkSeries[];
    note?: string;
    min?: number;
    max?: number;
    valueDigits?: number;
  }>(),
  {
    kind: "bar",
    scale: "linear",
    valueDigits: 3,
  },
);

const width = 760;
const height = 410;
const plot = {
  left: 96,
  right: 730,
  top: 58,
  bottom: 302,
};

const palette = ["#2563eb", "#059669", "#d97706", "#dc2626", "#7c3aed", "#0891b2"];
const databaseColors: Array<[RegExp, string]> = [
  [/DataVo-Flat/i, "#059669"],
  [/DataVo.*Production/i, "#1d4ed8"],
  [/DataVo.*Relaxed/i, "#0891b2"],
  [/^DataVo$/i, "#2563eb"],
  [/SQLite/i, "#d97706"],
  [/LiteDB/i, "#7c3aed"],
  [/DuckDB/i, "#dc2626"],
];
const active = ref<{ seriesIndex: number; pointIndex: number } | null>(null);

const values = computed(() => props.series.flatMap((series) => series.values));
const positiveValues = computed(() => values.value.filter((value) => value > 0));
const yMin = computed(() => {
  if (props.min !== undefined) {
    return props.min;
  }

  if (props.scale !== "log") {
    return 0;
  }

  const min = Math.min(...positiveValues.value);
  if (!Number.isFinite(min) || min <= 0) {
    return 1;
  }

  return Math.pow(10, Math.floor(Math.log10(min)) - 1);
});
const yMax = computed(() => {
  if (props.max !== undefined) {
    return props.max;
  }

  const max = Math.max(...values.value, 1);
  return props.scale === "log" ? Math.pow(10, Math.ceil(Math.log10(max))) : max * 1.08;
});

const scaleLabel = computed(() => props.scale === "log" ? "log scale" : "linear scale");
const ariaLabel = computed(() => `${props.title}, ${scaleLabel.value}, ${props.unit}`);

const normalizedSeries = computed(() =>
  props.series.map((series, seriesIndex) => ({
    ...series,
    points: series.values.map((value, pointIndex) => ({
      value,
      x: xPosition(pointIndex),
      y: yPosition(value),
      xLabel: props.xLabels[pointIndex] ?? String(pointIndex + 1),
      detail: series.details?.[pointIndex],
      seriesName: series.name,
      seriesIndex,
      pointIndex,
    })),
  })),
);

const linePaths = computed(() =>
  normalizedSeries.value.map((series) =>
    series.points
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
      .join(" "),
  ),
);

const showSeriesLabels = computed(() => props.kind === "bar" && props.series.length > 1);
const seriesLabels = computed(() => {
  if (!showSeriesLabels.value) {
    return [];
  }

  return normalizedSeries.value.flatMap((series) =>
    series.points.map((point) => ({
      seriesIndex: point.seriesIndex,
      pointIndex: point.pointIndex,
      x: barRect(point.seriesIndex, point.pointIndex).x + barRect(point.seriesIndex, point.pointIndex).width / 2,
      text: compactSeriesName(point.seriesName),
    })),
  );
});

const activePoint = computed(() => {
  if (!active.value) {
    return null;
  }

  return normalizedSeries.value[active.value.seriesIndex]?.points[active.value.pointIndex] ?? null;
});

const tooltipBox = computed(() => {
  const point = activePoint.value;
  if (!point) {
    return { x: 0, y: 0, width: 0, height: 0 };
  }

  const widthValue = point.detail ? 270 : 220;
  const heightValue = point.detail ? 62 : 45;
  const x = Math.min(Math.max(point.x + 12, plot.left), plot.right - widthValue);
  const y = Math.max(point.y - heightValue - 12, plot.top);
  return { x, y, width: widthValue, height: heightValue };
});

const yTicks = computed(() => {
  if (props.scale === "log") {
    const minPower = Math.floor(Math.log10(Math.max(yMin.value, Number.MIN_VALUE)));
    const maxPower = Math.ceil(Math.log10(yMax.value));
    return Array.from({ length: maxPower - minPower + 1 }, (_, index) => {
      const value = Math.pow(10, minPower + index);
      return { value, y: yPosition(value) };
    }).filter((tick) => tick.value >= yMin.value && tick.value <= yMax.value);
  }

  const steps = 5;
  return Array.from({ length: steps + 1 }, (_, index) => {
    const value = yMin.value + ((yMax.value - yMin.value) * index) / steps;
    return { value, y: yPosition(value) };
  });
});

function xPosition(index: number): number {
  const count = Math.max(props.xLabels.length, 1);
  if (props.kind === "bar") {
    const band = (plot.right - plot.left) / count;
    return plot.left + band * index + band / 2;
  }

  if (count === 1) {
    return (plot.left + plot.right) / 2;
  }

  return plot.left + ((plot.right - plot.left) * index) / (count - 1);
}

function yPosition(rawValue: number): number {
  const value = props.scale === "log" ? Math.max(rawValue, yMin.value) : rawValue;
  const ratio = props.scale === "log" ? logRatio(value) : linearRatio(value);
  return plot.bottom - ratio * (plot.bottom - plot.top);
}

function linearRatio(value: number): number {
  const span = yMax.value - yMin.value || 1;
  return (value - yMin.value) / span;
}

function logRatio(value: number): number {
  const min = Math.log10(Math.max(yMin.value, Number.MIN_VALUE));
  const max = Math.log10(Math.max(yMax.value, yMin.value * 10));
  return (Math.log10(Math.max(value, yMin.value)) - min) / (max - min || 1);
}

function barRect(seriesIndex: number, pointIndex: number) {
  const seriesCount = Math.max(props.series.length, 1);
  const count = Math.max(props.xLabels.length, 1);
  const band = (plot.right - plot.left) / count;
  const groupWidth = band * 0.7;
  const gap = Math.min(5, groupWidth * 0.08);
  const barWidth = (groupWidth - gap * (seriesCount - 1)) / seriesCount;
  const groupLeft = plot.left + band * pointIndex + (band - groupWidth) / 2;
  const x = groupLeft + seriesIndex * (barWidth + gap);
  const top = yPosition(props.series[seriesIndex].values[pointIndex]);
  return {
    x,
    y: top,
    width: Math.max(barWidth, 1),
    height: Math.max(plot.bottom - top, 1),
  };
}

function setActive(seriesIndex: number, pointIndex: number): void {
  active.value = { seriesIndex, pointIndex };
}

function clearActive(): void {
  active.value = null;
}

function isActive(seriesIndex: number, pointIndex: number): boolean {
  return active.value?.seriesIndex === seriesIndex && active.value?.pointIndex === pointIndex;
}

function seriesColor(index: number): string {
  const series = props.series[index];
  if (series?.color) {
    return series.color;
  }

  const mapped = databaseColors.find(([pattern]) => pattern.test(series?.name ?? ""));
  return mapped?.[1] ?? palette[index % palette.length];
}

function formatValue(value: number): string {
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: props.valueDigits,
  }).format(value);
}

function formatCompact(value: number): string {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: value >= 1000 ? 1 : 3,
  }).format(value);
}

function compactSeriesName(name: string): string {
  const compact: Array<[RegExp, string]> = [
    [/DataVo.*Production/i, "DataVo prod"],
    [/DataVo.*Relaxed/i, "DataVo relaxed"],
    [/DataVo-Flat/i, "DataVo-Flat"],
    [/SQLite.*normal/i, "SQLite normal"],
    [/SQLite.*full/i, "SQLite full"],
  ];
  const mapped = compact.find(([pattern]) => pattern.test(name));
  if (mapped) {
    return mapped[1];
  }

  return name.replace(/\s+/gu, " ").trim();
}

function pointAriaLabel(seriesIndex: number, pointIndex: number): string {
  const point = normalizedSeries.value[seriesIndex].points[pointIndex];
  return `${point.seriesName}, ${point.xLabel}, ${formatValue(point.value)} ${props.unit}${point.detail ? `. ${point.detail}` : ""}`;
}
</script>

<style scoped>
.benchmark-chart {
  margin: 28px 0;
  padding: 18px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg-soft);
}

.benchmark-chart__caption {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 4px 16px;
  align-items: start;
  margin-bottom: 12px;
}

.benchmark-chart__title {
  color: var(--vp-c-text-1);
  font-size: 16px;
  font-weight: 700;
  line-height: 1.3;
}

.benchmark-chart__subtitle {
  grid-column: 1 / -1;
  color: var(--vp-c-text-2);
  font-size: 13px;
  line-height: 1.45;
}

.benchmark-chart__scale {
  padding: 2px 7px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 999px;
  color: var(--vp-c-text-2);
  font-size: 12px;
  line-height: 1.4;
  white-space: nowrap;
}

.benchmark-chart__plot {
  overflow-x: auto;
}

.benchmark-chart__svg {
  display: block;
  min-width: 640px;
  width: 100%;
  height: auto;
}

.benchmark-chart__grid {
  stroke: var(--vp-c-divider);
  stroke-width: 1;
}

.benchmark-chart__axis {
  stroke: var(--vp-c-text-3);
  stroke-width: 1.2;
}

.benchmark-chart__axis-label,
.benchmark-chart__axis-title {
  fill: var(--vp-c-text-2);
  font-size: 12px;
}

.benchmark-chart__axis-title {
  font-weight: 700;
}

.benchmark-chart__axis-title--y {
  font-size: 11px;
}

.benchmark-chart__axis-title--x {
  font-size: 12px;
}

.benchmark-chart__x-label {
  font-size: 11px;
}

.benchmark-chart__series-label {
  font-size: 10px;
  font-weight: 700;
}

.benchmark-chart__line {
  fill: none;
  stroke-width: 2.5;
}

.benchmark-chart__point,
.benchmark-chart__bar {
  cursor: default;
  outline: none;
  transition: opacity 0.14s ease, stroke-width 0.14s ease, filter 0.14s ease;
}

.benchmark-chart__point {
  stroke: var(--vp-c-bg);
  stroke-width: 2;
}

.benchmark-chart__bar {
  opacity: 0.82;
}

.benchmark-chart__bar-cap {
  pointer-events: none;
  stroke-linecap: round;
  stroke-width: 3;
}

.benchmark-chart__point:hover,
.benchmark-chart__point:focus,
.benchmark-chart__point--active,
.benchmark-chart__bar:hover,
.benchmark-chart__bar:focus,
.benchmark-chart__bar--active,
.benchmark-chart__bar-cap--active {
  opacity: 1;
  stroke: var(--vp-c-text-1);
  stroke-width: 2;
  filter: drop-shadow(0 2px 4px rgb(0 0 0 / 0.2));
}

.benchmark-chart__tooltip rect {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
  stroke-width: 1;
}

.benchmark-chart__tooltip text {
  fill: var(--vp-c-text-1);
  font-size: 12px;
}

.benchmark-chart__tooltip-title {
  font-weight: 700;
}

.benchmark-chart__legend {
  display: flex;
  flex-wrap: wrap;
  gap: 10px 16px;
  margin-top: 10px;
  color: var(--vp-c-text-2);
  font-size: 12px;
}

.benchmark-chart__legend-item {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.benchmark-chart__swatch {
  width: 10px;
  height: 10px;
  border-radius: 2px;
}

.benchmark-chart__details {
  margin: 12px 0 0;
  color: var(--vp-c-text-2);
  font-size: 13px;
  line-height: 1.5;
}

@media (max-width: 700px) {
  .benchmark-chart {
    padding: 14px;
  }

  .benchmark-chart__caption {
    grid-template-columns: 1fr;
  }

  .benchmark-chart__scale {
    justify-self: start;
  }
}
</style>
