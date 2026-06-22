<template>
  <div id="app" class="dashboard">
    <header class="header">
      <button
        class="settings-btn"
        @click="
          pendingTimeFrame = timeFrameMinutes;
          pendingWindSpeedUnit = windSpeedUnit;
          pendingOldestAtTop = directionOldestAtTop;
          pendingExclusionWindowSec = exclusionWindowSec;
          pendingHideInvalidData = hideInvalidData;
          pendingAccelThresholdMs2 = accelThresholdMs2;
          pendingHeadingRateThresholdDegs = headingRateThresholdDegs;
          pendingAvgWindowMinutes = avgWindowMinutes;
          settingsOpen = true;
        "
        aria-label="Open settings"
      >
        ⚙
      </button>
      <div v-if="error" class="error-banner">{{ error }}</div>
    </header>

    <!-- Settings popup -->
    <div
      v-if="settingsOpen"
      class="settings-overlay"
      @click.self="settingsOpen = false"
    >
      <div class="settings-panel">
        <div class="settings-header">
          <h2>Settings</h2>
          <button
            class="settings-close"
            @click="settingsOpen = false"
            aria-label="Close settings"
          >
            ✕
          </button>
        </div>

        <div class="settings-row">
          <label for="timeframe-select">Time frame</label>
          <select id="timeframe-select" v-model.number="pendingTimeFrame">
            <option
              v-for="m in [5, 10, 15, 30, 45, 60, 75, 90]"
              :key="m"
              :value="m"
            >
              {{ m }} min
            </option>
          </select>
        </div>

        <div class="settings-row">
          <label for="speed-unit-select">Wind speed unit</label>
          <select id="speed-unit-select" v-model="pendingWindSpeedUnit">
            <option value="mps">Meters per second (m/s)</option>
            <option value="knots">Knots (kn)</option>
            <option value="kmh">Kilometres per hour (km/h)</option>
          </select>
        </div>

        <div class="settings-row">
          <label for="dir-order-toggle">Oldest at top</label>
          <input
            id="dir-order-toggle"
            type="checkbox"
            v-model="pendingOldestAtTop"
          />
        </div>

        <div class="settings-row">
          <label for="exclusion-window-select">Exclusion window</label>
          <select
            id="exclusion-window-select"
            v-model.number="pendingExclusionWindowSec"
          >
            <option :value="0">Off</option>
            <option v-for="s in [2, 5, 10, 15, 20, 30]" :key="s" :value="s">
              {{ s }} s
            </option>
          </select>
        </div>

        <div class="settings-row">
          <label for="hide-invalid-toggle">Hide invalid data</label>
          <input
            id="hide-invalid-toggle"
            type="checkbox"
            v-model="pendingHideInvalidData"
          />
        </div>

        <div class="settings-row">
          <label for="accel-threshold-input">Accel threshold (m/s²)</label>
          <input
            id="accel-threshold-input"
            type="number"
            min="0"
            step="0.05"
            v-model.number="pendingAccelThresholdMs2"
          />
        </div>

        <div class="settings-row">
          <label for="heading-threshold-input"
            >Heading rate threshold (°/s)</label
          >
          <input
            id="heading-threshold-input"
            type="number"
            min="0"
            step="0.5"
            v-model.number="pendingHeadingRateThresholdDegs"
          />
        </div>

        <div class="settings-row">
          <label for="avg-window-select">Average window</label>
          <select
            id="avg-window-select"
            v-model.number="pendingAvgWindowMinutes"
          >
            <option
              v-for="m in [5, 10, 15, 30, 45, 60]"
              :key="m"
              :value="m"
            >
              {{ m }} min
            </option>
          </select>
        </div>

        <div class="settings-footer">
          <button class="apply-btn" @click="applySettings">Apply</button>
        </div>
      </div>
    </div>

    <main v-if="!loading" class="content">
      <div class="charts">
        <div class="left-column">
          <div class="kpi-cards">
            <div class="kpi-card">
              <div class="label">True Wind Speed</div>
              <div class="value">
                {{
                  latestMeasurement
                    ? convertWindSpeed(latestMeasurement.true_wind_speed_mps).toFixed(
                        1
                      )
                    : "—"
                }}
                {{ windSpeedUnitLabel }}
              </div>
            </div>
            <div class="kpi-card">
              <div class="label">True Wind Direction</div>
              <div class="value">
                {{ latestMeasurement?.true_wind_dir_deg?.toFixed(0) || "—" }}°
              </div>
            </div>
            <div class="kpi-card">
              <div class="label">Avg True Wind Speed ({{ avgWindow }})</div>
              <div class="value">
                {{
                  averageMeasurement
                    ? convertWindSpeed(
                        averageMeasurement.true_wind_speed_mps
                      ).toFixed(1)
                    : "—"
                }}
                {{ windSpeedUnitLabel }}
              </div>
            </div>
            <div class="kpi-card">
              <div class="label">Avg True Wind Direction ({{ avgWindow }})</div>
              <div class="value">{{ averageMeasurement?.true_wind_dir_deg?.toFixed(0) || "—" }}°</div>
            </div>
          </div>
          <div id="wind-speed-chart" class="chart-container"></div>
        </div>
        <div id="wind-direction-chart" class="chart-container"></div>
      </div>
    </main>

    <div v-else class="loading">
      <p>Loading wind data...</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch, nextTick } from "vue";
import PocketBase from "pocketbase";
import * as echarts from "echarts";

interface Measurement {
  id: string;
  source_id: string;
  ts: string;
  gps_lat?: number;
  gps_lng?: number;
  sensor_heading_deg?: number;
  sensor_speed_mps?: number;
  true_wind_dir_deg: number;
  true_wind_speed_mps: number;
  apparent_wind_dir_deg?: number;
  apparent_wind_speed_mps?: number;
}

type WindSpeedUnit = "mps" | "knots" | "kmh";

const loading = ref(true);
const error = ref("");
const latestMeasurement = ref<Measurement | null>(null);
const measurements = ref<Measurement[]>([]);
const lastUpdateTime = ref("");
const chartsReady = ref(false);
const STORAGE_KEY = "windDashboardSettings";
function loadStoredSettings(): {
  timeFrameMinutes: number;
  windSpeedUnit: WindSpeedUnit;
  directionOldestAtTop: boolean;
  exclusionWindowSec: number;
  hideInvalidData: boolean;
  accelThresholdMs2: number;
  headingRateThresholdDegs: number;
  avgWindowMinutes: number;
} {
  const defaultSettings = {
    timeFrameMinutes: 30,
    windSpeedUnit: "mps",
    directionOldestAtTop: false,
    exclusionWindowSec: 5,
    hideInvalidData: false,
    accelThresholdMs2: 0.3,
    headingRateThresholdDegs: 10,
    avgWindowMinutes: 10,
  } as const;

  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw)
      return {
        ...defaultSettings,
        ...JSON.parse(raw),
      };
  } catch {
    /* ignore */
  }
  return defaultSettings;
}
const storedSettings = loadStoredSettings();
const timeFrameMinutes = ref(storedSettings.timeFrameMinutes);
const settingsOpen = ref(false);
const pendingTimeFrame = ref(timeFrameMinutes.value);
const pendingWindSpeedUnit = ref<WindSpeedUnit>(storedSettings.windSpeedUnit);
const pendingOldestAtTop = ref(storedSettings.directionOldestAtTop);
const pendingExclusionWindowSec = ref(storedSettings.exclusionWindowSec);
const pendingHideInvalidData = ref(storedSettings.hideInvalidData);
const pendingAccelThresholdMs2 = ref(storedSettings.accelThresholdMs2);
const pendingHeadingRateThresholdDegs = ref(
  storedSettings.headingRateThresholdDegs
);
const pendingAvgWindowMinutes = ref(storedSettings.avgWindowMinutes);

// Set to true to render newest direction samples at the bottom.
const windSpeedUnit = ref<WindSpeedUnit>(storedSettings.windSpeedUnit);
const directionOldestAtTop = ref(storedSettings.directionOldestAtTop);
const exclusionWindowSec = ref(storedSettings.exclusionWindowSec);
const hideInvalidData = ref(storedSettings.hideInvalidData);
const accelThresholdMs2 = ref(storedSettings.accelThresholdMs2);
const headingRateThresholdDegs = ref(storedSettings.headingRateThresholdDegs);
const avgWindowMinutes = ref(storedSettings.avgWindowMinutes);

const windSpeedUnitLabel = computed(() => {
  if (windSpeedUnit.value === "knots") return "kn";
  if (windSpeedUnit.value === "kmh") return "km/h";
  return "m/s";
});

function convertWindSpeed(speedMps: number): number {
  if (windSpeedUnit.value === "knots") return speedMps * 1.943844;
  if (windSpeedUnit.value === "kmh") return speedMps * 3.6;
  return speedMps;
}

// Human-readable label shown in the KPI cards
const avgWindow = computed(() => `${avgWindowMinutes.value} min`);

const averageDirection = (directions: number[]) => {
  // Circular mean for direction
  const sinSum = directions.reduce(
    (s, d) => s + Math.sin((d * Math.PI) / 180),
    0
  );
  const cosSum = directions.reduce(
    (s, d) => s + Math.cos((d * Math.PI) / 180),
    0
  );
  return ((Math.atan2(sinSum / directions.length, cosSum / directions.length) * 180) /
      Math.PI +
      360) %
    360;
};

// Average of true_wind_dir_deg and true_wind_speed_mps over the last avgWindowMinutes
const averageMeasurement = computed(() => {
  if (!latestMeasurement.value || measurements.value.length === 0) return null;
  const cutoff =
    new Date(latestMeasurement.value.ts).getTime() -
    avgWindowMinutes.value * 60 * 1000;
  const window = measurements.value.filter(
    (m) => new Date(m.ts).getTime() >= cutoff
  );
  if (window.length === 0) return null;
  const avgSpeed =
    window.reduce((s, m) => s + m.true_wind_speed_mps, 0) / window.length;
  const avgDir = averageDirection(window.map((m) => m.true_wind_dir_deg));
  return { true_wind_speed_mps: avgSpeed, true_wind_dir_deg: avgDir };
});

// Detect sensor acceleration / heading-rate events and return merged unreliable time ranges.
function computeUnreliableRanges(
  meas: Measurement[],
  windowMs: number
): Array<[number, number]> {
  if (windowMs <= 0) return [];
  const eventTimes: number[] = [];
  for (let i = 0; i < meas.length - 1; i++) {
    const a = meas[i];
    const b = meas[i + 1];
    const dtMs = Math.abs(new Date(a.ts).getTime() - new Date(b.ts).getTime());
    if (dtMs === 0) continue;
    const dtS = dtMs / 1000;
    const midTime = (new Date(a.ts).getTime() + new Date(b.ts).getTime()) / 2;
    if (a.sensor_speed_mps != null && b.sensor_speed_mps != null) {
      if (
        Math.abs(a.sensor_speed_mps - b.sensor_speed_mps) / dtS >
        accelThresholdMs2.value
      )
        eventTimes.push(midTime);
    }
    if (a.sensor_heading_deg != null && b.sensor_heading_deg != null) {
      let dh = Math.abs(a.sensor_heading_deg - b.sensor_heading_deg);
      if (dh > 180) dh = 360 - dh;
      if (dh / dtS > headingRateThresholdDegs.value) eventTimes.push(midTime);
    }
  }
  const raw = eventTimes.map(
    (t) => [t - windowMs, t + windowMs] as [number, number]
  );
  raw.sort((a, b) => a[0] - b[0]);
  const merged: Array<[number, number]> = [];
  for (const r of raw) {
    if (merged.length > 0 && r[0] <= merged[merged.length - 1][1])
      merged[merged.length - 1][1] = Math.max(
        merged[merged.length - 1][1],
        r[1]
      );
    else merged.push([...r] as [number, number]);
  }
  return merged;
}

async function applySettings() {
  timeFrameMinutes.value = pendingTimeFrame.value;
  windSpeedUnit.value = pendingWindSpeedUnit.value;
  directionOldestAtTop.value = pendingOldestAtTop.value;
  exclusionWindowSec.value = pendingExclusionWindowSec.value;
  hideInvalidData.value = pendingHideInvalidData.value;
  accelThresholdMs2.value = pendingAccelThresholdMs2.value;
  headingRateThresholdDegs.value = pendingHeadingRateThresholdDegs.value;
  avgWindowMinutes.value = pendingAvgWindowMinutes.value;
  settingsOpen.value = false;

  // Persist to local storage
  localStorage.setItem(
    STORAGE_KEY,
    JSON.stringify({
      timeFrameMinutes: timeFrameMinutes.value,
      windSpeedUnit: windSpeedUnit.value,
      directionOldestAtTop: directionOldestAtTop.value,
      exclusionWindowSec: exclusionWindowSec.value,
      hideInvalidData: hideInvalidData.value,
      accelThresholdMs2: accelThresholdMs2.value,
      headingRateThresholdDegs: headingRateThresholdDegs.value,
      avgWindowMinutes: avgWindowMinutes.value,
    })
  );

  // Dispose existing charts so they reinitialize cleanly
  chartsReady.value = false;
  directionChart?.dispose();
  speedChart?.dispose();
  directionChart = null;
  speedChart = null;

  await nextTick();
  initCharts();
  chartsReady.value = true;
  await loadMeasurements();
}

let pb: PocketBase;
let directionChart: echarts.ECharts | null = null;
let speedChart: echarts.ECharts | null = null;

function fail(reason: string): never {
  throw new Error(reason);
}

async function getPocketBaseUrl(): Promise<string> {
  if (window.location.hostname === "localhost") {
    return "http://localhost:8090";
  }
  const res = await fetch("/api/config");
  const config = await res.json();
  return config.pocketbaseUrl;
}

async function initializePocketBase() {
  try {
    pb = new PocketBase(await getPocketBaseUrl());

    // Subscribe to real-time updates
    try {
      await pb.collection("measurements").subscribe("*", (e: any) => {
        console.log("Realtime update received:", e.record);
        if (e.record) {
          latestMeasurement.value = e.record;
          measurements.value = [e.record, ...measurements.value].slice(
            0,
            timeFrameMinutes.value * 60
          );
          updateCharts();
          updateTime();
        }
      });
      console.log("✓ Realtime subscription active");
      // Initialize with latest measurements
      await loadMeasurements();
    } catch (e) {
      console.log(
        "Realtime subscription not available, falling back to polling:",
        e
      );
      // Fallback: load latest measurements
      await loadMeasurements();
      // Poll every 5 seconds
      setInterval(loadMeasurements, 5000);
    }

    loading.value = false;
  } catch (e) {
    error.value = `Failed to initialize: ${
      e instanceof Error ? e.message : String(e)
    }`;
    console.error("Initialization error:", e);
    loading.value = false;
  }
}

async function loadMeasurements() {
  try {
    const desiredCount = timeFrameMinutes.value * 60;
    const pageSize = Math.min(desiredCount, 1000); // Use smaller page size if requesting fewer than 1000

    let allRecords: Measurement[] = [];
    let page = 1;

    // Fetch pages until we have enough records or reach the end of data
    while (allRecords.length < desiredCount) {
      const records = await pb
        .collection("measurements")
        .getList<Measurement>(page, pageSize, {
          sort: "-ts",
        });
      allRecords = allRecords.concat(records.items);

      // If we got fewer items than requested, we've reached the end of data
      if (records.items.length < pageSize) {
        break;
      }

      page++;
    }

    // Trim to desired count
    allRecords = allRecords.slice(0, desiredCount);

    console.log(
      "Loaded",
      allRecords.length,
      "measurements (desired:",
      desiredCount,
      ", page size:",
      pageSize,
      ")"
    );
    if (allRecords.length > 0) {
      latestMeasurement.value = allRecords[0];
      console.log("Latest measurement:", { ...latestMeasurement.value });
      measurements.value = allRecords.filter(
        (m) =>
          new Date(m.ts).getTime() >
          new Date(
            (
              latestMeasurement.value ??
              fail("unreachable: no latest measurement")
            ).ts
          ).getTime() -
            timeFrameMinutes.value * 60 * 1000
      );
      updateCharts();
      updateTime();
    }
  } catch (e) {
    console.error("Failed to load measurements:", e);
  }
}

function updateTime() {
  if (latestMeasurement.value?.ts) {
    const date = new Date(latestMeasurement.value.ts);
    lastUpdateTime.value = date.toLocaleTimeString();
  }
}

function updateCharts() {
  if (!chartsReady.value) {
    return;
  }

  if (measurements.value.length === 0) {
    console.log("No measurements to chart");
    return;
  }

  // Wind direction chart (x = direction, y = time)
  const directionPoints = measurements.value.map((m) => ({
    direction: m.true_wind_dir_deg,
    timeMs: new Date(m.ts).getTime(),
  }));

  if (directionChart && directionPoints.length > 0) {
    // Calculate average wind direction
    const avgDirection = directionPoints.length > 0 ?
      averageDirection(directionPoints.map((dp) => dp.direction))
      : 0;

    // Compute displaySpan based on largest distance from average
    let maxDistance = 0;
    for (const point of directionPoints) {
      let distance = Math.abs(point.direction - avgDirection);
      // Handle wrap-around: the shortest distance might be the other way
      if (distance > 180) {
        distance = 360 - distance;
      }
      maxDistance = Math.max(maxDistance, distance);
    }

    // Add 10% padding and cap at 180
    const displaySpan = Math.min(180, maxDistance + 5);
    let axisMin = avgDirection - displaySpan;
    let axisMax = avgDirection + displaySpan;

    // Normalize data points for display with wrap-around
    const normalizedPoints = directionPoints.map((p) => {
      let value = p.direction;
      // Shift points to the correct side of the axis if they wrap
      if (value < axisMin) {
        value += 360;
      } else if (value > axisMax) {
        value -= 360;
      }
      return { ...p, direction: value };
    });

    // Round axis bounds to nearest 5 for clean labels
    axisMin = Math.round(axisMin / 5) * 5;
    axisMax = Math.round(axisMax / 5) * 5;

    // Build two data arrays:
    // - reliableSeriesData: solid line
    // - excludedSeriesData: dashed line for ignored windows
    const reliableSeriesData: Array<[number, number] | [null, null]> = [];
    const excludedSeriesData: Array<[number, number] | [null, null]> = [];
    const eps = 1e-6;
    const unreliableRanges = computeUnreliableRanges(
      measurements.value,
      exclusionWindowSec.value * 1000
    );

    function isUnreliable(timeMs: number): boolean {
      for (const [start, end] of unreliableRanges) {
        if (timeMs >= start && timeMs <= end) return true;
      }
      return false;
    }

    function pushPoint(
      direction: number,
      timeMs: number,
      unreliable: boolean
    ): void {
      if (unreliable) {
        reliableSeriesData.push([null, null]);
        excludedSeriesData.push([direction, timeMs]);
      } else {
        reliableSeriesData.push([direction, timeMs]);
        excludedSeriesData.push([null, null]);
      }
    }

    // Keep track of the last appended sample so dashed segments can include
    // copied valid endpoints at reliability transitions.
    let hasLastSample = false;
    let lastDirection = 0;
    let lastTimeMs = 0;
    let lastUnreliable = false;

    function appendSample(
      direction: number,
      timeMs: number,
      unreliable: boolean
    ): void {
      if (hasLastSample && lastUnreliable !== unreliable) {
        if (unreliable) {
          // Entering excluded region: copy the last reliable point into dashed series.
          pushPoint(lastDirection, lastTimeMs, true);
        } else {
          // Leaving excluded region: copy the current reliable point into dashed series.
          pushPoint(direction, timeMs, true);
        }
      }

      pushPoint(direction, timeMs, unreliable);
      hasLastSample = true;
      lastDirection = direction;
      lastTimeMs = timeMs;
      lastUnreliable = unreliable;
    }

    function pushBreak(): void {
      reliableSeriesData.push([null, null]);
      excludedSeriesData.push([null, null]);
      hasLastSample = false;
    }

    // Build series from normalized values only.
    // If the normalized gap exceeds 180 deg, force a cutoff break.
    for (let i = 0; i < normalizedPoints.length; ++i) {
      const point = normalizedPoints[i];
      const pointUnreliable = isUnreliable(point.timeMs);
      appendSample(point.direction, point.timeMs, pointUnreliable);

      if (i >= normalizedPoints.length - 1) continue;

      const nextPoint = normalizedPoints[i + 1];
      const gap = Math.abs(nextPoint.direction - point.direction);
      if (gap <= 180) continue;

      // Crossing side rule from normalized space:
      // current < avg => cross left cutoff first, current > avg => cross right cutoff first
      const crossesLeftFirst = point.direction < avgDirection;
      const beforeCutoff = crossesLeftFirst ? axisMin + eps : axisMax - eps;
      const afterCutoff = crossesLeftFirst ? axisMax - eps : axisMin + eps;
      const cutTimeBefore = point.timeMs + 0.499;
      const cutTimeAfter = point.timeMs + 0.501;
      const beforeUnreliable = isUnreliable(cutTimeBefore);
      const afterUnreliable = isUnreliable(cutTimeAfter);

      appendSample(beforeCutoff, cutTimeBefore, beforeUnreliable);
      pushBreak();
      appendSample(afterCutoff, cutTimeAfter, afterUnreliable);
    }

    // Log using original sample count
    console.log(
      "Updating direction chart with",
      directionPoints.length,
      "points, average:",
      avgDirection.toFixed(1)
    );
    try {
      directionChart.setOption({
        animation: false,
        animationDuration: 0,
        animationDurationUpdate: 0,
        xAxis: [
          {
            min: axisMin,
            max: axisMax,
          },
          {
            min: Math.floor(axisMin - avgDirection),
            max: Math.ceil(axisMax - avgDirection),
            splitLine: {
              lineStyle: {
                type: [5, 10],
              },
            },
          },
        ],
        yAxis: {
          type: "time",
          inverse: directionOldestAtTop.value,
          axisLabel: {
            formatter: (value: number) => new Date(value).toLocaleTimeString(),
          },
        },
        series: [
          {
            name: "Direction (Reliable)",
            data: reliableSeriesData,
            markLine: {
              symbol: directionOldestAtTop.value
                ? ["none", "arrow"]
                : ["arrow", "none"],
              data: [
                {
                  name: "Average",
                  xAxis: avgDirection,
                  lineStyle: {
                    color: "#667eea",
                    type: "dashed",
                    width: 2,
                  },
                  label: {
                    position: directionOldestAtTop.value ? "end" : "start",
                    formatter: `Avg: ${avgDirection.toFixed(1)}°`,
                  },
                },
              ],
            },
          },
          {
            name: "Direction (Excluded)",
            show: !hideInvalidData.value,
            data: hideInvalidData.value ? [] : excludedSeriesData,
            lineStyle: {
              type: "dashed",
              width: 2,
              color: "#667eea",
              opacity: 0.9,
            },
          },
        ],
      });
      console.log("✓ Direction chart updated");
    } catch (e) {
      console.error("Failed to update direction chart:", e);
    }
  } else {
    console.log(
      "Direction chart not ready or no data:",
      !!directionChart,
      directionPoints.length
    );
  }

  // Wind speed chart (smoothed with gust)
  const speedPoints = measurements.value.map((m) => ({
    speed: convertWindSpeed(m.true_wind_speed_mps),
    time: new Date(m.ts).toLocaleTimeString(),
  }));

  if (speedChart && speedPoints.length > 0) {
    console.log(
      "Updating speed chart with",
      speedPoints.length,
      "points, first 3:",
      speedPoints.slice(0, 3)
    );
    try {
      speedChart.setOption({
        animation: false,
        animationDuration: 0,
        animationDurationUpdate: 0,
        xAxis: {
          data: speedPoints.map((p) => p.time),
        },
        yAxis: {
          name: `Speed (${windSpeedUnitLabel.value})`,
        },
        series: [
          {
            data: speedPoints.map((p) => p.speed),
          },
        ],
      });
      console.log("✓ Speed chart updated");
    } catch (e) {
      console.error("Failed to update speed chart:", e);
    }
  } else {
    console.log(
      "Speed chart not ready or no data:",
      !!speedChart,
      speedPoints.length
    );
  }
}

function initCharts() {
  const dirContainer =
    document.getElementById("wind-direction-chart") ??
    fail("Direction chart container not found");
  const speedContainer =
    document.getElementById("wind-speed-chart") ??
    fail("Speed chart container not found");

  // Wind direction chart
  directionChart = echarts.init(dirContainer);
  directionChart.setOption({
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    grid: {
      left: "24px",
      right: "16px",
      bottom: "28px",
      top: "28px",
      containLabel: true,
    },
    xAxis: [
      {
        type: "value",
        name: "Direction (deg)",
        nameLocation: "middle",
        nameTextStyle: {
          padding: [0, 0, 8, 0],
        },
        min: 0,
        max: 360,
        position: "top",
        axisLabel: {
          formatter: (value: number) => {
            return `${(value + 360) % 360}`;
          },
        },
      },
      {
        type: "value",
        name: "Offset to Avg (deg)",
        nameLocation: "middle",
        nameTextStyle: {
          padding: [8, 0, 0, 0],
        },
        min: -180,
        max: 180,
        position: "bottom",
        axisLabel: {
          formatter: (value: number) => {
            if (value == 0) {
              return "";
            }
            if (value > 0) {
              return `+${value}`;
            }
            return `${value}`;
          },
        },
      },
    ],
    yAxis: {
      type: "time",
      name: "Time",
      nameLocation: "middle",
      nameTextStyle: {
        padding: [0, 0, 48, 0],
      },
      inverse: directionOldestAtTop.value,
      axisLabel: {
        inside: false,
        margin: 8,
        padding: [8, 0, 0, 0],
        formatter: (value: number) => new Date(value).toLocaleTimeString(),
      },
      axisLine: {
        show: false,
      },
      axisTick: {
        show: false,
      },
    },
    series: [
      {
        name: "Direction (Reliable)",
        type: "line",
        smooth: false,
        symbol: "circle",
        symbolSize: 5,
        showSymbol: false,
        encode: { x: 0, y: 1 },
        data: [],
        animation: false,
        itemStyle: { color: "#667eea" },
      },
      {
        name: "Direction (Excluded)",
        type: "line",
        smooth: false,
        symbol: "circle",
        symbolSize: 5,
        showSymbol: false,
        encode: { x: 0, y: 1 },
        data: [],
        animation: false,
        lineStyle: {
          type: "dashed",
          width: 2,
          color: "#667eea",
          opacity: 0.9,
        },
      },
    ],
  });
  console.log("✓ Direction chart initialized");

  // Wind speed chart
  speedChart = echarts.init(speedContainer);
  speedChart.setOption({
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    grid: {
      left: "24px",
      right: "16px",
      bottom: "16px",
      top: "16px",
      containLabel: true,
    },
    xAxis: {
      type: "category",
      name: "Time",
      // our data is sorted newest to oldest, we reverse it to show the graph with newest on the right
      inverse: true,
      nameLocation: "middle",
      nameTextStyle: {
        padding: [8, 0, 0, 0],
      },
      data: [],
    },
    yAxis: {
      type: "value",
      name: `Speed (${windSpeedUnitLabel.value})`,
      nameLocation: "middle",
      nameTextStyle: {
        padding: [0, 0, 12, 0],
      },
    },
    series: [
      {
        name: "Wind Speed",
        type: "line",
        data: [],
        animation: false,
        smooth: true,
        symbol: "circle",
        symbolSize: 4,
        showSymbol: false,
        itemStyle: {
          color: "#764ba2",
        },
        /*areaStyle: {
          color: 'rgba(118, 75, 162, 0.1)',
        },*/
      },
    ],
  });
  console.log("✓ Speed chart initialized");

  // Handle window resize
  window.addEventListener("resize", () => {
    directionChart?.resize();
    speedChart?.resize();
  });
}

onMounted(() => {
  initializePocketBase();
});

// Initialize charts once content becomes visible
watch(loading, async (isLoading) => {
  if (!isLoading) {
    console.log("Content visible, initializing charts...");
    await nextTick();
    initCharts();
    chartsReady.value = true;
    updateCharts();
  }
});
</script>

<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}
</style>

<style scoped>
#app {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen,
    Ubuntu, Cantarell, sans-serif;
  min-height: 100vh;
  padding: 20px;
  display: flex;
  flex-direction: column;
}

.header {
  position: relative;
}

.header h1 {
  font-size: 3rem;
  margin-bottom: 10px;
}

.error-banner {
  background: rgba(255, 0, 0, 0.8);
  color: white;
  padding: 10px 20px;
  border-radius: 8px;
  margin-top: 10px;
}

.content {
  animation: fadeIn 0.5s ease-in-out;
  display: flex;
  flex-direction: column;
  gap: 40px;
  flex-grow: 1;
}

.kpi-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 20px;
}

.kpi-card {
  background: rgba(255, 255, 255, 0.95);
  padding: 25px;
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
  backdrop-filter: blur(4px);
  transition: transform 0.3s ease, box-shadow 0.3s ease;
}

.kpi-card:hover {
  transform: translateY(-5px);
  box-shadow: 0 12px 40px rgba(0, 0, 0, 0.15);
}

.kpi-card .label {
  font-size: 0.9rem;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 8px;
  font-weight: 600;
}

.kpi-card .value {
  font-size: 2rem;
  color: #333;
  font-weight: bold;
}

.charts {
  display: grid;
  flex-grow: 1;
  grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
  gap: 20px;
}

.left-column {
  display: flex;
  flex-direction: column;
  gap: 20px;
}


.chart-container {
  background: rgba(255, 255, 255, 0.95);
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
  min-height: 500px;
  padding: 20px;
  flex-grow: 1;
}

.loading {
  text-align: center;
  color: white;
  font-size: 1.2rem;
  padding: 60px 20px;
}

/* ── Settings ─────────────────────────────────────────── */
.settings-btn {
  position: absolute;
  top: 0px;
  right: 0px;
  z-index: 1;
  background: rgba(255, 255, 255, 0.2);
  border: none;
  border-radius: 50%;
  width: 44px;
  height: 44px;
  font-size: 44px;
  cursor: pointer;
  color: black;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.2s;
}

.settings-btn:hover {
  background: rgba(255, 255, 255, 0.35);
}

.settings-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.45);
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: center;
}

.settings-panel {
  background: #fff;
  border-radius: 14px;
  padding: 28px 32px;
  min-width: 320px;
  box-shadow: 0 16px 48px rgba(0, 0, 0, 0.2);
}

.settings-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 24px;
}

.settings-header h2 {
  font-size: 1.3rem;
  color: #333;
  margin: 0;
}

.settings-close {
  background: none;
  border: none;
  font-size: 1.2rem;
  cursor: pointer;
  color: #666;
  line-height: 1;
}

.settings-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 18px;
  gap: 16px;
}

.settings-row label {
  font-size: 0.95rem;
  color: #444;
  font-weight: 500;
}

.settings-row select {
  padding: 6px 10px;
  border-radius: 8px;
  border: 1px solid #ccc;
  font-size: 0.95rem;
  cursor: pointer;
}

.settings-row input[type="checkbox"] {
  width: 18px;
  height: 18px;
  cursor: pointer;
}

.settings-footer {
  display: flex;
  justify-content: flex-end;
  margin-top: 8px;
}

.apply-btn {
  background: #667eea;
  color: white;
  border: none;
  border-radius: 8px;
  padding: 8px 22px;
  font-size: 0.95rem;
  font-weight: 600;
  cursor: pointer;
  transition: opacity 0.2s;
}

.apply-btn:hover {
  opacity: 0.88;
}

/* ── Animations ───────────────────────────────────────── */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@media (max-width: 768px) {
  .header h1 {
    font-size: 1.8rem;
  }

  .kpi-cards {
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
    gap: 15px;
  }

  .kpi-card .value {
    font-size: 1.5rem;
  }

  .charts {
    grid-template-columns: 1fr;
  }

  .chart-container {
    height: 400px;
  }
}
</style>
