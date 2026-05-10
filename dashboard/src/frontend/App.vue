<template>
  <div id="app" class="dashboard">
    <header class="header">
      <h1>🌪️ Wind Dashboard</h1>
      <div v-if="error" class="error-banner">{{ error }}</div>
    </header>

    <main v-if="!loading" class="content">
      <div class="kpi-cards">
        <div class="kpi-card">
          <div class="label">True Wind Speed</div>
          <div class="value">{{ latestMeasurement?.true_wind_speed_mps?.toFixed(1) || '—' }} m/s</div>
        </div>
        <div class="kpi-card">
          <div class="label">True Wind Direction</div>
          <div class="value">{{ latestMeasurement?.true_wind_dir_deg?.toFixed(0) || '—' }}°</div>
        </div>
        <div class="kpi-card">
          <div class="label">Sensor Heading</div>
          <div class="value">{{ latestMeasurement?.sensor_heading_deg?.toFixed(0) || '—' }}°</div>
        </div>
        <div class="kpi-card">
          <div class="label">Last Update</div>
          <div class="value">{{ lastUpdateTime }}</div>
        </div>
      </div>

      <div class="charts">
        <div id="wind-direction-chart" class="chart-container"></div>
        <div id="wind-speed-chart" class="chart-container"></div>
      </div>
    </main>

    <div v-else class="loading">
      <p>Loading wind data...</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch, nextTick } from 'vue'
import PocketBase from 'pocketbase'
import * as echarts from 'echarts'

interface Measurement {
  id: string
  source_id: string
  ts: string
  gps_lat?: number
  gps_lng?: number
  sensor_heading_deg?: number
  sensor_speed_mps?: number
  true_wind_dir_deg: number
  true_wind_speed_mps: number
  apparent_wind_dir_deg?: number
  apparent_wind_speed_mps?: number
}

const loading = ref(true)
const error = ref('')
const latestMeasurement = ref<Measurement | null>(null)
const measurements = ref<Measurement[]>([])
const lastUpdateTime = ref('')
const chartsReady = ref(false)
const timeFrameMinutes = ref(30);

// Set to false to render newest direction samples at the bottom.
const directionNewestAtTop = ref(true)

let pb: PocketBase
let directionChart: echarts.ECharts | null = null
let speedChart: echarts.ECharts | null = null

function fail(reason: string): never {
  throw new Error(reason);
}

async function getPocketBaseUrl(): Promise<string> {
  if (window.location.hostname === 'localhost') {
    return 'http://localhost:8090'
  }
  const res = await fetch('/api/config')
  const config = await res.json()
  return config.pocketbaseUrl
}

async function initializePocketBase() {
  try {
    pb = new PocketBase(await getPocketBaseUrl())

    // Subscribe to real-time updates
    try {
      await pb.collection('measurements').subscribe('*', (e: any) => {
        console.log('Realtime update received:', e.record)
        if (e.record) {
          latestMeasurement.value = e.record
          measurements.value = [e.record, ...measurements.value].slice(0, timeFrameMinutes.value * 60)
          updateCharts()
          updateTime()
        }
      })
      console.log('✓ Realtime subscription active')
      // Initialize with latest measurements
      await loadMeasurements()
    } catch (e) {
      console.log('Realtime subscription not available, falling back to polling:', e)
      // Fallback: load latest measurements
      await loadMeasurements()
      // Poll every 5 seconds
      setInterval(loadMeasurements, 5000)
    }

    loading.value = false
  } catch (e) {
    error.value = `Failed to initialize: ${e instanceof Error ? e.message : String(e)}`
    console.error('Initialization error:', e)
    loading.value = false
  }
}

async function loadMeasurements() {
  try {
    const records = await pb.collection('measurements').getList<Measurement>(1, timeFrameMinutes.value * 60, {
      sort: '-ts',
    })
    console.log('Loaded', records.items.length, 'measurements')
    if (records.items.length > 0) {
      latestMeasurement.value = records.items[0];
      console.log('Latest measurement:', {...latestMeasurement.value})
      measurements.value = records.items.filter(
        (m) => 
          new Date(m.ts).getTime() >
          new Date((latestMeasurement.value ?? fail("unreachable: no latest measurement")).ts).getTime()
           - timeFrameMinutes.value * 60 * 1000)
      updateCharts()
      updateTime()
    }
  } catch (e) {
    console.error('Failed to load measurements:', e)
  }
}

function updateTime() {
  if (latestMeasurement.value?.ts) {
    const date = new Date(latestMeasurement.value.ts)
    lastUpdateTime.value = date.toLocaleTimeString()
  }
}

function updateCharts() {
  if (!chartsReady.value) {
    return
  }

  if (measurements.value.length === 0) {
    console.log('No measurements to chart')
    return
  }

  // Wind direction chart (x = direction, y = time)
  const directionPoints = measurements.value
    .map((m) => ({
      direction: m.true_wind_dir_deg,
      time: new Date(m.ts).toLocaleTimeString(),
    }))

  if (directionChart && directionPoints.length > 0) {
    // Calculate average wind direction
    const avgDirection = directionPoints.length > 0 
      ? directionPoints.reduce((sum, p) => sum + p.direction, 0) / directionPoints.length 
      : 0
    
    console.log('Updating direction chart with', directionPoints.length, 'points, average:', avgDirection.toFixed(1))
    try {
      directionChart.setOption({
        yAxis: {
          data: directionPoints.map((p) => p.time),
          inverse: directionNewestAtTop.value,
        },
        series: [
          {
            data: directionPoints.map((p) => p.direction),
            markLine: {
              data: [
                {
                  name: 'Average',
                  xAxis: avgDirection,
                  lineStyle: {
                    color: '#667eea',
                    type: 'dashed',
                    width: 2,
                  },
                  label: {
                    position: 'end',
                    formatter: `Avg: ${avgDirection.toFixed(1)}°`,
                  },
                },
              ],
            },
          },
        ],
      })
      console.log('✓ Direction chart updated')
    } catch (e) {
      console.error('Failed to update direction chart:', e)
    }
  } else {
    console.log('Direction chart not ready or no data:', !!directionChart, directionPoints.length)
  }

  // Wind speed chart (smoothed with gust)
  const speedPoints = measurements.value
    .map((m) => ({
      speed: m.true_wind_speed_mps,
      time: new Date(m.ts).toLocaleTimeString(),
    }))

  if (speedChart && speedPoints.length > 0) {
    console.log('Updating speed chart with', speedPoints.length, 'points, first 3:', speedPoints.slice(0, 3))
    try {
      speedChart.setOption({
        xAxis: {
          data: speedPoints.map((p) => p.time)
        },
        series: [
          {
            data: speedPoints.map((p) => p.speed),
          },
        ],
      })
      console.log('✓ Speed chart updated')
    } catch (e) {
      console.error('Failed to update speed chart:', e)
    }
  } else {
    console.log('Speed chart not ready or no data:', !!speedChart, speedPoints.length)
  }
}

function initCharts() {
  const dirContainer = document.getElementById('wind-direction-chart') ?? fail('Direction chart container not found');
  const speedContainer = document.getElementById('wind-speed-chart') ?? fail('Speed chart container not found');

  // Wind direction chart
  directionChart = echarts.init(dirContainer)
  directionChart.setOption({
    grid: {
      left: "24px",
      right: "16px",
      bottom: "16px",
      top: "16px",
      containLabel: true,
    },
    xAxis: {
      type: 'value',
      name: 'Direction (deg)',
      nameLocation: "middle",
      nameTextStyle: {
        padding: [0, 0, 8, 0],
      },
      min: 0,
      max: 360,
      position: "top",
    },
    yAxis: {
      type: 'category',
      name: 'Time',
      nameLocation: "middle",
      nameTextStyle: {
        padding: [0, 0, 48, 0],
      },
      data: [],
      inverse: directionNewestAtTop.value,
      axisLabel: {
        inside: false,
        margin: 8,
        padding: [8, 0, 0, 0],
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
        name: 'Direction',
        type: 'line',
        smooth: false,
        symbol: 'circle',
        symbolSize: 5,
        showSymbol: false,
        data: [],
        animation: false,
        itemStyle: {
          color: '#667eea',
        },
      },
    ],
  })
  console.log('✓ Direction chart initialized')

  // Wind speed chart
  speedChart = echarts.init(speedContainer)
  speedChart.setOption({
    grid: {
      left: "24px",
      right: "16px",
      bottom: "16px",
      top: "16px",
      containLabel: true,
    },
    xAxis: {
      type: 'category',
      name: 'Time',
      // our data is sorted newest to oldest, we reverse it to show the graph with newest on the right
      inverse: true,
      nameLocation: "middle",
      nameTextStyle: {
        padding: [8, 0, 0, 0],
      },
      data: [],
    },
    yAxis: {
      type: 'value',
      name: 'Speed (m/s)',
      nameLocation: "middle",
      nameTextStyle: {
        padding: [0, 0, 12, 0],
      },
    },
    series: [
      {
        name: 'Wind Speed',
        type: 'line',
        data: [],
        animation: false,
        smooth: true,
        symbol: 'circle',
        symbolSize: 4,
        showSymbol: false,
        itemStyle: {
          color: '#764ba2',
        },
        /*areaStyle: {
          color: 'rgba(118, 75, 162, 0.1)',
        },*/
      },
    ],
  })
  console.log('✓ Speed chart initialized')

  // Handle window resize
  window.addEventListener('resize', () => {
    directionChart?.resize()
    speedChart?.resize()
  })
}

onMounted(() => {
  initializePocketBase()
})

// Initialize charts once content becomes visible
watch(loading, async (isLoading) => {
  if (!isLoading) {
    console.log('Content visible, initializing charts...')
    await nextTick()
    initCharts()
    chartsReady.value = true
    updateCharts()
  }
})
</script>

<style scoped>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

#app {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen,
    Ubuntu, Cantarell, sans-serif;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  min-height: 100vh;
  padding: 20px;
}

.dashboard {
  max-width: 1400px;
  margin: 0 auto;
}

.header {
  text-align: center;
  color: white;
  margin-bottom: 40px;
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
}

.kpi-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 20px;
  margin-bottom: 40px;
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
  grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
  gap: 20px;
}

.chart-container {
  background: rgba(255, 255, 255, 0.95);
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
  height: 500px;
  padding: 20px;
}

.loading {
  text-align: center;
  color: white;
  font-size: 1.2rem;
  padding: 60px 20px;
}

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
