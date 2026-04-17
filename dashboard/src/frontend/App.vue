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
import { ref, onMounted, watch } from 'vue'
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

let pb: PocketBase
let directionChart: echarts.ECharts | null = null
let speedChart: echarts.ECharts | null = null

async function initializePocketBase() {
  try {
    // Get config from server
    const configRes = await fetch('/api/config')
    const config = await configRes.json()

    pb = new PocketBase(config.pocketbaseUrl)

    // Subscribe to real-time updates
    try {
      await pb.collection('measurements').subscribe('*', (e: any) => {
        if (e.record) {
          latestMeasurement.value = e.record
          measurements.value = [e.record, ...measurements.value].slice(0, 100)
          updateCharts()
          updateTime()
        }
      })
    } catch (e) {
      console.log('Realtime subscription not available, falling back to polling')
      // Fallback: load latest measurements
      await loadMeasurements()
      // Poll every 5 seconds
      setInterval(loadMeasurements, 5000)
    }

    loading.value = false
  } catch (e) {
    error.value = `Failed to initialize: ${e instanceof Error ? e.message : String(e)}`
    loading.value = false
  }
}

async function loadMeasurements() {
  try {
    const records = await pb.collection('measurements').getList(1, 50, {
      sort: '-ts',
    })
    measurements.value = records.items as Measurement[]
    if (records.items.length > 0) {
      latestMeasurement.value = records.items[0]
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
  if (measurements.value.length === 0) return

  // Wind direction chart (centered polar)
  const dirData = measurements.value
    .filter((m) => m.true_wind_dir_deg !== null)
    .map((m) => m.true_wind_dir_deg)
    .reverse()

  if (directionChart && dirData.length > 0) {
    directionChart.setOption({
      series: [
        {
          data: dirData.map((angle, idx) => [idx, angle]),
        },
      ],
    })
  }

  // Wind speed chart (smoothed with gust)
  const speedData = measurements.value
    .filter((m) => m.true_wind_speed_mps !== null)
    .map((m) => m.true_wind_speed_mps)
    .reverse()

  if (speedChart && speedData.length > 0) {
    speedChart.setOption({
      xAxis: {
        data: Array.from({ length: speedData.length }, (_, i) => i),
      },
      series: [
        {
          data: speedData,
        },
      ],
    })
  }
}

function initCharts() {
  // Wind direction chart
  const dirContainer = document.getElementById('wind-direction-chart')
  if (dirContainer) {
    directionChart = echarts.init(dirContainer)
    directionChart.setOption({
      title: { text: 'True Wind Direction' },
      polar: {
        radius: '75%',
      },
      angleAxis: {
        type: 'value',
        max: 360,
        splitLine: {
          show: true,
        },
      },
      radiusAxis: {
        type: 'value',
      },
      series: [
        {
          coordinateSystem: 'polar',
          type: 'scatter',
          symbolSize: 6,
          data: [],
        },
      ],
    })
  }

  // Wind speed chart
  const speedContainer = document.getElementById('wind-speed-chart')
  if (speedContainer) {
    speedChart = echarts.init(speedContainer)
    speedChart.setOption({
      title: { text: 'True Wind Speed' },
      xAxis: {
        type: 'category',
        data: [],
      },
      yAxis: {
        type: 'value',
      },
      series: [
        {
          type: 'line',
          data: [],
          smooth: true,
          symbol: 'circle',
          symbolSize: 4,
        },
      ],
    })
  }

  // Handle window resize
  window.addEventListener('resize', () => {
    directionChart?.resize()
    speedChart?.resize()
  })
}

onMounted(() => {
  initCharts()
  initializePocketBase()
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
