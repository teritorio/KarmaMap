// Day-by-day count histogram (Apache ECharts; the name "Changes" series).
//
// The x-axis spans the FULL dataset coverage from manifest.json, and the
// y-axis is logarithmic by default (log(0) = -Infinity, so zero-count days
// render as gaps on the log axis, flat zero bars on the linear axis).

import * as echarts from 'echarts'

let chart = null
let resizeObserver = null
let rangeChangeCallback = null
let axisMin, axisMax // fixed x-axis bounds set once in initHistogram()
let logScale = true // default matches the #log-scale checkbox in app.js
let lastData = null // for re-rendering bars on a scale toggle

// setHistogramWindow() fires the same 'datazoom' event a user drag would,
// which would otherwise loop back into rangeChangeCallback.
let isProgrammaticZoom = false

// On the log axis a 0 count is a gap (null) rather than log(0) = -Infinity.
function formatBarValue(count) {
  if (logScale && count === 0) return null
  return count
}

function monthStartTimestamp(month) {
  const [y, m] = month.split('-').map(Number)
  return Date.UTC(y, m - 1, 1)
}

// Day 0 of next month = last day of this month.
function monthEndTimestamp(month) {
  const [y, m] = month.split('-').map(Number)
  return Date.UTC(y, m, 0)
}

function dateStrFromTimestamp(ts) {
  return new Date(ts).toISOString().slice(0, 10)
}

export function initHistogram(containerEl, manifest) {
  const minMonth = manifest.date_range?.min_month
  const maxMonth = manifest.date_range?.max_month
  axisMin = minMonth ? monthStartTimestamp(minMonth) : undefined
  axisMax = maxMonth ? monthEndTimestamp(maxMonth) : undefined

  chart = echarts.init(containerEl)

  chart.setOption({
    grid: { left: 44, right: 16, top: 12, bottom: 44 },
    xAxis: { type: 'time', min: axisMin, max: axisMax },
    yAxis: yAxisOption(),
    tooltip: { trigger: 'axis' },
    // Both dataZooms share xAxisIndex 0 and stay linked; the slider is
    // first so the handler below reads dataZoom[0] as the canonical window.
    dataZoom: [
      { type: 'slider', xAxisIndex: 0, height: 20, bottom: 10 },
      { type: 'inside', xAxisIndex: 0 },
    ],
    series: [{ type: 'bar', name: 'Changes', data: [] }],
  })

  chart.on('dataZoom', () => {
    if (isProgrammaticZoom) {
      isProgrammaticZoom = false
      return
    }
    if (!rangeChangeCallback) return

    const dz = chart.getOption().dataZoom[0]
    let startTs, endTs
    if (axisMin != null && axisMax != null) {
      // Percent start/end are always present and authoritative; prefer
      // them over startValue/endValue, which can go stale after a drag.
      startTs = axisMin + ((axisMax - axisMin) * dz.start) / 100
      endTs = axisMin + ((axisMax - axisMin) * dz.end) / 100
    } else if (dz.startValue != null && dz.endValue != null) {
      startTs = dz.startValue
      endTs = dz.endValue
    } else {
      return // no usable bounds (e.g. empty manifest)
    }

    rangeChangeCallback(dateStrFromTimestamp(startTs), dateStrFromTimestamp(endTs))
  })

  resizeObserver = new ResizeObserver(() => chart?.resize())
  resizeObserver.observe(containerEl)
  chart.resize() // may not fire initially via ResizeObserver
}

// On the log axis, min=1 starts at a real power of ten.
function yAxisOption() {
  return logScale ? { type: 'log', logBase: 10, min: 1 } : { type: 'value' }
}

// Display-only toggle; re-renders bars without a new fetch. replaceMerge
// prevents the log axis's min:1 leaking into the linear axis.
export function setLogScale(enabled) {
  logScale = enabled
  if (!chart) return
  chart.setOption({ yAxis: yAxisOption() }, { replaceMerge: ['yAxis'] })
  if (lastData) renderBars()
}

export function onHistogramRangeChange(callback) {
  rangeChangeCallback = callback
}

export function setHistogramWindow(startDateStr, endDateStr) {
  if (!chart) return
  isProgrammaticZoom = true
  chart.dispatchAction({
    type: 'dataZoom',
    startValue: new Date(`${startDateStr}T00:00:00Z`).getTime(),
    endValue: new Date(`${endDateStr}T00:00:00Z`).getTime(),
  })
}

function renderBars() {
  if (!chart || !lastData) return

  const { byDay, startDateStr, endDateStr } = lastData
  const data = []
  let cursor = new Date(`${startDateStr}T00:00:00Z`)
  const end = new Date(`${endDateStr}T00:00:00Z`)
  while (cursor <= end) {
    const day = cursor.toISOString().slice(0, 10)
    data.push([cursor.getTime(), formatBarValue(byDay.get(day) ?? 0)])
    cursor = new Date(cursor.getTime() + 86400000)
  }

  chart.setOption({ series: [{ data }] })
  setHistogramWindow(startDateStr, endDateStr)
}

export function setHistogramData(byDay, startDateStr, endDateStr) {
  if (!chart) return
  lastData = { byDay, startDateStr, endDateStr }
  renderBars()
}
