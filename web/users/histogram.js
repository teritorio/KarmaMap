// Day-by-day edit-volume histogram (Apache ECharts; series "Edits"). Unlike
// the changes viewer's full-coverage timeline, this shows only the days the
// user actually edited (sparse data), so the x-axis has no fixed bounds and
// zero-count days simply do not appear. The y-axis is logarithmic by
// default.

import * as echarts from 'echarts'

let chart = null
let resizeObserver = null
let logScale = true
let lastData = null

export function initHistogram(containerEl) {
  chart = echarts.init(containerEl)

  chart.setOption({
    grid: { left: 72, right: 16, top: 12, bottom: 44 },
    xAxis: { type: 'time' },
    yAxis: yAxisOption(),
    tooltip: { trigger: 'axis' },
    series: [{ type: 'bar', name: 'Edits', data: [] }],
  })

  resizeObserver = new ResizeObserver(() => chart?.resize())
  resizeObserver.observe(containerEl)
  chart.resize() // may not fire initially via ResizeObserver
}

// On the log axis, min=1 starts at a real power of ten.
function yAxisOption() {
  return logScale ? { type: 'log', logBase: 10, min: 1 } : { type: 'value' }
}

export function setLogScale(enabled) {
  logScale = enabled
  if (!chart) return
  chart.setOption({ yAxis: yAxisOption() }, { replaceMerge: ['yAxis'] })
  if (lastData) renderBars()
}

function renderBars() {
  if (!chart || !lastData) return
  const days = [...lastData.entries()].sort(([a], [b]) => a.localeCompare(b))
  const data = days.map(([day, count]) => [
    new Date(`${day}T00:00:00Z`).getTime(),
    logScale && count === 0 ? null : count,
  ])
  chart.setOption({ series: [{ data }] })
}

export function setHistogramData(byDay) {
  if (!chart) return
  lastData = byDay
  renderBars()
}