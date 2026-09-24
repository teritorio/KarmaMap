// Day-by-day activity histogram (Apache ECharts; series "Edits (+
// relations)"). Unlike the changes viewer's full-coverage timeline, this
// shows only the days the user actually edited (sparse data), so the x-axis
// has no fixed bounds and zero-count days simply do not appear. Each day
// counts the nine history counters: the six node/way changes plus the three
// relation counters. The y-axis is logarithmic by default.
//
// Days carrying vandalism_flag bits (users_history.parquet) get a solid
// full-height red markArea band and an altered blue volume bar; the tooltip
// lists the activated screens (paper sec. 5).

import * as echarts from 'echarts'
import { FLAG_LABELS } from './reputation.js'

// Volume bars: normal blue, altered blue on flagged days. The flag band's
// solid red fill spans the full y-axis behind the volume bar, so a
// flagged day reads red at full height even where its volume is small.
const VOLUME_COLOR = '#5470c6'
const FLAGGED_VOLUME_COLOR = '#3b5998'
const FLAG_COLOR = '#d63c3c'

const DAY_MS = 86400000

let chart = null
let resizeObserver = null
let logScale = true
let lastData = null // { byDay, flagByDay }

export function initHistogram(containerEl) {
  chart = echarts.init(containerEl)

  chart.setOption({
    grid: { left: 72, right: 16, top: 12, bottom: 44 },
    xAxis: { type: 'time' },
    yAxis: yAxisOption(),
    tooltip: {
      trigger: 'axis',
      formatter: tooltipFormatter,
    },
    series: [{ type: 'bar', name: 'Edits (+ relations)', data: [] }],
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

function tooltipFormatter(params) {
  const first = Array.isArray(params) ? params[0] : params
  const ts = first?.axisValue ?? first?.value?.[0]
  if (ts == null || !lastData) return ''
  const day = new Date(ts).toISOString().slice(0, 10)
  const count = lastData.byDay.get(day) ?? 0
  const flags = lastData.flagByDay.get(day) ?? 0
  const lines = [`<b>${day}</b>`, `Total edits: ${count.toLocaleString()}`]
  const active = FLAG_LABELS.filter((f) => flags & f.mask)
  if (active.length === 0) {
    lines.push('<span style="color:#888">No vandalism flags</span>')
  } else {
    for (const f of active) {
      lines.push(`<span style="color:#d63c3c;font-weight:700">\u25cf</span> ${f.label}`)
    }
  }
  return lines.join('<br/>')
}

function renderBars() {
  if (!chart || !lastData) return
  const { byDay, flagByDay } = lastData
  const days = [...byDay.entries()].sort(([a], [b]) => a.localeCompare(b))
  if (days.length === 0) {
    chart.setOption({ series: [{ data: [], markArea: { data: [] } }] })
    return
  }
  const ts = (day) => new Date(`${day}T00:00:00Z`).getTime()
  const volume = days.map(([day, count]) => ({
    value: [ts(day), logScale && count === 0 ? null : count],
    itemStyle: {
      color: (flagByDay.get(day) ?? 0) ? FLAGGED_VOLUME_COLOR : VOLUME_COLOR,
    },
  }))
  // A markArea between the day's bounds spans the full y-axis on both the
  // log and linear axes, so the red wash always reaches the chart top.
  const markArea = {
    silent: true,
    itemStyle: { color: FLAG_COLOR },
    data: days
      .filter(([day]) => (flagByDay.get(day) ?? 0))
      .map(([day]) => [{ xAxis: ts(day) }, { xAxis: ts(day) + DAY_MS }]),
  }
  chart.setOption({ series: [{ data: volume, markArea }] })
}

export function setHistogramData(byDay, flagByDay) {
  if (!chart) return
  lastData = { byDay, flagByDay }
  renderBars()
}
