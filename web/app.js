import { loadManifest } from './manifest.js'
import { bboxToCells, cellsMinMaxSet } from './h3-bbox.js'
import { queryChanges } from './query.js'
import { initMap, renderResults, getViewportBbox, setResultsLogScale } from './map.js'
import { initHistogram, setHistogramData, setHistogramWindow, setLogScale, onHistogramRangeChange } from './histogram.js'
import { readPermalink, writePermalink } from './permalink.js'

// Where the Caddy service serves output-dir/.
const BASE_URL = 'http://localhost:8080'

// Minimum map zoom for a query; below this the bbox covers too many cells.
const MIN_ZOOM = 12

// Month span of the date range shown on first load.
const DEFAULT_MONTHS_SPAN = 4

const statusEl = document.getElementById('status')
const startDateEl = document.getElementById('start-date')
const endDateEl = document.getElementById('end-date')
const histogramEl = document.getElementById('histogram')
const logScaleEl = document.getElementById('log-scale')

function setStatus(text) {
  statusEl.textContent = text
}

function pad2(n) {
  return String(n).padStart(2, '0')
}

function monthStartDate(month) {
  return `${month}-01`
}

// Day 0 of next month = last day of this month.
function monthEndDate(month) {
  const [y, m] = month.split('-').map(Number)
  const lastDay = new Date(Date.UTC(y, m, 0)).getUTCDate()
  return `${month}-${pad2(lastDay)}`
}

function debounce(fn, delay) {
  let timer
  return (...args) => {
    clearTimeout(timer)
    timer = setTimeout(() => fn(...args), delay)
  }
}

function dateToMonth(dateStr) {
  return dateStr.slice(0, 7)
}

function subtractMonths(month, n) {
  let [y, m] = month.split('-').map(Number)
  m -= n
  while (m < 1) {
    m += 12
    y -= 1
  }
  return `${y}-${pad2(m)}`
}

// Whole months between two "YYYY-MM" bounds, inclusive.
function monthsBetween(startMonth, endMonth) {
  const [sy, sm] = startMonth.split('-').map(Number)
  const [ey, em] = endMonth.split('-').map(Number)
  return (ey - sy) * 12 + (em - sm) + 1
}

// Manifest coverage is month-granular, so day pickers are bounded to whole
// edge months. A permalink's start/end take priority over these defaults.
function setupDateRangeInputs(manifest, permalink) {
  const minMonth = manifest.date_range?.min_month
  const maxMonth = manifest.date_range?.max_month
  if (!minMonth || !maxMonth) return

  const minDate = monthStartDate(minMonth)
  const maxDate = monthEndDate(maxMonth)

  for (const el of [startDateEl, endDateEl]) {
    el.min = minDate
    el.max = maxDate
  }

  const defaultStartMonth = subtractMonths(maxMonth, DEFAULT_MONTHS_SPAN - 1)
  const defaultStartDate = monthStartDate(defaultStartMonth) < minDate ? minDate : monthStartDate(defaultStartMonth)

  startDateEl.value = permalink.start || defaultStartDate
  endDateEl.value = permalink.end || maxDate
}

function updatePermalink(map) {
  const center = map.getCenter()
  writePermalink({
    lng: center.lng,
    lat: center.lat,
    zoom: map.getZoom(),
    start: startDateEl.value,
    end: endDateEl.value,
    log: logScaleEl.checked,
  })
}

// Single source of truth for the status text and runQuery()'s gate check.
function checkGuardrails(map) {
  const startDateStr = startDateEl.value
  const endDateStr = endDateEl.value

  if (!startDateStr || !endDateStr) {
    return { ok: false, message: 'No data available for the selected range.' }
  }
  if (startDateStr > endDateStr) {
    return { ok: false, message: 'Start date must not be after end date.' }
  }

  const spanMonths = monthsBetween(dateToMonth(startDateStr), dateToMonth(endDateStr))

  const zoom = map.getZoom()
  if (zoom < MIN_ZOOM) {
    return {
      ok: false,
      message: `Zoom in more to query (current zoom ${zoom.toFixed(1)}, need at least ${MIN_ZOOM}).`,
    }
  }

  return {
    ok: true,
    message: `Ready to query (zoom ${zoom.toFixed(1)}, ${spanMonths} month${spanMonths > 1 ? 's' : ''}).`,
  }
}

function refreshStatus(map) {
  const { message } = checkGuardrails(map)
  setStatus(message)
}

let queryInFlight = false
let queryQueued = false

async function runQuery(map, manifest) {
  // Re-entrancy guard: auto-sync fires on every map/date change, which
  // can overlap a still-running fetch. Queue one follow-up instead of
  // spawning parallel queries that could render out of order.
  if (queryInFlight) {
    queryQueued = true
    return
  }

  const { ok, message } = checkGuardrails(map)
  if (!ok) {
    setStatus(message)
    return
  }

  const startDateStr = startDateEl.value
  const endDateStr = endDateEl.value
  const startMonth = dateToMonth(startDateStr)
  const endMonth = dateToMonth(endDateStr)

  const bbox = getViewportBbox(map)
  const hexCells = bboxToCells(bbox, manifest.h3_resolution)
  const { min: cellMin, max: cellMax, set: cellSet } = cellsMinMaxSet(hexCells)

  // Partition files are selected per calendar month (startMonth..endMonth);
  // day-level precision is applied by this startDate/endDate range inside
  // each fetched file's parquetQuery call.
  const startDate = new Date(`${startDateStr}T00:00:00Z`)
  const endDate = new Date(`${endDateStr}T00:00:00Z`)

  setStatus(`Querying ${hexCells.length} H3 cells across ${startDateStr}..${endDateStr}...`)

  queryInFlight = true
  try {
    const { byCell, byDay } = await queryChanges({
      baseUrl: BASE_URL,
      manifest,
      cellMin,
      cellMax,
      cellSet,
      startDate,
      endDate,
      startMonth,
      endMonth,
    })

    renderResults(map, byCell, logScaleEl.checked)
    setHistogramData(byDay, startDateStr, endDateStr)

    const total = [...byCell.values()].reduce((a, b) => a + b, 0)
    setStatus(`${byCell.size} cells, ${total} total changes.`)
  } catch (err) {
    console.error(err)
    setStatus(`Query failed: ${err.message}`)
  } finally {
    queryInFlight = false
    if (queryQueued) {
      queryQueued = false
      runQuery(map, manifest)
    }
  }
}

async function main() {
  setStatus('Loading manifest...')

  let manifest
  try {
    manifest = await loadManifest(BASE_URL)
  } catch (err) {
    setStatus(`Failed to load manifest.json from ${BASE_URL}. Is "docker compose up caddy" running? (${err.message})`)
    return
  }

  const permalink = readPermalink()
  logScaleEl.checked = permalink.log !== false

  try {
    setupDateRangeInputs(manifest, permalink)

    const map = initMap('map', permalink)
    initHistogram(histogramEl, manifest)
    setHistogramWindow(startDateEl.value, endDateEl.value)

    // Display-only state is idempotent to restore before any data is shown.
    setLogScale(logScaleEl.checked)
    setResultsLogScale(map, logScaleEl.checked)

    // Log/linear toggle shared by the histogram y-axis and the map color
    // ramp; display-only (no re-query), but persisted in the permalink.
    logScaleEl.addEventListener('change', () => {
      const log = logScaleEl.checked
      setLogScale(log)
      setResultsLogScale(map, log)
      updatePermalink(map)
    })

    // Re-query whenever the map view or date range changes (debounced, no
    // query button).
    const syncQuery = debounce(() => runQuery(map, manifest), 300)

    // Live date 'input' sync: status + histogram window, no permalink/query.
    const onDateInput = () => {
      refreshStatus(map)
      setHistogramWindow(startDateEl.value, endDateEl.value)
    }
    // Persist to the permalink and schedule the debounced re-query.
    const onDateChange = () => {
      updatePermalink(map)
      syncQuery()
    }

    onHistogramRangeChange((start, end) => {
      startDateEl.value = start
      endDateEl.value = end
      refreshStatus(map)
      onDateChange()
    })

    map.on('move', () => refreshStatus(map))
    map.on('moveend', onDateChange)
    startDateEl.addEventListener('input', onDateInput)
    endDateEl.addEventListener('input', onDateInput)
    startDateEl.addEventListener('change', onDateChange)
    endDateEl.addEventListener('change', onDateChange)

    map.on('load', () => {
      refreshStatus(map)
      runQuery(map, manifest)
    })
    refreshStatus(map) // 'load' may have fired before this listener was attached
  } catch (err) {
    console.error(err)
    setStatus(`Setup failed: ${err.message} (see console for details)`)
  }
}

main()
