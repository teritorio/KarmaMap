// Suspects viewer: shows the latest flagged (uid, change_date) days from
// suspect.parquet, the update-only re-export of the non-zero suspect_flag
// rows of users_history.parquet. The file is written newest-first (change_date
// descending), so this page reads only the leading rows/pages for the 100 most
// recent flagged days — one row per day with the day's total change count and
// the value each review filter tripped on: the peak edit-burst hour (filter 2:
// more than 500 modified/deleted objects in one hour), the peak spatial spread
// (filter 4, the local extension: 20+ edits in one hour across 3+ H3 cells
// spanning 20 km²), the mass tag keys (filter 5: every key on >90% of an hour's
// ≥100 modified/deleted objects), the far-move count with the largest of those
// moves (filter 3: more than 500 m), the countries the day's cells touch, and
// the ranking frozen at the day's first flag. Every value is stored, so a
// filter that did not fire reads as an empty
// cell. Same architecture as the users/changes viewers (page + app + query over
// the shared lib in web/lib).

import { loadManifest, dayKey, userProfileUrls } from '../lib/api.js'
import { showExtractInfo } from '../lib/header.js'
import { querySuspectLatest } from './query.js'

// Data root: the data/ directory one level above the viewer pages.
const BASE_URL = '../data'

// Number of latest flagged days shown.
const LATEST = 100

const statusEl = document.getElementById('status')
const tbodyEl = document.querySelector('#suspect tbody')

const filterEl = document.getElementById('country-filter')

let allRows = []

function setStatus(text) {
  statusEl.textContent = text
  statusEl.title = text
}

function escapeHtml(text) {
  return text.replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c])
}

function renderUserLinks(name) {
  if (name.startsWith('<') && name.endsWith('>')) return escapeHtml(name)
  const { osm, hdyc } = userProfileUrls(name)
  return `<a href="${osm}" target="_blank" rel="noopener noreferrer">${escapeHtml(name)}</a> ` +
    `(<a href="${hdyc}" target="_blank" rel="noopener noreferrer">hdyc➚</a>)`
}

// A metric column: 0 (or an empty tag list) means the filter never fired, so
// the cell stays empty rather than showing a zero the reviewer would have to
// read as a measurement.
function renderEditsPerHour(row) {
  const n = Number(row.max_edits_per_hour ?? 0)
  return n > 0 ? `<td class="value">${n.toLocaleString()}</td>` : '<td></td>'
}

function renderNameLangEvents(row) {
  const n = Number(row.name_lang_events ?? 0)
  return n > 0 ? `<td class="value">${n.toLocaleString()}</td>` : '<td></td>'
}

function renderSpread(row) {
  const km2 = Number(row.peak_spread_km2 ?? 0)
  return km2 > 0 ? `<td class="value">${Math.round(km2).toLocaleString()} km&sup2;</td>` : '<td></td>'
}

// changed_keys and countries are list<utf8>, so hyparquet may hand back the
// row's values flat or wrapped in the list's single intermediate group. Both
// are flattened to one level of strings before they are joined.
function flattenList(value) {
  if (!Array.isArray(value)) return []
  const flat = []
  for (const item of value) {
    if (Array.isArray(item)) {
      for (const nested of item) {
        if (typeof nested === 'string') flat.push(nested)
      }
    } else if (typeof item === 'string') {
      flat.push(item)
    }
  }
  return flat
}

function renderTags(row) {
  const keys = flattenList(row.changed_keys)
  return keys.length === 0 ? '<td></td>' : `<td>${escapeHtml(keys.join(', '))}</td>`
}

// An ISO alpha-2 code as its flag emoji (regional indicators) plus the code.
function countryLabel(code) {
  if (!/^[A-Za-z]{2}$/.test(code)) return code
  const upper = code.toUpperCase()
  const base = 0x1F1E6
  return String.fromCodePoint(base + upper.charCodeAt(0) - 65) +
    String.fromCodePoint(base + upper.charCodeAt(1) - 65) + ` ${upper}`
}

function renderCountries(row) {
  const codes = flattenList(row.countries)
  return codes.length === 0
    ? '<td></td>'
    : `<td>${escapeHtml(codes.map(countryLabel).join(', '))}</td>`
}

function renderFarMoves(row) {
  const n = Number(row.far_move_count ?? 0)
  return n > 0 ? `<td class="value">${n.toLocaleString()}</td>` : '<td class="value"></td>'
}

function renderMaxMove(row) {
  const m = Number(row.max_move_meters ?? 0)
  if (m <= 0) return '<td class="value"></td>'
  return m >= 1000
    ? `<td class="value">${(m / 1000).toFixed(1)} km</td>`
    : `<td class="value">${m.toLocaleString()} m</td>`
}

function renderRows(rows) {
  tbodyEl.innerHTML = rows.map((row) => `<tr>` +
    `<td>${escapeHtml(dayKey(row.change_date))}</td>` +
    renderCountries(row) +
    `<td class="value"><a href="../users/#user=${row.username}">#${Number(row.ranking_at_day ?? 0)}</a></td>` +
    `<td class="user">${renderUserLinks(row.username)}</td>` +
    `<td class="value">${Number(row.uid)}</td>` +
    `<td class="value">${Number(row.changes ?? 0).toLocaleString()}</td>` +
    renderEditsPerHour(row) +
    renderNameLangEvents(row) +
    renderSpread(row) +
    renderTags(row) +
    renderFarMoves(row) +
    renderMaxMove(row) +
    `</tr>`).join('')
}

// Applies all column filters. Each control declares its filter semantics
// through data-filter-type/data-bound, so a column can combine several
// inputs (e.g. a numeric range).
function applyFilter() {
  const controls = Array.from(document.querySelectorAll('[data-col]'))
  const specs = {}
  for (const control of controls) {
    const col = control.dataset.col
    if (!specs[col]) specs[col] = {}
    const type = control.dataset.filterType || 'text'
    const bound = control.dataset.bound || 'value'
    if (control.tagName === 'SELECT') {
      specs[col].selectValue = control.value
      specs[col].type = 'country'
    } else {
      specs[col].type = type
      if (type === 'range') {
        specs[col][bound] = control.value
      } else {
        specs[col].value = control.value
      }
    }
  }

  const rows = allRows.filter((row) => Object.entries(specs).every(([col, spec]) => {
    switch (spec.type) {
      case 'date': {
        const value = (spec.value ?? '').trim()
        if (value === '') return true
        return dayKey(row.change_date) === value
      }
      case 'country': {
        const choice = spec.selectValue ?? ''
        if (choice === '' || choice === 'ALL') return true
        if (choice === '__NONE__') return flattenList(row.countries).length === 0
        return flattenList(row.countries).includes(choice)
      }
      case 'text': {
        const q = (spec.value ?? '').trim().toLowerCase()
        if (q === '') return true
        const haystack = col === 'user'
          ? String(row.username ?? '')
          : flattenList(row.changed_keys).join(', ')
        return haystack.toLowerCase().includes(q)
      }
      case 'exact': {
        const q = (spec.value ?? '').trim()
        if (q === '') return true
        return String(row.uid ?? '') === q || Number(row.uid) === Number(q)
      }
      case 'min': {
        const q = (spec.value ?? '').trim()
        if (q === '') return true
        const value = col === 'changes' ? Number(row.changes ?? 0)
          : col === 'edits' ? Number(row.max_edits_per_hour ?? 0)
          : col === 'name_lang_events' ? Number(row.name_lang_events ?? 0)
          : col === 'spread' ? Number(row.peak_spread_km2 ?? 0)
          : col === 'farmoves' ? Number(row.far_move_count ?? 0)
          : col === 'maxmove' ? Number(row.max_move_meters ?? 0)
          : 0
        return value >= Number(q)
      }
      case 'range': {
        const min = (spec.min ?? '').trim()
        const max = (spec.max ?? '').trim()
        const value = Number(row.ranking_at_day ?? 0)
        if (min !== '' && value < Number(min)) return false
        if (max !== '' && value > Number(max)) return false
        return true
      }
      default:
        return true
    }
  }))

  renderRows(rows)
  syncUrlFromFilters()
  const countryQuery = (specs.countries?.selectValue ?? '').trim().toUpperCase()
  setStatus(
    allRows.length === 0
      ? 'No flagged days yet; the suspects dataset is empty.'
      : `Showing ${rows.length} of the ${allRows.length} latest suspect ` +
        `${allRows.length === 1 ? 'day' : 'days'}${countryQuery && countryQuery !== '__NONE__' ? ` in ${countryQuery}` : ''}.`)
}

// Text used to match a row against a column filter: includes both the raw
// value and its rendered form so filters accept either spelling.
function columnText(col, row) {
  switch (col) {
    case 'date':
      return dayKey(row.change_date)
    case 'countries': {
      const codes = flattenList(row.countries)
      return codes.map(countryLabel).join(', ') + ' ' + codes.join(' ')
    }
    case 'ranking':
      return `#${Number(row.ranking_at_day ?? 0)}`
    case 'user':
      return String(row.username ?? '')
    case 'uid':
      return String(row.uid ?? '')
    case 'changes': {
      const n = Number(row.changes ?? 0)
      return `${n} ${n.toLocaleString()}`
    }
    case 'edits': {
      const n = Number(row.max_edits_per_hour ?? 0)
      return n > 0 ? `${n} ${n.toLocaleString()}` : ''
    }
    case 'name_lang_events': {
      const n = Number(row.name_lang_events ?? 0)
      return n > 0 ? `${n} ${n.toLocaleString()}` : ''
    }
    case 'spread': {
      const km2 = Number(row.peak_spread_km2 ?? 0)
      return km2 > 0 ? `${km2} ${Math.round(km2)} km²` : ''
    }
    case 'tags':
      return flattenList(row.changed_keys).join(', ')
    case 'farmoves': {
      const n = Number(row.far_move_count ?? 0)
      return n > 0 ? `${n} ${n.toLocaleString()}` : ''
    }
    case 'maxmove': {
      const m = Number(row.max_move_meters ?? 0)
      if (m <= 0) return ''
      if (m >= 1000) return `${m} ${(m / 1000).toFixed(1)} km`
      return `${m} m`
    }
    default:
      return ''
  }
}

function paramName(control) {
  const col = control.dataset.col
  if (col === 'date') return 'date'
  if (col === 'countries') return 'country'
  if (col === 'user') return 'user'
  if (col === 'uid') return 'uid'
  if (col === 'tags') return 'tags'
  const type = control.dataset.filterType || 'text'
  const bound = control.dataset.bound || 'value'
  if (type === 'range') return bound === 'max' ? `${col}_max` : `${col}_min`
  if (type === 'min') return `${col}_min`
  return col
}

const FILTER_PARAM_NAMES = [
  'date', 'country', 'ranking_min', 'ranking_max', 'user', 'uid',
  'changes_min', 'edits_min', 'name_lang_events_min', 'spread_min', 'tags',
  'farmoves_min', 'maxmove_min',
]

function applyUrlToFilters() {
  const params = new URLSearchParams(window.location.search)
  for (const control of document.querySelectorAll('[data-col]')) {
    const key = paramName(control)
    if (params.has(key)) {
      control.value = params.get(key)
    } else if (control.tagName === 'SELECT') {
      control.value = ''
    } else {
      control.value = ''
    }
  }
}

function syncUrlFromFilters() {
  const url = new URL(window.location.href)
  for (const key of FILTER_PARAM_NAMES) url.searchParams.delete(key)
  for (const control of document.querySelectorAll('[data-col]')) {
    const value = String(control.value ?? '').trim()
    if (value === '') continue
    url.searchParams.set(paramName(control), value)
  }
  window.history.replaceState(null, '', url)
}

function populateCountryFilter() {
  const select = document.querySelector('select[data-col="countries"]')
  if (!select) return
  const existing = new Set(Array.from(select.options).map((opt) => opt.value))
  const codes = [...new Set(allRows.flatMap((row) => flattenList(row.countries)))]
    .sort((a, b) => String(a).localeCompare(String(b)))
  for (const code of codes) {
    if (existing.has(code)) continue
    const option = document.createElement('option')
    option.value = code
    option.textContent = code
    select.appendChild(option)
    existing.add(code)
  }
}

async function main() {
  setStatus('Loading manifest...')

  let manifest
  try {
    manifest = await loadManifest(BASE_URL)
  } catch (err) {
    setStatus(`Failed to load manifest.json from ${BASE_URL}. Is the data server running? (${err.message})`)
    return
  }

  showExtractInfo(manifest)

  const datasets = manifest.datasets ?? {}
  if (!datasets.suspect) {
    setStatus('suspects not in manifest — the pipeline did not produce the suspects dataset (a pure import writes no flags).')
    return
  }

  const dataset = datasets.suspect
  setStatus(`Loading the ${LATEST} latest suspect days...`)
  try {
    allRows = await querySuspectLatest(BASE_URL, dataset.path, dataset.footer_size, LATEST)
    populateCountryFilter()
    applyUrlToFilters()
    applyFilter()
  } catch (err) {
    console.error(err)
    setStatus(`Query failed: ${err.message}`)
  }
}

window.addEventListener('popstate', () => {
  applyUrlToFilters()
  applyFilter()
})

document.querySelectorAll('.col-filter').forEach((control) => {
  control.addEventListener('input', applyFilter)
  control.addEventListener('change', applyFilter)
})

main()
