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
// moves (filter 3: more than 500 m), and the ranking frozen at the day's first
// flag. Every value is stored, so a filter that did not fire reads as an empty
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

function renderSpread(row) {
  const km2 = Number(row.peak_spread_km2 ?? 0)
  return km2 > 0 ? `<td class="value">${Math.round(km2).toLocaleString()} km&sup2;</td>` : '<td></td>'
}

// changed_keys is a list<utf8>, so hyparquet may hand back the row's values
// flat or wrapped in the list's single intermediate group. Both are flattened
// to one level of strings before they are joined.
function flattenTags(value) {
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
  const keys = flattenTags(row.changed_keys)
  return keys.length === 0 ? '<td></td>' : `<td>${escapeHtml(keys.join(', '))}</td>`
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
    `<td class="value"><a href="../users/#user=${row.username}">#${Number(row.ranking_at_day ?? 0)}</a></td>` +
    `<td class="user">${renderUserLinks(row.username)}</td>` +
    `<td class="value">${Number(row.uid)}</td>` +
    `<td class="value">${Number(row.changes ?? 0).toLocaleString()}</td>` +
    renderEditsPerHour(row) +
    renderSpread(row) +
    renderTags(row) +
    renderFarMoves(row) +
    renderMaxMove(row) +
    `</tr>`).join('')
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
    const rows = await querySuspectLatest(BASE_URL, dataset.path, dataset.footer_size, LATEST)
    renderRows(rows)
    setStatus(
      rows.length === 0
        ? 'No flagged days yet; the suspects dataset is empty.'
        : `Showing the ${rows.length} latest suspect ${rows.length === 1 ? 'day' : 'days'}.`)
  } catch (err) {
    console.error(err)
    setStatus(`Query failed: ${err.message}`)
  }
}

main()
