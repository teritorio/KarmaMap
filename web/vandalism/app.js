// Vandalism viewer: shows the latest flagged (uid, change_date) days from
// vandalism.parquet, the update-only re-export of the non-zero vandalism_flag
// rows of users_history.parquet. The file is written newest-first (change_date
// descending), so this page reads only the leading rows/pages for the 100 most
// recent flagged days — one row per day carrying the combined flag bits, the
// day's total change count, its far-move count and the reputation frozen at
// the day's first flag. Same architecture as the users/changes viewers (page +
// app + query over the shared lib in web/lib).

import { loadManifest, dayKey, userProfileUrls } from '../lib/api.js'
import { queryVandalismLatest } from './query.js'
import { FLAG_LABELS } from '../users/reputation.js'

// Data root: the data/ directory one level above the viewer pages.
const BASE_URL = '../data'

// Number of latest flagged days shown.
const LATEST = 100

const statusEl = document.getElementById('status')
const tbodyEl = document.querySelector('#vandalism tbody')

function setStatus(text) {
  statusEl.textContent = text
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

// One chip per active flag bit, colored by filter (mirrors the users
// viewer's history-chart labels). Both the chip title and the flags column
// list the screen names.
function flagChips(flags) {
  const active = FLAG_LABELS.filter((f) => flags & f.mask)
  if (active.length === 0) return '<span class="flags"></span>'
  const cls = { 0x01: 'f2', 0x02: 'f3', 0x04: 'f1' }
  return `<span class="flags">` +
    active.map((f) =>
      `<span class="flag-chip ${cls[f.mask]}" title="${escapeHtml(f.label)}">${f.label.split(':')[0]}</span>`).join('') +
    `</span>`
}

function renderRows(rows) {
  tbodyEl.innerHTML = rows.map((row) => {
    const flags = Number(row.vandalism_flag ?? 0)
    return `<tr>` +
      `<td>${escapeHtml(dayKey(row.change_date))}</td>` +
      `<td class="value"><a href="../users/#user=${row.username}">#${Number(row.reputation_at_day ?? 0)}</a></td>` +
      `<td class="user">${renderUserLinks(row.username)}</td>` +
      `<td class="value">${Number(row.uid)}</td>` +
      `<td class="value">${Number(row.changes ?? 0).toLocaleString()}</td>` +
      `<td>${flagChips(flags)}</td>` +
      `<td class="value">${Number(row.far_move_count ?? 0).toLocaleString()}</td>` +
      `</tr>`
  }).join('')
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

  const datasets = manifest.datasets ?? {}
  if (!datasets.vandalism) {
    setStatus('vandalism not in manifest — the pipeline did not produce the vandalism dataset (a pure import writes no flags).')
    return
  }

  const dataset = datasets.vandalism
  setStatus(`Loading the ${LATEST} latest vandalism days...`)
  try {
    const rows = await queryVandalismLatest(BASE_URL, dataset.path, dataset.footer_size, LATEST)
    renderRows(rows)
    setStatus(
      rows.length === 0
        ? 'No flagged days yet; the vandalism dataset is empty.'
        : `Showing the ${rows.length} latest vandalism ${rows.length === 1 ? 'day' : 'days'}.`)
  } catch (err) {
    console.error(err)
    setStatus(`Query failed: ${err.message}`)
  }
}

main()
