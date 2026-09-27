// Suspects viewer: shows the latest flagged (uid, change_date) days from
// suspect.parquet, the update-only re-export of the non-zero suspect_flag
// rows of users_history.parquet. The file is written newest-first (change_date
// descending), so this page reads only the leading rows/pages for the 100 most
// recent flagged days — one row per day with the day's total change count, the
// edit-burst marker (filter 2: more than 500 modified/deleted objects in one
// hour), its far-move count and the ranking frozen at the day's first flag.
// Same architecture as the users/changes viewers (page + app + query over the
// shared lib in web/lib).

import { loadManifest, dayKey, userProfileUrls } from '../lib/api.js'
import { showExtractInfo } from '../lib/header.js'
import { querySuspectLatest } from './query.js'
import { FLAG_FILTER_2 } from '../users/ranking.js'

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

function renderRows(rows) {
  tbodyEl.innerHTML = rows.map((row) => {
    const editBurst = (Number(row.suspect_flag ?? 0) & FLAG_FILTER_2) !== 0
    return `<tr>` +
      `<td>${escapeHtml(dayKey(row.change_date))}</td>` +
      `<td class="value"><a href="../users/#user=${row.username}">#${Number(row.ranking_at_day ?? 0)}</a></td>` +
      `<td class="user">${renderUserLinks(row.username)}</td>` +
      `<td class="value">${Number(row.uid)}</td>` +
      `<td class="value">${Number(row.changes ?? 0).toLocaleString()}</td>` +
      `<td>${editBurst ? 'Yes' : ''}</td>` +
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
