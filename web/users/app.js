// Users viewer: looks up an OSM username in user_profiles.parquet, fetches
// that user's daily rows from user_indicators.parquet, and renders raw
// indicator totals plus an edit-activity timeline. Same architecture as the
// changes viewer (page + app + query + histogram + permalink modules), but
// no spatial component.

import { loadManifest } from './manifest.js'
import { readPermalink, writePermalink } from './permalink.js'
import { queryProfiles, queryIndicators, computeScores, dayKey } from './query.js'
import { initHistogram, setHistogramData, setLogScale } from './histogram.js'

// Where the Caddy service serves output-dir/.
const BASE_URL = 'http://localhost:8080'

const usernameEl = document.getElementById('username')
const searchBtn = document.getElementById('search')
const statusEl = document.getElementById('status')
const profileEl = document.getElementById('profile')
const scoresEl = document.getElementById('scores')
const timelineEl = document.getElementById('timeline')

function setStatus(text) {
  statusEl.textContent = text
}

function escapeHtml(text) {
  return text.replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c])
}

const SCORE_LABELS = [
  ['node_created', 'Node created'],
  ['node_modified', 'Node modified'],
  ['node_deleted', 'Node deleted'],
  ['way_created', 'Way created'],
  ['way_modified', 'Way modified'],
  ['way_deleted', 'Way deleted'],
  ['relocated', 'Relocated'],
  ['short_lived', 'Short-lived'],
  ['rapid_edit', 'Rapid edits'],
]

function renderProfile(scores) {
  const badge = scores.bulkNewUser
    ? '<span class="badge badge-yes">yes</span>'
    : '<span class="badge badge-no">no</span>'
  profileEl.innerHTML = [
    `<div class="field"><strong>username</strong><span>${escapeHtml(usernameEl.value.trim())}</span></div>`,
    `<div class="field"><strong>uid</strong><span>${scores.uid ?? '-'}</span></div>`,
    `<div class="field"><strong>first seen (UTC)</strong><span>${scores.firstSeenDay != null ? dayKey(scores.firstSeenDay) : '-'}</span></div>`,
    `<div class="field"><strong>bulk new user</strong><span>${badge}</span></div>`,
  ].join('')
}

function renderScores(scores) {
  const cards = [
    `<div class="card total"><div class="label">Total edits</div><div class="value">${scores.totalEdits.toLocaleString()}</div></div>`,
  ]
  for (const [key, label] of SCORE_LABELS) {
    cards.push(
      `<div class="card"><div class="label">${label}</div><div class="value">${scores.counters[key].toLocaleString()}</div></div>`,
    )
  }
  scoresEl.innerHTML = cards.join('')
}

let inFlight = false

async function search(manifest) {
  const name = usernameEl.value.trim()
  if (!name) {
    setStatus('Enter an OSM username.')
    return
  }
  if (inFlight) return
  inFlight = true
  writePermalink(name)
  setStatus(`Looking up user ${name}...`)

  try {
    const profiles = await queryProfiles(BASE_URL, manifest.datasets.user_profiles.path, name)
    if (profiles.length === 0) {
      profileEl.innerHTML = ''
      scoresEl.innerHTML = ''
      setHistogramData(new Map())
      setStatus(`No profile found for "${name}".`)
      return
    }

    const uids = [...new Set(profiles.map((p) => p.uid))]
    const indicators = await queryIndicators(BASE_URL, manifest.datasets.user_indicators.path, uids)
    const scores = computeScores(profiles, indicators)

    renderProfile(scores)
    renderScores(scores)
    setHistogramData(scores.byDay)
    setStatus(`${name}: ${scores.totalEdits} edits across ${scores.byDay.size} active day${scores.byDay.size === 1 ? '' : 's'}.`)
  } catch (err) {
    console.error(err)
    setStatus(`Query failed: ${err.message}`)
  } finally {
    inFlight = false
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

  initHistogram(timelineEl)

  const { user } = readPermalink()
  if (user) usernameEl.value = user

  const datasets = manifest.datasets ?? {}
  if (!datasets.user_profiles || !datasets.user_indicators) {
    setStatus('user_profiles/user_indicators not in manifest — rerun the pipeline with --user-indicators.')
    return
  }

  // Log/linear toggle for the histogram y-axis; display-only.
  const logScaleBtn = document.createElement('label')
  logScaleBtn.innerHTML = '<input type="checkbox" id="log-scale" checked /> Log scale'
  document.getElementById('controls').insertBefore(logScaleBtn, statusEl)
  logScaleBtn.querySelector('input').addEventListener('change', (e) => {
    setLogScale(e.target.checked)
  })
  setLogScale(true)

  searchBtn.addEventListener('click', () => search(manifest))
  usernameEl.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') search(manifest)
  })

  if (usernameEl.value) search(manifest)
}

main()