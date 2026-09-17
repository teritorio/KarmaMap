// Users viewer: looks up an OSM username directly in user_reputation.parquet
// (the pipeline stamps the current username per uid), fetches that user's
// exact reputation row and their per-day rows from user_indicators.parquet,
// and renders the OSMPatrol reputation, raw indicator totals, the profile
// identity fields and an edit-activity timeline. Same architecture as the
// changes viewer (page + app + query + histogram + permalink modules), but
// no spatial component.

import { loadManifest } from './manifest.js'
import { readPermalink, writePermalink } from './permalink.js'
import {
  queryReputationByUsername, queryIndicators, computeScores,
  dayKey, TAG_COUNTERS, REP_CAPS, REP_FORMULA,
} from './query.js'
import { initHistogram, setHistogramData, setLogScale } from './histogram.js'

// Data root: the data/ directory one level above the viewer pages.
const BASE_URL = '../data'

const usernameEl = document.getElementById('username')
const searchBtn = document.getElementById('search')
const statusEl = document.getElementById('status')
const profileEl = document.getElementById('profile')
const scoreEl = document.getElementById('score')
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

// Score tables grouped by reputation-formula aspect (paper §4), plus the
// change counters that fall outside the reputation (mods/deletes). Each
// item: [key, label, desc, role].
const SCORE_GROUPS = [
  {
    title: `Created objects — nodes (${REP_CAPS.node} pts)`,
    desc: 'Visible version-1 nodes feed the reputation aspect, capped at the weight and scored by the user\u2019s percentile rank among contributors active on created nodes.',
    cards: [['node_created', 'Node created', 'Visible nodes at their creation (version-1 versions).', `${REP_CAPS.node} pts`]],
  },
  {
    title: `Created objects — ways (${REP_CAPS.way} pts)`,
    desc: 'Visible version-1 ways feed the reputation aspect, capped at the weight and scored by the user\u2019s percentile rank among contributors active on created ways.',
    cards: [['way_created', 'Way created', 'Visible ways at their creation (version-1 versions).', `${REP_CAPS.way} pts`]],
  },
  {
    title: `Created objects — relations (${REP_CAPS.relation} pts)`,
    desc: 'Visible version-1 relations. Reputation-only for the score (\u00a74); counted in the activity timeline.',
    cards: [['relation_created', 'Relation created', 'Relations at their creation (visible, version 1). Reputation-only for the score, counted in the day timeline.', `${REP_CAPS.relation} pts`]],
  },
  {
    title: 'Top12 tags \u2014 4 pts each',
    desc: 'Tags used at each object\u2019s creation (\u00a74; the paper\u2019s \u201caddress\u201d is replaced by \u201cplace\u201d). Each tag is capped at 4 points and scored by the user\u2019s percentile rank among contributors using that tag at creation.',
    cards: TAG_COUNTERS.map((key) => [key, `Tag ${key.slice(4)}`, `Created nodes/ways/relations carrying the top-level key \u201c${key.slice(4)}\u201d (Top12 reputation aspect).`, '4 pts']),
  },
  {
    title: 'Other counters',
    desc: 'Modifications and deletions of nodes and ways. Not part of the reputation.',
    cards: [
      ['node_modified', 'Node modified', 'Visible node versions edited after creation (version > 1).', 'excluded'],
      ['node_deleted', 'Node deleted', 'Node versions deleted or hidden (invisible versions).', 'excluded'],
      ['way_modified', 'Way modified', 'Visible way versions edited after creation (version > 1).', 'excluded'],
      ['way_deleted', 'Way deleted', 'Way versions deleted or hidden (invisible versions).', 'excluded'],
    ],
  },
]

// Relative reputation-detail keys map to their full counter keys.
const DETAIL_COUNTER = { node: 'node_created', way: 'way_created', relation: 'relation_created' }

function renderProfile(name, scores) {
  const fields = [
    ['OSM user', name],
    ['uid', String(scores.uid)],
    ['First seen', dayKey(scores.firstSeenDay)],
  ]
    .map(([label, value]) =>
      `<div class="field"><strong>${escapeHtml(label)}</strong><span>${escapeHtml(value)}</span></div>`)
    .join('')
  profileEl.innerHTML = `${fields}`
}

function renderScore(scores) {
  const { value, max, note } = scores.reputation
  scoreEl.innerHTML = `
    <div class="headline">
      <span class="value">${value}</span>
      <span class="of">/ ${max} reputation</span>
    </div>
    <div class="note">${escapeHtml(note)}</div>
    <div class="formula">${escapeHtml(REP_FORMULA)}</div>
    <div class="formula-note">P(x) = percentile rank among contributors active on that aspect; each aspect is capped at its paper weight</div>`
}

function renderScores(scores) {
  const repByCounter = new Map(
    scores.reputation.details.map((d) => [DETAIL_COUNTER[d.key] ?? d.key, d]),
  )
  const roleCell = (key, staticRole) => {
    const d = repByCounter.get(key)
    if (!d) return `<td class="role">${staticRole}</td>`
    const pct = Math.floor(d.pct)
    const pctLabel = d.raw <= 0 ? 'no activity'
      : d.active === 1 ? 'only contributor'
        : d.pct >= 100 ? 'best'
          : d.pct <= 0 ? 'lowest'
            : `above ${pct}%`
    const rankNote = d.raw <= 0 ? 'no activity on this aspect'
      : d.active === 1
        ? `the only contributor on this aspect (dataset max ${d.max.toLocaleString()})`
        : d.pct >= 100
          ? `top among ${d.active.toLocaleString()} contributors on this aspect (dataset max ${d.max.toLocaleString()})`
          : d.pct <= 0
            ? `lowest on this aspect (${d.active.toLocaleString()} active)`
            : `above ${pct}% of ${d.active.toLocaleString()} contributors on this aspect (dataset max ${d.max.toLocaleString()})`
    return `<td class="role" title="${rankNote}">${d.points} pts \u00b7 ${pctLabel}</td>`
  }
  const parts = [
    `<section class="score-group">` +
      `<h3>Activity</h3>` +
      `<p class="desc">Sum of the six node/way change counters; the activity graph counts relation creations too, but relations score nothing in the reputation.</p>` +
      `<table class="counter-table">` +
      `<thead><tr><th>Counter</th><th class="value">Value</th><th class="role">Reputation</th></tr></thead>` +
      `<tbody><tr>` +
      `<td class="name"><span class="counter-name">Total edits</span><span class="count-desc">Created + modified + deleted nodes and ways across the whole timeline.</span></td>` +
      `<td class="value">${scores.totalEdits.toLocaleString()}</td>` +
      `<td class="role"></td></tr></tbody>` +
      `</table></section>`,
  ]
  for (const group of SCORE_GROUPS) {
    const rows = group.cards.map(([key, label, desc, role]) =>
      `<tr><td class="name"><span class="counter-name">${label}</span><span class="count-desc">${desc}</span></td>` +
      `<td class="value">${scores.counters[key].toLocaleString()}</td>` +
      `${roleCell(key, role)}</tr>`).join('')
    parts.push(
      `<section class="score-group"><h3>${group.title}</h3>` +
      `<p class="desc">${group.desc}</p>` +
      `<table class="counter-table"><thead><tr><th>Counter</th><th class="value">Value</th><th class="role">Reputation</th></tr></thead>` +
      `<tbody>${rows}</tbody></table></section>`,
    )
  }
  scoresEl.innerHTML = parts.join('')
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
    const repDataset = manifest.datasets.user_reputation
    const { rows: reps, stats } = repDataset
      ? await queryReputationByUsername(BASE_URL, repDataset.path, name)
      : { rows: [], stats: {} }
    if (reps.length === 0) {
      profileEl.innerHTML = ''
      scoreEl.innerHTML = ''
      scoresEl.innerHTML = ''
      setHistogramData(new Map())
      setStatus(`No profile found for "${name}".`)
      return
    }

    const uids = [...new Set(reps.map((p) => p.uid))]
    const indicators = await queryIndicators(BASE_URL, manifest.datasets.user_indicators.path, uids)
    const scores = computeScores(reps, indicators, stats)

    renderProfile(name, scores)
    renderScore(scores)
    renderScores(scores)
    setHistogramData(scores.byDay)
    setStatus(`${name}: reputation ${scores.reputation.value}, ${scores.totalEdits} edits across ${scores.byDay.size} active day${scores.byDay.size === 1 ? '' : 's'}.`)
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
    setStatus(`Failed to load manifest.json from ${BASE_URL}. Is the data server running? (${err.message})`)
    return
  }

  initHistogram(timelineEl)

  const { user } = readPermalink()
  if (user) usernameEl.value = user

  const datasets = manifest.datasets ?? {}
  if (!datasets.user_reputation || !datasets.user_indicators) {
    setStatus('user_reputation/user_indicators not in manifest — rerun the pipeline with --user-indicators.')
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
