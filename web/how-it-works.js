// Renders the guide Markdown as HTML. The Markdown files stay the single
// source of truth: this page fetches one and renders it in the browser, so
// editing a .md needs no build step and the two cannot drift.
//
// The document is chosen with ?doc=changes|users|suspect, defaulting to the
// overview, so each site page can link straight to the guide for its own
// viewer.
//
// markdown-it's default `html: false` escapes any raw HTML in the source
// rather than passing it through, and its link validation rejects
// javascript: URLs, so the rendered output is safe to assign to innerHTML
// without a sanitizer. linkify stays off so the output matches the source
// exactly (no auto-linking of prose like "openstreetmap.org").

import MarkdownIt from 'markdown-it'

const DOCS = {
  '': {
    file: './HOW_IT_WORKS.md',
    title: 'How it works — KarmaMap',
    description: 'What each KarmaMap page shows and what its numbers mean: the changes map, the 0-100 contributor ranking, and the suspect flagged days.',
  },
  changes: {
    file: './changes/HOW_IT_WORKS.md',
    title: 'The changes map — KarmaMap',
    description: 'The KarmaMap changes map: hexagonal cells shaded by how many map edits happened in them, and what a cell number counts.',
  },
  users: {
    file: './users/HOW_IT_WORKS.md',
    title: "The contributor's profile — KarmaMap",
    description: "The KarmaMap contributor's profile: the 0-100 ranking, the raw counts behind it, and the day-by-day activity timeline.",
  },
  suspect: {
    file: './suspect/HOW_IT_WORKS.md',
    title: 'Suspect flagged days — KarmaMap',
    description: 'The KarmaMap suspects page: the 100 most recent days a review filter flagged, what each column means, and the filters behind them.',
  },
}

const md = new MarkdownIt({ html: false, linkify: false, typographer: false })

const contentEl = document.getElementById('content')

// A link to another guide is written as a plain relative link in the source,
// so it also works in the raw file. Here it points at the rendered page. The
// four guides share one file name, so a link is matched on the path it
// resolves to, not on its file name.
function docPaths(page) {
  const paths = new Map()
  for (const [key, doc] of Object.entries(DOCS)) {
    paths.set(new URL(doc.file, page).pathname, key)
  }
  return paths
}

// Section ids, so a heading can be linked to directly.
function slugify(text) {
  return text
    .toLowerCase()
    .replace(/`/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

// Post-process the rendered DOM: wrap tables so they scroll instead of
// overflowing on a narrow screen, id the headings, point cross-guide links
// at the rendered pages, and send absolute links to a new tab (the viewers
// do the same for outbound OSM links).
function enhance(html, base) {
  const frag = document.createElement('div')
  frag.innerHTML = html

  for (const table of frag.querySelectorAll('table')) {
    const wrap = document.createElement('div')
    wrap.className = 'table-wrap'
    table.replaceWith(wrap)
    wrap.append(table)
  }

  for (const heading of frag.querySelectorAll('h2, h3')) {
    const id = slugify(heading.textContent)
    if (id) heading.id = id
  }

  const docs = docPaths(location.href)
  for (const link of frag.querySelectorAll('a[href]')) {
    const href = link.getAttribute('href')
    if (href.startsWith('http')) {
      link.target = '_blank'
      link.rel = 'noopener noreferrer'
      continue
    }
    if (href.startsWith('#')) continue
    const [path, hash] = href.split('#')
    const key = docs.get(new URL(path, base).pathname)
    if (key !== undefined) link.href = `?doc=${key}${hash ? `#${hash}` : ''}`
  }

  return frag
}

// The heading ids only exist once the document is rendered, so the browser
// cannot scroll to a deep link on its own.
function scrollToHash() {
  if (!location.hash) return
  const target = document.getElementById(decodeURIComponent(location.hash.slice(1)))
  if (target) target.scrollIntoView()
}

function setMeta(attr, value, content) {
  const el = document.querySelector(`meta[${attr}="${value}"]`)
  if (el) el.setAttribute('content', content)
}

function currentDoc() {
  const key = new URLSearchParams(location.search).get('doc') ?? ''
  return Object.hasOwn(DOCS, key) ? [key, DOCS[key]] : ['', DOCS['']]
}

async function main() {
  const [key, doc] = currentDoc()

  document.title = doc.title
  setMeta('name', 'description', doc.description)
  setMeta('property', 'og:title', doc.title)
  setMeta('property', 'og:description', doc.description)

  const alt = document.querySelector('link[rel="alternate"][type="text/markdown"]')
  if (alt) alt.href = doc.file

  const source = document.getElementById('doc-source')
  if (source) {
    source.href = doc.file
    source.textContent = doc.file.replace(/^\.\//, '')
  }

  for (const link of document.querySelectorAll('.doc-nav a[data-doc]')) {
    if (link.dataset.doc === key) link.setAttribute('aria-current', 'page')
    else link.removeAttribute('aria-current')
  }

  try {
    const res = await fetch(doc.file)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const frag = enhance(md.render(await res.text()), new URL(doc.file, location.href))
    // Unwrap the scratch div so the DOM matches the Markdown structure.
    contentEl.replaceChildren(...frag.childNodes)
    scrollToHash()
  } catch (err) {
    console.error(err)
    contentEl.replaceChildren()
    const p = document.createElement('p')
    p.className = 'error'
    p.textContent = `Could not load the guide (${err.message}). `
    const link = document.createElement('a')
    link.href = doc.file
    link.textContent = 'Open the Markdown source'
    p.append(link, '.')
    contentEl.append(p)
  } finally {
    contentEl.removeAttribute('aria-busy')
  }
}

main()
