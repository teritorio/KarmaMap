// Renders HOW_IT_WORKS.md as HTML. The Markdown file stays the single
// source of truth: this page fetches it and renders it in the browser, so
// editing the .md needs no build step and the two cannot drift.
//
// markdown-it's default `html: false` escapes any raw HTML in the source
// rather than passing it through, and its link validation rejects
// javascript: URLs, so the rendered output is safe to assign to innerHTML
// without a sanitizer. linkify stays off so the output matches the source
// exactly (no auto-linking of prose like "openstreetmap.org").

import MarkdownIt from 'markdown-it'

const SRC = './HOW_IT_WORKS.md'

const md = new MarkdownIt({ html: false, linkify: false, typographer: false })

const contentEl = document.getElementById('content')

// Section ids, so a heading can be linked to directly.
function slugify(text) {
  return text
    .toLowerCase()
    .replace(/`/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

// Post-process the rendered DOM: wrap tables so they scroll instead of
// overflowing on a narrow screen, id the headings, and send absolute links
// to a new tab (the viewers do the same for outbound OSM links).
function enhance(html) {
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

  for (const link of frag.querySelectorAll('a[href^="http"]')) {
    link.target = '_blank'
    link.rel = 'noopener noreferrer'
  }

  return frag
}

async function main() {
  try {
    const res = await fetch(SRC)
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const frag = enhance(md.render(await res.text()))
    // Unwrap the scratch div so the DOM matches the Markdown structure.
    contentEl.replaceChildren(...frag.childNodes)
  } catch (err) {
    console.error(err)
    contentEl.replaceChildren()
    const p = document.createElement('p')
    p.className = 'error'
    p.textContent = `Could not load the guide (${err.message}). `
    const link = document.createElement('a')
    link.href = SRC
    link.textContent = 'Open the Markdown source'
    p.append(link, '.')
    contentEl.append(p)
  } finally {
    contentEl.removeAttribute('aria-busy')
  }
}

main()
