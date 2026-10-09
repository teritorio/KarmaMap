// Viewer header: the extract badge (which extract the data covers, and the
// date it was last updated), read from the manifest.

import { extractInfo } from './api.js'

export function showExtractInfo(manifest) {
  const el = document.getElementById('extract-info')
  if (!el) return

  const { region, name, updatedAt, detail } = extractInfo(manifest)
  const parts = []
  if (name) parts.push(region ? `${region} / ${name}` : name)
  if (updatedAt) parts.push(`updated ${updatedAt}`)
  el.textContent = parts.join(' · ')
  el.title = detail
}
