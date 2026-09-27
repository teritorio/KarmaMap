// Viewer header: the extract badge (which extract the data covers, and the
// date it was last updated), read from the manifest.

import { extractInfo } from './api.js'

export function showExtractInfo(manifest) {
  const el = document.getElementById('extract-info')
  if (!el) return

  const { region, name, updatedOn, detail } = extractInfo(manifest)
  const parts = []
  if (name) parts.push(region ? `${region} / ${name}` : name)
  if (updatedOn) parts.push(`updated ${updatedOn}`)
  el.textContent = parts.join(' · ')
  el.title = detail
}
