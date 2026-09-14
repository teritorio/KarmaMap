// Reads/writes the page's permalink state (map view + date range + log
// scale) to the URL hash, e.g.
// "#lng=1.52&lat=42.51&zoom=12.00&start=2020-01-01&end=2020-02-01&log=1".
//
// MapLibre's built-in `hash: true` is not used — it owns the entire hash
// and conflicts with also storing the date range and log scale. Instead the
// hash is managed here in one consistent format.

function currentParams() {
  return new URLSearchParams(window.location.hash.slice(1))
}

export function readPermalink() {
  const params = currentParams()
  const lng = params.has('lng') ? Number(params.get('lng')) : null
  const lat = params.has('lat') ? Number(params.get('lat')) : null
  const zoom = params.has('zoom') ? Number(params.get('zoom')) : null
  const start = params.get('start')
  const end = params.get('end')
  // Log scale is the default (checkbox starts checked), so absent or
  // log=1 means log, and only log=0 means linear.
  const log = params.get('log') !== '0'
  return { lng, lat, zoom, start, end, log }
}

export function writePermalink({ lng, lat, zoom, start, end, log }) {
  const params = new URLSearchParams()
  if (Number.isFinite(lng)) params.set('lng', lng.toFixed(5))
  if (Number.isFinite(lat)) params.set('lat', lat.toFixed(5))
  if (Number.isFinite(zoom)) params.set('zoom', zoom.toFixed(2))
  if (start) params.set('start', start)
  if (end) params.set('end', end)
  params.set('log', log ? '1' : '0')

  // replaceState avoids flooding browser history on every map pan/zoom.
  history.replaceState(null, '', `#${params.toString()}`)
}
