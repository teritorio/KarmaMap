// Reads/writes the page's permalink state (the searched OSM username) to
// the URL hash, e.g. "#user=alice". Mirrors the changes viewer's hash
// handling (replaceState, no history flooding).

function currentParams() {
  return new URLSearchParams(window.location.hash.slice(1))
}

export function readPermalink() {
  return { user: currentParams().get('user') ?? '' }
}

export function writePermalink(user) {
  const params = new URLSearchParams()
  if (user) params.set('user', user)
  history.replaceState(null, '', `#${params.toString()}`)
}
