// Loads output-dir/manifest.json, written by the C++ pipeline
// (src/manifest.cpp), so the frontend never has to guess whether the
// user-indicator files exist or under which paths.

export async function loadManifest(baseUrl) {
  const res = await fetch(`${baseUrl}/manifest.json`)
  if (!res.ok) {
    throw new Error(`Failed to load manifest.json: HTTP ${res.status}`)
  }
  return res.json()
}
