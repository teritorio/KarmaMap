// Loads output-dir/manifest.json, written by the C++ pipeline
// (src/manifest.cpp), so the frontend never has to guess the H3
// resolution used, which years are available, or the exact date span.

export async function loadManifest(baseUrl) {
  const res = await fetch(`${baseUrl}/manifest.json`)
  if (!res.ok) {
    throw new Error(`Failed to load manifest.json: HTTP ${res.status}`)
  }
  return res.json()
}

// First and last day with data in the changes dataset (from date_range,
// which the C++ pipeline reads out of the data.parquet footers). Absent on
// an empty dataset, in which case no bounds can be set.
export function coverageDays(manifest) {
  const range = manifest.date_range
  if (!range?.min_date || !range?.max_date) return null
  return { minDate: range.min_date, maxDate: range.max_date }
}
