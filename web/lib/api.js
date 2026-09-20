// Data-access helpers shared by the changes and users datasets: loading the
// manifest (written by the C++ pipeline) and converting the uint16
// change_date day counts, which are UTC days since the Unix epoch
// (1970-01-01), so day-range filters and day-to-key conversions both work
// on integer day counts.

// Fetches and parses {baseUrl}/manifest.json, so the frontend never has to
// guess the H3 resolution used, which years are available, or the exact
// date span.
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

// UTC day count from a midnight-UTC JS Date, matching the uint16
// change_date values stored in the Parquet files.
export function epochDay(date) {
  return Math.floor(date.getTime() / 86400000)
}

// "YYYY-MM-DD" key for a change_date day-count value.
export function dayKey(changeDate) {
  return new Date(Number(changeDate) * 86400000).toISOString().slice(0, 10)
}