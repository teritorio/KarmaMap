// Bbox + date-range query across the partitioned Parquet dataset: the date
// range selects the year partition files (intersected with what the
// manifest says exists on disk), each distinct file is queried once with
// hyparquet (row-group/page pruning on h3_cell and change_date), then
// filtered to the exact cell set (h3_cell only supports a contiguous
// range) and aggregated client-side across node_count + way_count.

import { parquetQuery, asyncBufferFromUrl } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

function monthsInRange(startMonth, endMonth) {
  const [sy, sm] = startMonth.split('-').map(Number)
  const [ey, em] = endMonth.split('-').map(Number)

  const months = []
  let y = sy
  let m = sm
  while (y < ey || (y === ey && m <= em)) {
    months.push(`${y}-${String(m).padStart(2, '0')}`)
    m += 1
    if (m > 12) {
      m = 1
      y += 1
    }
  }
  return months
}

function yearsInRange(startMonth, endMonth) {
  const years = new Set(monthsInRange(startMonth, endMonth).map((m) => m.slice(0, 4)))
  return [...years]
}

// Partition file URL for a year, matching the C++ writer's hive layout
// (root_dir/year=YYYY/data.parquet).
function partitionPath(datasetPath, year) {
  return `${datasetPath}/year=${year}/data.parquet`
}

// change_date is a uint16 count of UTC days since the Unix epoch
// (1970-01-01), so day-range filtering and day-to-key conversion both work
// on integer day counts.
async function queryOneFile(baseUrl, path, cellMin, cellMax, startDay, endDay, cellSet) {
  const url = `${baseUrl}/${path}`

  let file
  try {
    file = await asyncBufferFromUrl({ url })
  } catch (err) {
    // A fetch failure is "no data for this file", not a fatal query error.
    console.warn(`Skipping ${url}: ${err.message}`)
    return []
  }

  const rows = await parquetQuery({
    file,
    compressors,
    filter: {
      h3_cell: { $gte: cellMin, $lte: cellMax },
      change_date: { $gte: startDay, $lte: endDay },
    },
  })

  // The range filter is a coarse pre-filter; keep only rows in the exact
  // (non-contiguous) bbox cell set.
  return rows.filter((row) => cellSet.has(BigInt(row.h3_cell)))
}

// UTC day count from a midnight-UTC JS Date, matching the uint16
// change_date values stored in the Parquet files.
function epochDay(date) {
  return Math.floor(date.getTime() / 86400000)
}

// "YYYY-MM-DD" key for a change_date day-count value.
function dayKey(changeDate) {
  return new Date(Number(changeDate) * 86400000).toISOString().slice(0, 10)
}

// Returns byCell (Map<bigint, count>) and byDay (Map<"YYYY-MM-DD", count>),
// both summed across every dataset and every year in range.
export async function queryChanges({
  baseUrl,
  manifest,
  cellMin,
  cellMax,
  cellSet,
  startDate,
  endDate,
  startMonth,
  endMonth,
}) {
  const years = yearsInRange(startMonth, endMonth)

  const startDay = epochDay(startDate)
  const endDay = epochDay(endDate)

  const tasks = []
  for (const year of years) {
    for (const datasetName of Object.keys(manifest.datasets)) {
      const dataset = manifest.datasets[datasetName]
      // Non-partitioned datasets (e.g. the user-indicator files) declare an
      // empty partition list; they are not part of the year-partitioned
      // bbox query and are skipped entirely.
      if (!Array.isArray(dataset.partitions) || dataset.partitions.length === 0) continue
      if (!dataset.partitions.includes(year)) continue // no file for this year, skip the fetch entirely

      const path = partitionPath(dataset.path, year)
      tasks.push(queryOneFile(baseUrl, path, cellMin, cellMax, startDay, endDay, cellSet))
    }
  }

  const results = await Promise.all(tasks)

  const byCell = new Map()
  const byDay = new Map()
  for (const rows of results) {
    for (const row of rows) {
      const count = Number(row.node_count ?? 0) + Number(row.way_count ?? 0)

      const cell = BigInt(row.h3_cell)
      byCell.set(cell, (byCell.get(cell) ?? 0) + count)

      const day = dayKey(row.change_date)
      byDay.set(day, (byDay.get(day) ?? 0) + count)
    }
  }

  return { byCell, byDay }
}