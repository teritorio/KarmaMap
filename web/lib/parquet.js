// Low-level Parquet access over byte-range HTTP (hyparquet), shared by the
// changes and users queries. Files are never downloaded in full: hyparquet's
// asyncBufferFromUrl fetches the footer, row-group metadata and only the
// pages a query's range filter needs.

import { parquetQuery, asyncBufferFromUrl, parquetMetadataAsync } from 'hyparquet'
import { compressors } from 'hyparquet-compressors'

// Opens a Parquet file under baseUrl/path, returning null (not a thrown
// error) on fetch failure: a missing file means "no data", not a fatal
// query error.
async function fetchFile(baseUrl, path) {
  const url = `${baseUrl}/${path}`
  try {
    return await asyncBufferFromUrl({ url })
  } catch (err) {
    // A fetch failure means the file isn't there, not a fatal query error.
    console.warn(`Skipping ${url}: ${err.message}`)
    return null
  }
}

// Parses the file metadata. When footer_size is known it points exactly at
// the parquet footer, so the initial fetch reads just those bytes instead of
// the 512 KB tail window hyparquet uses by default. `always` forces a parse
// even without footer_size (for callers that read the returned metadata).
async function maybeParseMetadata(file, footerSize, always) {
  if (!always && !footerSize) return undefined
  return parquetMetadataAsync(file, footerSize ? { initialFetchSize: footerSize + 8 } : undefined)
}

// Runs a hyparquet query over one non-partitioned file, returning the
// matching rows. A failed or absent file yields []. Range filters prune row
// groups and pages on the server side; exact membership is left to the
// caller.
export async function queryRows(baseUrl, path, filter, columns, footerSize) {
  const file = await fetchFile(baseUrl, path)
  if (!file) return []
  const metadata = await maybeParseMetadata(file, footerSize, false)
  return parquetQuery({ file, compressors, filter, columns, metadata })
}

// Like queryRows, but also returns the parsed metadata for callers that read
// file-level key_value_metadata (the user-reputation aspect stats): the
// metadata is parsed even without footer_size. Returns { metadata, rows },
// or null when the file is absent.
export async function queryRowsWithMetadata(baseUrl, path, filter, columns, footerSize) {
  const file = await fetchFile(baseUrl, path)
  if (!file) return null
  const metadata = await maybeParseMetadata(file, footerSize, true)
  const rows = await parquetQuery({ file, compressors, filter, columns, metadata })
  return { metadata, rows }
}
