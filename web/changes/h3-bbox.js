// bbox <-> H3 cell helpers (h3-js). bbox = [minLng, minLat, maxLng, maxLat].

import { polygonToCells, gridDisk, cellToBoundary } from 'h3-js'

// Cells whose hexagons intersect the bbox (inclusive of touching), not just
// those whose centers fall inside. polygonToCells seeds from center-contained
// cells; gridDisk(seed, 1) adds their edge neighbors, which covers every
// intersecting cell for viewport-scale boxes (far larger than a single cell).
export function bboxToCells(bbox, resolution) {
  const [minLng, minLat, maxLng, maxLat] = bbox
  const ring = [
    [minLng, minLat],
    [maxLng, minLat],
    [maxLng, maxLat],
    [minLng, maxLat],
    [minLng, minLat],
  ]

  const seeds = polygonToCells([ring], resolution, true)
  const candidates = new Set()
  for (const seed of seeds) {
    candidates.add(seed)
    for (const neighbor of gridDisk(seed, 1)) {
      candidates.add(neighbor)
    }
  }

  return [...candidates].filter((hex) => polygonIntersectsRect(cellToBoundary(hex, true), bbox))
}

// point-in-rect, boundaries inclusive
function pointInRect(pt, rect) {
  const [x, y] = pt
  const [minX, minY, maxX, maxY] = rect
  return x >= minX && x <= maxX && y >= minY && y <= maxY
}

// Liang-Barsky: does segment p0-p1 cross (or touch) the rect?
function segmentIntersectsRect(p0, p1, rect) {
  const [minX, minY, maxX, maxY] = rect
  let [dx, dy] = [p1[0] - p0[0], p1[1] - p0[1]]
  let [t0, t1] = [0, 1]
  const p = [-dx, dx, -dy, dy]
  const q = [p0[0] - minX, maxX - p0[0], p0[1] - minY, maxY - p0[1]]
  for (let i = 0; i < 4; i++) {
    if (p[i] === 0) {
      if (q[i] < 0) return false
    } else {
      const r = q[i] / p[i]
      if (p[i] < 0) {
        if (r > t1) return false
        if (r > t0) t0 = r
      } else {
        if (r < t0) return false
        if (r < t1) t1 = r
      }
    }
  }
  return true
}

// ray-cast point-in-polygon for a closed ring
function pointInPolygon(pt, ring) {
  const [x, y] = pt
  let inside = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) {
      inside = !inside
    }
  }
  return inside
}

function polygonIntersectsRect(ring, rect) {
  if (ring.some((pt) => pointInRect(pt, rect))) return true
  for (let i = 0, j = ring.length - 2; i < ring.length - 1; j = i++) {
    if (segmentIntersectsRect(ring[j], ring[i], rect)) return true
  }
  const [minX, minY, maxX, maxY] = rect
  const corners = [
    [minX, minY],
    [maxX, minY],
    [maxX, maxY],
    [minX, maxY],
  ]
  return corners.some((pt) => pointInPolygon(pt, ring))
}

// Numeric bounds (BigInt, matching the Parquet uint64 h3_cell column) and
// exact membership Set from a list of hex cell strings.
export function cellsMinMaxSet(hexCells) {
  if (hexCells.length === 0) {
    throw new Error('bboxToCells returned no cells - is the bbox valid?')
  }

  const values = hexCells.map((hex) => BigInt('0x' + hex))
  let min = values[0]
  let max = values[0]
  const set = new Set()

  for (const v of values) {
    if (v < min) min = v
    if (v > max) max = v
    set.add(v)
  }

  return { min, max, set }
}