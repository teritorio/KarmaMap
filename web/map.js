// MapLibre setup + rendering aggregated H3 cells as a GeoJSON fill layer
// (MapLibre has no native H3 layer, so cells become polygons via h3-js).

import * as maplibregl from 'maplibre-gl'
import { cellToBoundary } from 'h3-js'

// Free vector style with no API key required.
const DEMO_STYLE = 'https://vecto.teritorio.xyz/styles/positron/style.json'

// Default view when no permalink is present: Andorra, zoom 12.
const DEFAULT_CENTER = { lng: 1.5218, lat: 42.5063 }
const DEFAULT_ZOOM = 12

export function initMap(containerId, initialView) {
  const map = new maplibregl.Map({
    container: containerId,
    style: DEMO_STYLE,
    center: [initialView?.lng ?? DEFAULT_CENTER.lng, initialView?.lat ?? DEFAULT_CENTER.lat],
    zoom: initialView?.zoom ?? DEFAULT_ZOOM,
    hash: false, // permalink.js manages the URL hash, not MapLibre itself
  })
  map.addControl(new maplibregl.NavigationControl())
  return map
}

function toFeatureCollection(aggregated) {
  const features = []
  for (const [cell, count] of aggregated.entries()) {
    const hex = cell.toString(16)
    // cellToBoundary(..., true) returns [lng, lat] pairs (GeoJSON order).
    const boundary = cellToBoundary(hex, true)
    features.push({
      type: 'Feature',
      properties: { count },
      geometry: { type: 'Polygon', coordinates: [boundary] },
    })
  }
  return { type: 'FeatureCollection', features }
}

// The h3-fill ramp for a given scale. Log uses log10(count) so the color
// breakpoints match linear (10/50/200/1000) but spread logarithmically;
// the input is clamped to >= 0 because log10(0) is -Infinity.
function fillColorRamp(log) {
  return log
    ? {
        'fill-color': [
          'interpolate', ['linear'],
          ['max', 0, ['log10', ['get', 'count']]],
          0, '#ffffb2',
          1, '#fecc5c',
          1.7, '#fd8d3c',
          2.3, '#e31a1c',
          3, '#800026',
        ],
        'fill-opacity': 0.6,
      }
    : {
        'fill-color': [
          'interpolate', ['linear'], ['get', 'count'],
          0, '#ffffb2',
          10, '#fecc5c',
          50, '#fd8d3c',
          200, '#e31a1c',
          1000, '#800026',
        ],
        'fill-opacity': 0.6,
      }
}

const SOURCE_ID = 'h3-results'

export function renderResults(map, aggregated, log) {
  const geojson = toFeatureCollection(aggregated)

  const source = map.getSource(SOURCE_ID)
  if (source) {
    source.setData(geojson)
    return
  }

  map.addSource(SOURCE_ID, { type: 'geojson', data: geojson })

  map.addLayer({
    id: 'h3-fill',
    type: 'fill',
    source: SOURCE_ID,
    paint: fillColorRamp(log),
  })

  map.addLayer({
    id: 'h3-outline',
    type: 'line',
    source: SOURCE_ID,
    paint: { 'line-color': '#333333', 'line-width': 0.5 },
  })
}

export function setResultsLogScale(map, log) {
  if (!map.getLayer('h3-fill')) return
  map.setPaintProperty('h3-fill', 'fill-color', fillColorRamp(log)['fill-color'])
}

export function getViewportBbox(map) {
  const bounds = map.getBounds()
  return [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]
}
