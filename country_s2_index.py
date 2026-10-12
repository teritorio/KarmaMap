#!/usr/bin/env python3
"""
Assign countries to S2 cells with a recursive subdivision.

A cell that touches at most one country is emitted as that country (even when
the cell also covers ocean). A cell that touches two or more countries is split
into its four children, down to --max-level where a still-contested cell goes to
the country with the largest area inside it. Only land borders between
countries are refined to --max-level; coastlines and interiors stay coarse.

All clipping and area measurement happen in EPSG:6933 (equal-area).

Output:
  * Parquet file, ordered by the smallest descendant leaf of each row's cell
    so a range scan over descendants is contiguous, with columns
        s2_cell     uint64  S2 cell id (range_min = s2_cell - (s2_cell & -s2_cell) + 1)
        country_id  uint8   id of the winning country (0 = no valid ISO code)
        level       uint8   level of s2_cell (0-30)
  * JSON lookup table (id -> ISO 3166-1 alpha-2 code), built from all valid
    codes of the input file, sorted alphabetically (ids 1..255, 0 = null).

Usage:
    python country_s2_index.py [--max-level 13] \
        ne_10m_admin_0_map_units.zip country.parquet

Progress is printed to stderr (silence with --quiet). The id -> ISO 3166-1
alpha-2 lookup table is always written next to the output as country_ids.json.

Requirements:
    pip install s2sphere geopandas shapely pandas pyarrow pyproj
"""
import argparse
import json
import math
import os
import sys
from collections import Counter

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import s2sphere
from pyproj import Transformer
from shapely.affinity import translate
from shapely.geometry import LineString, Polygon, box
from shapely.ops import split as split_geometry
from shapely.ops import unary_union

EQUAL_AREA_CRS = "EPSG:6933"  # World Cylindrical Equal Area
WORLD_BOX = box(-180, -90, 180, 90)
ROW_GROUP_ROWS = 500_000
BUFFER_ROWS = 500_000

_QUIET = False


def log(message: str) -> None:
    if not _QUIET:
        print(message, file=sys.stderr, flush=True)


def resolve_iso_a2(gdf: gpd.GeoDataFrame) -> pd.Series:
    """Return a valid ISO alpha-2 code per row.

    Prefers ISO_A2_EH (repaired codes) when present, falls back to ISO_A2.
    Natural Earth uses '-99' as a no-data sentinel; such values are ignored.
    """
    iso = pd.Series(pd.NA, index=gdf.index, dtype="string")
    for col in ("ISO_A2_EH", "ISO_A2"):
        if col not in gdf.columns:
            continue
        values = gdf[col].astype("string").str.strip().str.upper()
        valid = values.str.fullmatch(r"[A-Z]{2}").fillna(False)
        iso = iso.where(iso.notna() | ~valid, values)
    return iso


def load_countries(path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path).to_crs("EPSG:4326")
    geom_name = gdf.geometry.name
    gdf = gdf.rename(columns={c: c.upper() for c in gdf.columns if c != geom_name})
    gdf["iso_a2"] = resolve_iso_a2(gdf)
    fixed = gpd.GeoSeries(
        shapely.make_valid(gdf.geometry.values), index=gdf.index, crs="EPSG:4326"
    )
    out = gpd.GeoDataFrame({"iso_a2": gdf["iso_a2"]}, geometry=fixed, crs="EPSG:4326")
    return out.reset_index(drop=True)


def build_country_ids(countries: gpd.GeoDataFrame) -> dict:
    """Map each valid ISO alpha-2 code to an id in 1..255 (0 is reserved for null)."""
    codes = sorted(countries["iso_a2"].dropna().unique())
    if len(codes) > 255:
        raise ValueError(f"{len(codes)} distinct codes do not fit in uint8 (max 255)")
    return {code: i for i, code in enumerate(codes, start=1)}


def _wrap_delta(delta: float) -> float:
    if delta > 180:
        return delta - 360
    if delta < -180:
        return delta + 360
    return delta


def split_antimeridian(geom):
    """Split a lon/lat geometry into parts that do not cross the antimeridian.

    A lon/lat ring whose coordinates jump between +180 and -180 (e.g. Russia,
    Fiji) must be cut at the meridian so planar clipping in EPSG:6933 works.
    Returns a list of polygon parts within [-180, 180].
    """
    min_x, _, max_x, _ = geom.bounds
    if max_x - min_x <= 180:
        return [geom]

    coords = shapely.get_coordinates(geom)
    coords[:, 0] = np.where(coords[:, 0] < 0, coords[:, 0] + 360.0, coords[:, 0])
    shifted = shapely.set_coordinates(geom, coords)

    cutter = LineString([(180.0, -90.0), (180.0, 90.0)])
    parts = []
    for part in shapely.get_parts(split_geometry(shifted, cutter)):
        if part.geom_type != "Polygon" or part.is_empty or part.area == 0:
            continue
        if part.centroid.x > 180:
            part = translate(part, xoff=-360.0)
        parts.append(part)
    return parts or [geom]


def polygonal_only(geom):
    """The polygonal parts of `geom`, as a Polygon/MultiPolygon or None."""
    parts = [
        p
        for p in shapely.get_parts(geom)
        if p.geom_type == "Polygon" and p.area > 0
    ]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else shapely.multipolygons(parts)


def _project(geom, transformer):
    """Project a lon/lat geometry with `transformer` (vectorised)."""
    if geom.is_empty:
        return geom
    coords = shapely.get_coordinates(geom)
    if len(coords) == 0:
        return geom
    x, y = transformer.transform(coords[:, 0], coords[:, 1])
    return shapely.set_coordinates(geom, np.column_stack([x, y]))


def projected_area_km2(geom) -> float:
    """Area of an EPSG:6933 geometry in km^2."""
    if geom.is_empty:
        return 0.0
    return geom.area / 1e6


def _latlng_to_xyz(lat: float, lng: float):
    phi = math.radians(lat)
    theta = math.radians(lng)
    cos_phi = math.cos(phi)
    return (cos_phi * math.cos(theta), cos_phi * math.sin(theta), math.sin(phi))


def _xyz_to_latlng(x: float, y: float, z: float):
    lat = math.degrees(math.atan2(z, math.hypot(x, y)))
    lng = math.degrees(math.atan2(y, x))
    return lat, lng


def _slerp(a, b, t):
    dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
    dot = max(-1.0, min(1.0, dot))
    theta = math.acos(dot)
    if theta < 1e-12:
        return a
    sin_theta = math.sin(theta)
    w0 = math.sin((1.0 - t) * theta) / sin_theta
    w1 = math.sin(t * theta) / sin_theta
    return (w0 * a[0] + w1 * b[0], w0 * a[1] + w1 * b[1], w0 * a[2] + w1 * b[2])


def _cell_ring_latlng(cell_id):
    """Boundary of an S2 cell as (lat, lng) points, geodesic edges densified."""
    level = cell_id.level()
    if level <= 2:
        steps = 16
    elif level <= 5:
        steps = 8
    elif level <= 9:
        steps = 4
    else:
        steps = 1
    cell = s2sphere.Cell(cell_id)
    corners = []
    for k in range(4):
        ll = s2sphere.LatLng.from_point(cell.get_vertex(k))
        corners.append((ll.lat().degrees, ll.lng().degrees))
    ring = []
    for i in range(4):
        a = _latlng_to_xyz(*corners[i])
        b = _latlng_to_xyz(*corners[(i + 1) % 4])
        for j in range(steps):
            ring.append(_xyz_to_latlng(*_slerp(a, b, j / steps)))
    return ring


def _cell_polygon_lonlat(pts):
    """Planar lon/lat polygon of a cell, handling the antimeridian and poles.

    Longitudes are unwrapped along the boundary; a ring that winds around a
    pole is closed through that pole; the result is clipped back into
    [-180, 180] (as a MultiPolygon when it straddles the antimeridian).
    """
    xs = [pts[0][1]]
    for i in range(1, len(pts)):
        xs.append(xs[-1] + _wrap_delta(pts[i][1] - pts[i - 1][1]))
    x_close = xs[-1] + _wrap_delta(pts[0][1] - pts[-1][1])
    ring = [(x, lat) for x, (lat, _) in zip(xs, pts)]

    if abs(x_close - xs[0]) > 180:
        pole = 90.0 if pts[0][0] > 0 else -90.0
        ring += [(x_close, pts[0][0]), (x_close, pole), (xs[0], pole)]

    poly = Polygon(ring)
    min_x, _, max_x, _ = poly.bounds
    if min_x >= -180 and max_x <= 180:
        return poly

    parts = []
    for k in range(-2, 3):
        part = translate(poly, xoff=360 * k).intersection(WORLD_BOX)
        if part.area > 0:
            parts.append(part)
    return unary_union(parts)


def s2_cell_polygon_6933(cell_id, transformer):
    """S2 cell boundary as an EPSG:6933 polygon (MultiPolygon across the seam)."""
    return _project(_cell_polygon_lonlat(_cell_ring_latlng(cell_id)), transformer)


def s2_children_sorted(cell_id):
    """The four children of an S2 cell, ascending by range_min."""
    return sorted(cell_id.children(), key=lambda c: c.range_min())


# Centers of the six S2 cube faces (face -> (lat, lng)).
_FACE_CENTERS = [(0.0, 0.0), (0.0, 90.0), (90.0, 0.0), (0.0, 180.0), (0.0, -90.0), (-90.0, 0.0)]


def face_cell(face: int):
    """The level-0 S2 cell covering cube face `face`."""
    lat, lng = _FACE_CENTERS[face]
    return s2sphere.CellId.from_lat_lng(
        s2sphere.LatLng.from_degrees(lat, lng)
    ).parent(0)


def build_index(countries: gpd.GeoDataFrame, max_level: int, output: str) -> tuple:
    """Write the country index Parquet file with a recursive S2 subdivision."""
    transformer = Transformer.from_crs("EPSG:4326", EQUAL_AREA_CRS, always_xy=True)
    code_to_id = build_country_ids(countries)

    part_isos, parts_6933 = [], []
    for iso, geom in zip(countries["iso_a2"].to_numpy(dtype=object), countries.geometry):
        iso = iso if isinstance(iso, str) else None
        for part in split_antimeridian(geom):
            if part.is_empty or part.area == 0:
                continue
            part_isos.append(iso)
            parts_6933.append(_project(part, transformer))

    schema = pa.schema(
        [
            ("s2_cell", pa.uint64()),
            ("country_id", pa.uint8()),
            ("level", pa.uint8()),
        ]
    )
    writer = pq.ParquetWriter(output, schema, compression="zstd")

    cell_buf, id_buf, level_buf, total = [], [], [], 0
    level_counter = Counter()

    def flush():
        nonlocal cell_buf, id_buf, level_buf, total
        if not cell_buf:
            return
        table = pa.table(
            {
                "s2_cell": pa.array(cell_buf, type=pa.uint64()),
                "country_id": pa.array(id_buf, type=pa.uint8()),
                "level": pa.array(level_buf, type=pa.uint8()),
            },
            schema=schema,
        )
        writer.write_table(table, row_group_size=ROW_GROUP_ROWS)
        total += len(cell_buf)
        cell_buf, id_buf, level_buf = [], [], []

    def emit(cell_id, iso):
        cell_buf.append(cell_id.id() & 0xFFFFFFFFFFFFFFFF)
        id_buf.append(code_to_id.get(iso, 0))
        level_buf.append(cell_id.level())
        level_counter[cell_id.level()] += 1
        if len(cell_buf) >= BUFFER_ROWS:
            flush()

    def largest_area_iso(cand):
        area_by_iso = {}
        for iso, geom in cand:
            area = projected_area_km2(geom)
            if area > 0:
                area_by_iso[iso] = area_by_iso.get(iso, 0.0) + area
        best_iso, best_key = None, None
        for iso, area in area_by_iso.items():
            key = (-area, iso if isinstance(iso, str) else "")
            if best_key is None or key < best_key:
                best_key, best_iso = key, iso
        return best_iso

    def descend(cell_id, cand):
        if not cand:
            return
        iso_set = {iso for iso, _ in cand}
        if len(iso_set) == 1:
            emit(cell_id, next(iter(iso_set)))
            return
        if cell_id.level() == max_level:
            best = largest_area_iso(cand)
            if best is not None:
                emit(cell_id, best)
            return
        for child in s2_children_sorted(cell_id):
            child_poly = s2_cell_polygon_6933(child, transformer)
            child_cand = []
            for iso, geom in cand:
                inter = polygonal_only(shapely.intersection(geom, child_poly))
                if inter is not None:
                    child_cand.append((iso, inter))
            if child_cand:
                descend(child, child_cand)

    try:
        for face in range(6):
            face_id = face_cell(face)
            face_poly = s2_cell_polygon_6933(face_id, transformer)
            face_cand = []
            for iso, geom in zip(part_isos, parts_6933):
                inter = polygonal_only(shapely.intersection(geom, face_poly))
                if inter is not None:
                    face_cand.append((iso, inter))
            log(
                f"face {face + 1}/6: {len(face_cand)} pieces, "
                f"{total + len(cell_buf)} cells so far"
            )
            if face_cand:
                descend(face_id, face_cand)
        flush()
    finally:
        writer.close()

    return total, code_to_id, level_counter


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("input", help="Natural Earth Admin 0 file (.shp, .gpkg, .zip, ...)")
    parser.add_argument("output", help="Output Parquet path")
    parser.add_argument(
        "--max-level", type=int, default=13, help="Finest S2 level (0-30, default: 13)"
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress output")
    args = parser.parse_args()

    if not 0 <= args.max_level <= 30:
        parser.error("--max-level must be between 0 and 30")

    global _QUIET
    _QUIET = args.quiet

    log(f"reading countries from {args.input}")
    countries = load_countries(args.input)
    log(f"  {len(countries)} admin units loaded")
    log(f"building country index at max S2 level {args.max_level}")
    total, code_to_id, level_counter = build_index(
        countries, args.max_level, args.output
    )

    mapping_path = os.path.join(os.path.dirname(args.output), "country_ids.json")
    id_to_code = {"0": None, **{str(i): c for c, i in code_to_id.items()}}
    log(f"writing lookup table ({len(code_to_id)} codes) to {mapping_path}")
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(id_to_code, f, indent=1, ensure_ascii=False)

    breakdown = " ".join(
        f"L{level}={level_counter[level]}" for level in sorted(level_counter)
    )
    print(
        f"{total} cells written to {args.output} [{breakdown}]; "
        f"lookup table in {mapping_path}"
    )


if __name__ == "__main__":
    main()
