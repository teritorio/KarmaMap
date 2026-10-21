# The changes map

Two things are true everywhere on the site:

- **The data is read in your browser.** The pages fetch only the rows they
  need from a set of public data files. Nothing you do here is sent to a
  server, and no query is run on your behalf.
- **Everything is counted in UTC calendar days.** A "day" is a UTC day, not
  your local day, and a user's activity is grouped into the day it happened
  in UTC.

## What you see

A map of hexagonal cells, each shaded by how many map edits happened inside
it during the selected dates, with a day-by-day histogram of the same
numbers above the map. At the top: a **From** / **To** date range, a **Log
scale** checkbox, and a status line telling you what the last query did
("1204 cells, 8431 total changes.").

The cell size is set by the dataset and shown nowhere on the page; at the
default resolution each hexagon is roughly 175 m per edge, about 0.1 km².

## How to use it

- **Zoom in to at least level 12.** Below that a single screen holds more
  cells than a sensible query returns, so the page asks you to zoom in
  first. The status line shows your current zoom and the level it needs.
- **Pan and zoom to query.** There is no query button: the map re-queries
  shortly after you stop moving, using the cells currently in view.
- **Drag the histogram slider** to set the date range, or type the dates.
  The two stay in sync, and both are limited to the first and last day the
  dataset actually covers — you cannot ask for dates that hold no data.
- **The first load shows the last four months.** Widen the range to see
  more; a long range over a dense area is the heaviest query the page will
  make.
- **Log scale** (on by default) suits dense areas, where a handful of
  cells hold orders of magnitude more edits than the rest. Turn it off to
  compare absolute values. Changing it only restyles what is already
  loaded; it does not re-query.
- **Share the view.** The address bar keeps the map position, zoom, dates
  and scale as you work, so copying the URL gives someone exactly what you
  are looking at.

## How to read the numbers

A cell's colour is the total number of **node and way edits attributed to
that cell** over the selected dates:

- A node edit is counted in the cell holding that point.
- A way edit is counted in every cell its nodes fall in, but only once per
  cell per edit, so a long motorway counts in each hexagon it crosses, not
  once per node.
- A deleted object is counted where it was last known to be, since a
  deletion carries no position of its own.
- Node and way counts are merged, so one cell's number is edits of both
  kinds together.

The histogram splits that same total by day, for the cells currently in
view. Its axis spans the whole period the dataset covers, so you can drag
the window to any part of the history even when that part is off screen.
Days with no edits in the current view appear as gaps on the log scale and
flat bars on the linear scale. Panning the map changes both the colours
and the histogram.

## What this page does not show

- **No relations.** Only nodes and ways are counted on the map.
- **No individual edits or users.** A cell total is an aggregate; use the
  [contributor's profile](../users/HOW_IT_WORKS.md) or the
  [suspect flagged days](../suspect/HOW_IT_WORKS.md) for a person.
- **Not live.** The data covers an OSM history extract the site operator
  imported, plus whatever replication updates have been applied since. The
  date pickers show exactly how far it reaches.
- **Not every edit is wrong.** A hot cell means a lot of mapping happened
  there — survey work, an import, a redraw. Redrawing a city centre
  legitimately produces an enormous number of changes in a few cells.
