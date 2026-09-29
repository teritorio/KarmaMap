# How it works

This guide walks through the three KarmaMap pages: what each one shows,
what every number means, and how to read it without any technical
background. The scoring rules come from a published paper, cited at the
end.

Two things are true everywhere on the site:

- **The data is read in your browser.** The pages fetch only the rows they
  need from a set of public data files. Nothing you do here is sent to a
  server, and no query is run on your behalf.
- **Everything is counted in UTC calendar days.** A "day" is a UTC day, not
  your local day, and a user's activity is grouped into the day it happened
  in UTC.

## The changes map

### What you see

A map of hexagonal cells, each shaded by how many map edits happened inside
it during the selected dates, with a day-by-day histogram of the same
numbers above the map. At the top: a **From** / **To** date range, a **Log
scale** checkbox, and a status line telling you what the last query did
("1204 cells, 8431 total changes.").

The cell size is set by the dataset and shown nowhere on the page; at the
default resolution each hexagon is roughly 175 m per edge, about 0.1 km².

### How to use it

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

### How to read the numbers

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

### What this page does not show

- **No relations.** Only nodes and ways are counted on the map.
- **No individual edits or users.** A cell total is an aggregate; use the
  other two pages for a person.
- **Not live.** The data covers an OSM history extract the site operator
  imported, plus whatever replication updates have been applied since. The
  date pickers show exactly how far it reaches.
- **Not every edit is wrong.** A hot cell means a lot of mapping happened
  there — survey work, an import, a redraw. Redrawing a city centre
  legitimately produces an enormous number of changes in a few cells.

## Contributor's profile

### Finding a user

Type an **exact OSM username** and press Enter or Search. The match is
exact, so spelling and capitalisation must match the account. The profile
links out to the account on openstreetmap.org and to
[hdyc](https://hdyc.neis-one.org/), a third-party history viewer.

The profile strip shows the account, its numeric user id, and **First
seen**: the first day this user appears *in this dataset*. That is not the
day they joined OpenStreetMap — it is the first day they edited inside the
area and period covered here.

### The ranking (0-100)

The headline number is the paper's contributor score, from 0 (a new
contributor) to 100 (an expert). It is built only from what someone
**created**, never from modifications or deletions:

| Aspect | Maximum points |
|---|---|
| Nodes created | 20 |
| Ways created | 20 |
| Relations created | 12 |
| Each of 12 top-level map keys used at creation | 4 each (48 total) |
| **Total** | **100** |

The 12 keys are the project's most used top-level keys: `amenity`,
`boundary`, `building`, `highway`, `landuse`, `leisure`, `name`, `natural`,
`place`, `railway`, `sport`, `waterway`. The paper's list included
`address`; this dataset counts `place` instead, because OSM address tagging
uses `addr:` prefixes (`addr:street`) rather than a plain `address` key.

Each aspect is worth up to its weight, and within that weight the score is
your **percentile among everyone in this dataset who used that aspect**:

- no activity on an aspect scores 0 for it;
- the single busiest contributor scores the full weight;
- contributors with equal totals share the same position;
- nobody can exceed an aspect's weight, however dominant they are.

So a ranking is a **position within this dataset**, not a measure of
quality, skill or trustworthiness. A careful mapper in a sparsely mapped
region can score low because few people map there; a bulk importer can
score high by creating a lot. The `Ranking` column on each aspect says
where that user sits, with a tooltip giving the numbers behind it.

The paper fixes the weights and the 0-100 scale but not the formula that
turns raw counts into a percentage. Percentile ranking is this
implementation's choice, and it is why the numbers stay spread out across
the scale instead of collapsing to zero.

### The counter tables

Below the ranking, every raw count the score is built from, one table per
group:

- **Activity** — total edits: nodes and ways created, modified and deleted,
  added up.
- **Created objects — nodes (20 pts)**, **— ways (20 pts)** and
  **— relations (12 pts)** — how many of each the user created. All three
  feed the ranking. Relations score, but they are not part of the activity
  total.
- **Top12 tags — 4 pts each** — how many objects the user created carrying
  each of those keys. Ranked like the other aspects.
- **Other counters** — modifications and deletions of nodes and ways. Shown
  for information; they score nothing.

The `Ranking` column marks each aspect with the points it earned and where
that sits: *no activity*, *lowest*, *only contributor*, *best*, or *above
N%* of the contributors active on that aspect.

### The activity timeline

One bar per day the user edited, over their whole history in the dataset —
idle days are simply absent, so a sparse contributor sees a dotted row of
bars with gaps between them rather than a solid block. A day's bar counts
everything they changed that day, relations included, which is why it can
be higher than their node and way totals suggest. Hover a bar for the day's
total and any suspect flags. The log scale toggle behaves as on the changes
page.

**Red bands mark days a filter fired** (see below). Hover one to
see which filter. Red means "this day is worth a look", not "this user did
something wrong".

The username you searched stays in the address bar, so that URL reopens the
same profile.

## Suspect flagged days

### What the table is

The 100 most recent flagged user-days, newest first, one row per user per
day. It is a shortlist for a human to review, not a verdict: **a flag is a
prompt to look, not an accusation.** The filters are deliberately broad, so
bulk imports, data migrations, automated redraws and a genuine cleanup
session all trip them.

The table is populated only after the dataset has been updated from the OSM
replication stream. On a freshly imported dataset there is nothing to flag
and the page says so.

### The columns

| Column | Meaning |
|---|---|
| **Date** | The UTC day the filter fired. |
| **Ranking at day** | The user's ranking **frozen at the moment that day was first flagged**, not today's. Click it to open the user's page. |
| **User** / **uid** | The account and its numeric id. |
| **Changes** | Everything that user changed that day, nodes, ways and relations together. A burst day shows a large number here. |
| **Edits/h** | The busiest hour of that day: the largest number of objects modified or deleted within any single hour. Shown only when it is above 500, the point at which filter 2 fires. |
| **Spread** | The widest hour of that day: the largest area its edits covered, in km². Shown only when it reaches 20 km² over at least 3 distinct H3 cells, the point at which filter 4 fires. |
| **Mass tags** | The tag keys that covered more than 90% of an hour's modified or deleted objects on that day, where that hour held at least 100 objects — the keys filter 5 fires on. A day can list more than one key. |
| **Far moves** | How many times that day the user dragged a node more than 500 m. |
| **Max move** | The longest of those drags, in meters or kilometers. |

The four number columns are blank unless the filter they belong to fired, so
an empty cell means that filter did not apply to the day. `Edits/h` and
`Spread` describe the day itself and are rebuilt from the stored data on every
update, so a day first flagged by a different filter picks them up later.
`Ranking at day` and `Max move` are recorded once, the first time a day is
flagged, and then kept as they were — they are not recomputed as the user's
ranking grows or as later edits arrive, so an empty `Max move` does not always
mean none happened: it can also mean the day was first flagged by another
filter.

### The filters

The three filters are the ones proposed in the paper this project follows.
A fourth and fifth filter are local extensions, **not from the paper**; Filter 5
flags when one tag key covers >90% of a 1-hour window's modified/deleted objects
(minimum 100 objects).
A filter is only an automatic rule that marks days for review: it forms no
judgement about the edit itself, and nothing is hidden, blocked or reverted
because of it.

| Filter | Rule | What you see |
|---|---|---|
| 1. Low ranking | A new contributor, or one with a ranking below 5%, has their edits shown for review | No column of its own; it drives `Ranking at day` |
| 2. Edit burst | More than 500 objects modified or deleted within one hour | An `Edits/h` count above 500 |
| 3. Far move | A node dragged more than 500 m | A raised `Far moves` count and a `Max move` distance |
| 4. Spatial spread | Edits in 1h span ≥3 H3 cells whose combined area ≥ 20 km² | A `Spread` of at least 20 km² |
| 5. Tag activity | One tag key on >90% of modified/deleted objects in a 1h window with ≥100 total | The keys listed in `Mass tags` |

Two details about how they are applied:

- **Filter 1 only looks forward.** It marks the days a low-ranking
  contributor edits *from now on*. As someone builds a history their
  ranking rises, and the days already marked stay marked — but the past is
  never re-examined because a ranking later dropped.
- **Filters 2, 3, 4 and 5 follow bursts and mistakes, not people.** All four
  fire on activity, so a single flagged day says nothing about a contributor's
  usual behaviour; the whole timeline on the users page is the context.

## Reference

The ranking and all three filters are from:

> Pascal Neis, Marcus Goetz, Alexander Zipf.
> *Towards Automatic Vandalism Detection in OpenStreetMap.*
> ISPRS International Journal of Geo-Information 2012, 1(3), 315-332.
> DOI: [10.3390/ijgi1030315](https://doi.org/10.3390/ijgi1030315)

The paper describes a rule-based system that scores each edit by combining
the contributor's standing with the edit itself, evaluated independently so
that different patrol groups can weight them differently. KarmaMap
implements the contributor standing and three of the paper's filters, and
publishes both so a reader can judge how much to trust a flag:

- Ranking weights and the 0-100 scale, as tabulated above.
- Ranking built from created objects and the top keys used at creation,
  modifications and deletions excluded.
- Filter 1: "Show all edits of new users and/or users with a very low
  ranking (<5%)."
- Filter 2: "Show all users who modified or deleted more than 500 objects
  within one hour."
- Filter 3: "Show all users who modified node objects and moved the object
  for more than 500m."

Not implemented here: the paper's per-edit analysis of whether tag
combinations are valid, its finer flag for geometry moved about 11 m, and
the black-list and white-list of accounts that a patrol would maintain by
hand. The paper's raw-count-to-percentage mapping is also not specified
there; see the ranking section above for what this dataset does instead.
