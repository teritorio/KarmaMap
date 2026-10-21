# Suspect flagged days

Two things are true everywhere on the site:

- **The data is read in your browser.** The pages fetch only the rows they
  need from a set of public data files. Nothing you do here is sent to a
  server, and no query is run on your behalf.
- **Everything is counted in UTC calendar days.** A "day" is a UTC day, not
  your local day, and a user's activity is grouped into the day it happened
  in UTC.

## What the table is

The 100 most recent flagged user-days, newest first, one row per user per
day. It is a shortlist for a human to review, not a verdict: **a flag is a
prompt to look, not an accusation.** The filters are deliberately broad, so
bulk imports, data migrations, automated redraws and a genuine cleanup
session all trip them.

The table is populated only after the dataset has been updated from the OSM
replication stream. On a freshly imported dataset there is nothing to flag
and the page says so.

## Filtering the table

Every column header carries a filter and the table narrows as you type. The
filters only pick out the days worth looking at next; they never change what
was flagged.

- **Date** takes one UTC day, **uid** an exact id, **Ranking at day** a range.
- **Changes**, **Edits/h**, **Name lang events**, **Spread**, **Far moves** and
  **Max move** take a minimum, so `200` on `Edits/h` keeps the days with at
  least 200 edits in their busiest hour.
- **User** and **Mass tags** match text: the username and the tag keys.
- **Countries** takes one ISO 3166-1 alpha-2 code. Type it (`FR`) or pick it
  from the list of the codes the loaded days actually contain, and the flag of
  that country is shown next to the field. `No country` keeps the days with no
  country at all: either no cell was recorded for the day, or its cells fall
  where the country index does not resolve.

The address bar keeps the filters, so copying the URL reopens the same
filtered shortlist. The status line says how many of the 100 loaded days are
showing.

## The columns

| Column | Meaning |
|---|---|
| **Date** | The UTC day the filter fired. |
| **Countries** | The distinct countries the day's edited cells fall in, as ISO 3166-1 alpha-2 codes with their flag. Each cell's centre is resolved through a country index, so the column is empty for a day whose cells fall in the sea or in a cell the index does not resolve. |
| **Ranking at day** | The user's ranking **frozen at the moment that day was first flagged**, not today's. Click it to open the user's page. |
| **User** / **uid** | The account and its numeric id. |
| **Changes** | Everything that user changed that day, nodes, ways and relations together. A burst day shows a large number here. |
| **Edits/h** | The busiest hour of that day: the largest number of objects modified or deleted within any single hour. Shown only when it is above 500, the point at which filter 2 fires. |
| **Name lang events** | How many objects on that day had their language tags (`name:xx`, `wikipedia:xx`) removed, overwritten or swapped, the value filter 6 fires on. Shown only when that filter fired. |
| **Spread** | The widest hour of that day: the largest area its edits covered, in km². Shown only when it reaches 20 km² over at least 3 distinct H3 cells, the point at which filter 4 fires. |
| **Mass tags** | The tag keys that covered more than 90% of an hour's modified or deleted objects on that day, where that hour held at least 100 objects — the keys filter 5 fires on. A day can list more than one key. |
| **Far moves** | How many times that day the user dragged a node more than 500 m. |
| **Max move** | The longest of those drags, in meters or kilometers. |

The metric columns are blank unless the filter they belong to fired, so an
empty cell means that filter did not apply to the day. `Edits/h`, `Spread`,
`Mass tags` and `Name lang events` describe the day itself and are rebuilt from
the stored data on every update, so a day first flagged by a different filter
picks them up later. `Ranking at day`, `Far moves` and `Max move` are recorded
once, the first time a day is flagged, and then kept as they were: they are not
recomputed as the user's ranking grows or as later edits arrive, so an empty
`Max move` does not always mean none happened: it can also mean the day was
first flagged by another filter.

## The filters

The three filters are the ones proposed in the paper this project follows.
Three more are local extensions, **not from the paper**: filter 4 fires when a
user's edits in one hour span at least 3 cells covering 20 km², filter 5 when
one tag key covers >90% of a 1-hour window's modified/deleted objects (minimum
100 objects), and filter 6 when language tags are removed, overwritten or
swapped on an object.

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
| 6. Language tags | A language variant (`name:xx`, `wikipedia:xx`) removed, overwritten or swapped, on at least one object of that day and language | The object count in `Name lang events` |

Three details about how they are applied:

- **Filter 1 only looks forward.** It marks the days a low-ranking
  contributor edits *from now on*. As someone builds a history their
  ranking rises, and the days already marked stay marked — but the past is
  never re-examined because a ranking later dropped.
- **Filters 2, 3, 4, 5 and 6 follow bursts and mistakes, not people.** All
  five fire on activity, so a single flagged day says nothing about a
  contributor's usual behaviour; the whole timeline on the
  [contributor's profile page](../users/HOW_IT_WORKS.md) is the context.
- **Filter 6 ignores near misses.** A bare `name` value that moved by at most
  two characters is treated as a typo fix and not counted, and an object counts
  once per day and language however many of its tags changed.

## Reference

The first three filters are from:

> Pascal Neis, Marcus Goetz, Alexander Zipf.
> *Towards Automatic Vandalism Detection in OpenStreetMap.*
> ISPRS International Journal of Geo-Information 2012, 1(3), 315-332.
> DOI: [10.3390/ijgi1030315](https://doi.org/10.3390/ijgi1030315)

The paper describes a rule-based system that scores each edit by combining
the contributor's standing with the edit itself, evaluated independently so
that different patrol groups can weight them differently. KarmaMap
implements the contributor standing and three of the paper's filters, and
publishes both so a reader can judge how much to trust a flag:

- Filter 1: "Show all edits of new users and/or users with a very low
  ranking (<5%)."
- Filter 2: "Show all users who modified or deleted more than 500 objects
  within one hour."
- Filter 3: "Show all users who modified node objects and moved the object
  for more than 500m."

Filter 1 reads the contributor standing, which the
[contributor's profile guide](../users/HOW_IT_WORKS.md) explains. Not
implemented here: the
paper's per-edit analysis of whether tag combinations are valid, its finer
flag for geometry moved about 11 m, and the black-list and white-list of
accounts that a patrol would maintain by hand.
