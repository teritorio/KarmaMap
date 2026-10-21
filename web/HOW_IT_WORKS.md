# How it works

This guide walks through the three KarmaMap pages: what each one shows,
what every number means, and how to read it without any technical
background. Each page has its own guide:

- **[The changes map](changes/HOW_IT_WORKS.md)** — hexagonal cells shaded by
  how many map edits happened in them, and what a cell's number counts.
- **[The contributor's profile](users/HOW_IT_WORKS.md)** — one user's 0-100
  ranking, the raw counts behind it, and their day-by-day activity.
- **[Suspect flagged days](suspect/HOW_IT_WORKS.md)** — the 100 most recent
  days a review flagged, what each column means, and the filters behind them.

Two things are true everywhere on the site:

- **The data is read in your browser.** The pages fetch only the rows they
  need from a set of public data files. Nothing you do here is sent to a
  server, and no query is run on your behalf.
- **Everything is counted in UTC calendar days.** A "day" is a UTC day, not
  your local day, and a user's activity is grouped into the day it happened
  in UTC.
