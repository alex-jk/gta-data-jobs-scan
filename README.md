# GTA Data Jobs Scan

Scrapes data science / analytics postings in the Greater Toronto Area from
SimplyHired and LinkedIn, de-duplicates them, and exports a shortlist filtered
by salary and ranked by how well each posting matches your skill profile.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`torch` is intentionally unpinned. It is only needed for the optional
description summariser; the scraper and the salary pipeline run without it.

## Running

```bash
python job_scan.py
```

| Option | What it does |
| ------ | ------------ |
| 1 | Scrape new jobs, then de-duplicate and export the shortlist |
| 2 | Re-check saved URLs and drop postings that are no longer live |
| 3 | De-duplicate the main CSV only |
| 4 | Re-export the shortlist from the existing CSV (no scraping) |

Option 1 pauses partway through and opens the LinkedIn login page. Log in
manually in the browser window, then press ENTER in the terminal. Because of
that pause, the scrape has to run on a machine where you can see the browser.

## Output

| File | Contents |
| ---- | -------- |
| `simplyhired_final_cleaned.csv` | Every unique posting scraped so far |
| `unique_jobs_shortlist.csv` | The shortlist, ranked |

Most employers never publish a salary, so a missing figure is treated as
**unknown**, not as a failure to meet the target. Those postings stay in the
shortlist and are ranked by profile fit. Only postings that publish a salary
*and* fall below the target are dropped.

Every row carries a `salary_status` column, so the file can be re-sorted or
re-filtered in a spreadsheet without rerunning anything:

| `salary_status` | Meaning |
| --------------- | ------- |
| `meets_target` | Published salary reaches `SALARY_TARGET_CAD` |
| `not_posted` | No salary published; kept and ranked by fit |
| `below_target` | Published salary is under target; excluded by default |

The shortlist is ordered with confirmed matches first, then unpublished
salaries, each group ranked by `fit_score`. Set `EXCLUDE_BELOW_TARGET = False`
to keep below-target postings too; they stay in the main CSV either way.

## Tuning

All knobs are constants at the top of `job_scan.py`:

- `SALARY_TARGET_CAD` - minimum acceptable annual compensation. A posting
  qualifies when the **top** of its advertised range reaches this figure, so a
  range straddling the target is kept.
- `EXCLUDE_BELOW_TARGET` - whether to drop postings whose published salary is
  under the target. Never affects postings with no published salary.
- `KEYWORDS`, `LOCATION`, `RADIUS` - what and where to search.
- `STRONG_KEYWORDS` / `AMBIGUOUS_KEYWORDS` / `BAD_KEYWORDS` - title filtering.
  A strong title is kept outright; an ambiguous one is kept only if the
  description contains a `TECH_KEYWORDS` term.

Skill weights used for `fit_score` live in `PROFILE_SKILLS` in `salary.py`.

## Salary handling

Hourly, daily, weekly and monthly pay are normalised to an annual figure
(2080 hours/year) so everything is comparable on one scale. Dollar amounts that
are not compensation, such as assets under management, revenue, referral
bonuses and wellness accounts, are rejected rather than recorded as pay.

## Tests

```bash
python test_pipeline.py     # or: pytest test_pipeline.py
```

## Analysis notebook

`summarize_jobs_data.ipynb` clusters job titles with TF-IDF and agglomerative
clustering. Run the scraper first; the notebook reads
`simplyhired_final_cleaned.csv`.
