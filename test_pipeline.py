"""
Tests for salary parsing, profile scoring and de-duplication.

Runs under pytest, or standalone with `python test_pipeline.py`.
"""

import pandas as pd

from salary import best_salary, parse_salary, score_profile_fit
from job_scan import canonical_url, dedupe_jobs, job_signature, sanitize_title


# ---------------------------------------------------------------------------
# Salary parsing: figures that ARE compensation
# ---------------------------------------------------------------------------

def test_annual_range():
    s = parse_salary("$120,000 - $150,000 a year")
    assert s.period == "year"
    assert s.min_annual == 120_000
    assert s.max_annual == 150_000


def test_annual_range_en_dash_and_per_year():
    s = parse_salary("Base pay range $118,000–$142,500 per year")
    assert s.min_annual == 118_000
    assert s.max_annual == 142_500


def test_k_suffix():
    s = parse_salary("Compensation: $130K to $160K annually")
    assert s.min_annual == 130_000
    assert s.max_annual == 160_000


def test_hourly_is_annualised():
    s = parse_salary("$60.00 - $72.50 an hour")
    assert s.period == "hour"
    assert s.min_annual == 60.0 * 2080
    assert s.max_annual == 72.5 * 2080


def test_monthly_is_annualised():
    s = parse_salary("$11,000 per month")
    assert s.period == "month"
    assert s.max_annual == 132_000


def test_single_figure_with_salary_cue_and_no_period():
    s = parse_salary("Salary: $135,000")
    assert s.period == "year"
    assert s.max_annual == 135_000


def test_ca_dollar_prefix():
    s = parse_salary("CA$125,000 per annum")
    assert s.max_annual == 125_000


# ---------------------------------------------------------------------------
# Salary parsing: figures that are NOT compensation
# ---------------------------------------------------------------------------

def test_rejects_assets_under_management():
    assert not parse_salary("We manage over $500 million in assets.").found


def test_rejects_revenue():
    assert not parse_salary("The firm generated $2.5 billion in revenue.").found


def test_rejects_wellness_perk():
    assert not parse_salary("Benefits include a $2,000 wellness spending account.").found


def test_rejects_referral_bonus():
    assert not parse_salary("Referral bonus of $1,500 for every placement.").found


def test_rejects_bare_dollar_amount():
    assert not parse_salary("Projects valued at $40,000 each.").found


def test_rejects_empty_and_na():
    assert not parse_salary("").found
    assert not parse_salary("N/A").found


def test_picks_band_over_nearby_bonus():
    text = (
        "Signing bonus of $5,000. The salary range for this role is "
        "$128,000 - $155,000 per year."
    )
    s = parse_salary(text)
    assert s.min_annual == 128_000
    assert s.max_annual == 155_000


def test_best_salary_falls_back_to_description():
    # LinkedIn never fills the salary field; the number is in the description.
    s = best_salary("N/A", "Base salary: $140,000 - $165,000 per year.")
    assert s.max_annual == 165_000


# ---------------------------------------------------------------------------
# Target threshold
# ---------------------------------------------------------------------------

def test_meets_uses_top_of_range():
    assert parse_salary("$110,000 - $135,000 a year").meets(118_000)
    assert not parse_salary("$95,000 - $110,000 a year").meets(118_000)


def test_unknown_salary_does_not_meet_target():
    assert not parse_salary("Competitive salary offered").meets(118_000)


# ---------------------------------------------------------------------------
# Profile fit
# ---------------------------------------------------------------------------

def test_profile_fit_matches_core_stack():
    score, matched = score_profile_fit(
        "Senior Data Scientist",
        "You will use Python, SQL and Dataiku to build predictive models "
        "and present results in Tableau to banking stakeholders.",
    )
    assert score > 0
    for skill in ("python", "sql", "dataiku", "tableau", "machine learning"):
        assert skill in matched


def test_profile_fit_ignores_placeholder_text():
    score, matched = score_profile_fit("N/A", "N/A")
    assert score == 0
    assert matched == ""


# ---------------------------------------------------------------------------
# URL canonicalisation and de-duplication
# ---------------------------------------------------------------------------

def test_canonical_url_strips_query_and_trailing_slash():
    a = canonical_url("https://www.linkedin.com/jobs/view/4012345678/?refId=abc")
    b = canonical_url("https://www.linkedin.com/jobs/view/4012345678")
    assert a == b


def test_canonical_url_handles_missing():
    assert canonical_url("N/A") == ""
    assert canonical_url("") == ""
    assert canonical_url(None) == ""


def test_signature_ignores_punctuation_and_case():
    assert job_signature("Data Scientist", "Intact.") == job_signature(
        "data scientist", "Intact"
    )


def test_sanitize_title_removes_doubling_and_noise():
    assert sanitize_title("Data Scientist Data Scientist") == "Data Scientist"
    assert sanitize_title("Data ScientistData Scientist") == "Data Scientist"
    assert sanitize_title("Statistician with verification") == "Statistician"


def test_dedupe_keeps_distinct_jobs_without_urls():
    """The old pipeline collapsed every url-less row into one, because it
    de-duplicated on the literal string "N/A"."""
    df = pd.DataFrame(
        [
            {"title": "Data Scientist", "company": "RBC", "url": "N/A"},
            {"title": "Senior Data Scientist", "company": "TD", "url": "N/A"},
            {"title": "Statistician", "company": "Shopify", "url": ""},
            {"title": "ML Engineer", "company": "Wealthsimple", "url": "https://x/1"},
        ]
    )
    out = dedupe_jobs(df)
    assert len(out) == 4


def test_dedupe_removes_real_duplicates():
    df = pd.DataFrame(
        [
            {"title": "Data Scientist", "company": "RBC", "url": "https://x/1"},
            {"title": "Data Scientist", "company": "RBC", "url": "https://x/1?src=a"},
            {"title": "Data Scientist Data Scientist", "company": "RBC.", "url": ""},
            {"title": "Data Analyst", "company": "RBC", "url": "https://x/2"},
        ]
    )
    out = dedupe_jobs(df)
    assert len(out) == 2
    assert set(out["title"]) == {"Data Scientist", "Data Analyst"}


if __name__ == "__main__":
    import sys

    failures = 0
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_")]
    for name, fn in tests:
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL  {name}: {exc or 'assertion failed'}")
        except Exception as exc:  # noqa: BLE001 - surface any error in the report
            failures += 1
            print(f"ERROR {name}: {type(exc).__name__}: {exc}")

    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    sys.exit(1 if failures else 0)
