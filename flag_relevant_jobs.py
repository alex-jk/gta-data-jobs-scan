"""
Adds `ml_relevant` (True/False) + `relevance_reason` columns to
simplyhired_final_cleaned.csv, flagging postings that involve hands-on
statistical modeling / machine learning / clustering work, as opposed to
roles that are primarily reporting, dashboards, or descriptive BI analytics.

A keyword match can't reliably tell these apart -- a "Data Analyst" posting
can secretly involve real modeling, and a "Data Scientist" title can secretly
be pure dashboard work -- so this reads each job's content via the OpenAI API,
the same way summarize_existing.py does.

Re-running only classifies rows that don't have a flag yet, so it's safe to
run again after a fresh scrape without re-spending on rows already done.
"""
import os
import time

import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

CSV = "simplyhired_final_cleaned.csv"
CHECKPOINT_EVERY = 25

CRITERIA = """You are screening data job postings for a candidate who specifically wants
HANDS-ON statistical modeling, machine learning, or clustering / predictive-modeling work
(e.g. building models, feature engineering, applying ML algorithms, experimentation,
statistical analysis for inference or prediction).

The candidate does NOT want roles that are primarily about building dashboards, reports,
descriptive BI analytics, data visualization, or basic SQL reporting -- even if those roles
mention "data", "analytics", or "AI" heavily in passing.

Given the job title and description below, decide if this role is a good match.
Respond in EXACTLY this format and nothing else:
RELEVANT|<one short sentence reason>
or
NOT_RELEVANT|<one short sentence reason>
"""


def classify(client, title, text, model="gpt-3.5-turbo"):
    if not text or text.strip().lower() in {"", "n/a", "nan"}:
        return False, "no description available to judge"
    prompt = f"{CRITERIA}\n\nTitle: {title}\n\nDescription:\n{text[:2000]}"
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            max_tokens=60,
        )
        out = resp.choices[0].message.content.strip()
        label, _, reason = out.partition("|")
        is_relevant = label.strip().upper().startswith("RELEVANT")
        return is_relevant, (reason.strip() or out)
    except Exception as exc:
        return False, f"classification failed: {type(exc).__name__}"


def main():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY not set.")
        raise SystemExit(1)
    client = OpenAI(api_key=api_key)

    df = pd.read_csv(CSV)

    for col in ("ml_relevant", "relevance_reason"):
        if col not in df.columns:
            df[col] = pd.NA
        # Blank/all-NaN columns read back as float64; writing a bool/string
        # into a float64 cell raises a hard TypeError in this pandas version.
        df[col] = df[col].astype(object)

    targets = df[df["ml_relevant"].isna()]
    print(f"Classifying {len(targets)} rows (skipping {len(df) - len(targets)} already flagged)...")
    if targets.empty:
        return

    start = time.time()
    for n, (idx, row) in enumerate(targets.iterrows(), start=1):
        title = str(row.get("title", ""))
        text = str(row.get("description_summary", "")).strip()
        if not text or text.lower() == "nan":
            text = str(row.get("description", "")).strip()

        t0 = time.time()
        is_relevant, reason = classify(client, title, text)
        elapsed = time.time() - t0

        df.at[idx, "ml_relevant"] = is_relevant
        df.at[idx, "relevance_reason"] = reason

        tag = "ML/STATS" if is_relevant else "reporting/other"
        print(f"[{n}/{len(targets)}] {elapsed:4.1f}s  [{tag:16}]  {title[:60]}  -- {reason}")

        if n % CHECKPOINT_EVERY == 0:
            df.to_csv(CSV, index=False, encoding="utf-8")
            print(f"   -- checkpoint saved ({n}/{len(targets)}) --")

    df.to_csv(CSV, index=False, encoding="utf-8")
    n_relevant = df["ml_relevant"].fillna(False).astype(bool).sum()
    print(f"\nDone in {time.time() - start:.0f}s. {n_relevant}/{len(df)} rows flagged as ML/stats-relevant.")


if __name__ == "__main__":
    main()
