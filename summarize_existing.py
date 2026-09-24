import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd

CSV = "simplyhired_final_cleaned.csv"
OUT_CSV = "simplyhired_final_cleaned_with_summaries.csv"
SHORTLIST = "unique_jobs_shortlist.csv"

if not os.path.exists(CSV):
    print(f"Source CSV not found: {CSV}")
    raise SystemExit(1)

from job_scan import summarize_new_jobs_buffer

# Read
df = pd.read_csv(CSV)

# Ensure a column for summaries
if "description_summary" not in df.columns:
    df["description_summary"] = pd.NA

# Select rows missing a summary
mask = df["description_summary"].isna() | (df["description_summary"].astype(str).str.strip() == "")
rows_to_summarize = df[mask]

if rows_to_summarize.empty:
    print("No rows need summarization. Exiting.")
    # Still write shortlist if not present
    shortlist_cols = [c for c in ["title", "company", "location", "salary", "description_summary", "url"] if c in df.columns]
    df[shortlist_cols].to_csv(SHORTLIST, index=False)
    print(f"Wrote shortlist to {SHORTLIST}")
    raise SystemExit(0)

print(f"Summarizing {len(rows_to_summarize)} rows (only missing summaries)...")

# Keep original indices to merge back
rows = rows_to_summarize.to_dict(orient="records")

result_df = summarize_new_jobs_buffer(rows)

# result_df corresponds to rows in the same order; merge back using a temporary index column
result_df = result_df.reset_index(drop=True)
rows_to_summarize = rows_to_summarize.reset_index()

# Map summaries and salary back into original df by matching on row order
for i, r in result_df.iterrows():
    orig_index = rows_to_summarize.at[i, 'index']
    if 'description_summary' in r:
        df.at[orig_index, 'description_summary'] = r.get('description_summary')
    if 'salary' in r:
        df.at[orig_index, 'salary'] = r.get('salary', df.at[orig_index, 'salary'])

# Save updated CSV (backup original)
backup = CSV + ".bak"
if not os.path.exists(backup):
    os.rename(CSV, backup)
    print(f"Backed up original CSV to {backup}")

df.to_csv(OUT_CSV, index=False)
print(f"Wrote updated CSV with summaries to {OUT_CSV}")

# Write a shortlist with common columns
shortlist_cols = [c for c in ["title", "company", "location", "salary", "description_summary", "url"] if c in df.columns]
if shortlist_cols:
    df[shortlist_cols].to_csv(SHORTLIST, index=False)
    print(f"Wrote shortlist to {SHORTLIST}")
else:
    print("No shortlist columns available to write.")
