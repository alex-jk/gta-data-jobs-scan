"""
Read-only smoke test: summarizes just the first 4 rows of the real
simplyhired_final_cleaned.csv and prints title/description/summary side by
side, so the result can be eyeballed directly instead of trusting the batch
run blind. Does not write any output file -- safe to run alongside a full
summarize_existing.py run in another terminal.
"""
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from job_scan import summarize_new_jobs_buffer

df = pd.read_csv("simplyhired_final_cleaned.csv").head(4)
rows = df.to_dict(orient="records")
result = summarize_new_jobs_buffer(rows)

for _, row in result.iterrows():
    print("=" * 90)
    print("TITLE:  ", row["title"])
    print("COMPANY:", row.get("company"))
    print("URL:    ", row.get("url"))
    print(f"\nDESCRIPTION ({len(str(row['description']))} chars):")
    print(str(row["description"])[:400], "...")
    print("\nSUMMARY:")
    print(row["description_summary"])
    print()
