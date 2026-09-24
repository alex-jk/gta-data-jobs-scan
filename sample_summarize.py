import os
import json
import pandas as pd
from job_scan import summarize_new_jobs_buffer

# Read sample jobs from existing scraped CSV if present, otherwise create dummy samples
CSV = "simplyhired_final_cleaned.csv"
if os.path.exists(CSV):
    df = pd.read_csv(CSV)
    samples = df.head(5).to_dict(orient="records")
else:
    samples = [
        {"title": "Senior Python Developer", "description": "We are looking for a Senior Python developer with experience in Django, REST, AWS, PostgreSQL. Responsibilities include building APIs and mentoring juniors.", "salary": "N/A"},
        {"title": "Data Scientist", "description": "Data Scientist role: experience with ML pipelines, sklearn, pandas, statistics, model deployment.", "salary": "N/A"},
        {"title": "Frontend Engineer", "description": "React, TypeScript, CSS, and accessibility experience required. Work with designers to build UI components.", "salary": "N/A"},
        {"title": "DevOps Engineer", "description": "Experience with CI/CD, Docker, Kubernetes, infrastructure as code, monitoring and logging.", "salary": "N/A"},
        {"title": "Product Manager", "description": "Work with stakeholders to define product requirements, roadmap, and drive feature delivery.", "salary": "N/A"},
    ]

print(f"Running sample summarize for {len(samples)} jobs using environment settings.")
results_df = summarize_new_jobs_buffer(samples)
print("--- Results ---")
print(results_df[["title", "description_summary", "salary"]].to_json(orient="records", force_ascii=False, indent=2))
with open("sample_summaries.json", "w", encoding="utf-8") as f:
    f.write(results_df[["title", "description_summary", "salary"]].to_json(orient="records", force_ascii=False, indent=2))
print("Wrote sample_summaries.json")
