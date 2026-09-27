import os
import random
import re
import time
from urllib.parse import urlsplit

import pandas as pd
from bs4 import BeautifulSoup

from salary import best_salary, score_profile_fit

# Selenium is only needed for the scraping/verification paths. The cleaning,
# de-duplication and export paths run on a plain CSV, so importing this module
# must not require a browser stack to be installed.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import NoSuchElementException
    from webdriver_manager.chrome import ChromeDriverManager

    SELENIUM_AVAILABLE = True
    SELENIUM_IMPORT_ERROR = None
except ImportError as exc:  # pragma: no cover - depends on local install
    SELENIUM_AVAILABLE = False
    SELENIUM_IMPORT_ERROR = exc
    NoSuchElementException = Exception


# ---------- CONFIG ----------
KEYWORDS = [
    "data scientist",
    "data analyst",
    "statistician",
    "senior data scientist",
    "advanced analytics",
]
LOCATION = "Toronto, ON"
RADIUS = 50
OUTPUT_FILE = "simplyhired_final_cleaned.csv"
UNIQUE_JOBS_FILE = "unique_jobs_shortlist.csv"
MAX_JOBS_TO_SCRAPE = 500
MAX_PAGES_PER_KEYWORD = 18
# How many buffered scraped jobs to collect before flushing to disk
FLUSH_EVERY = 50

# Minimum acceptable total annual compensation, in CAD. Postings are kept when
# the TOP of their advertised range reaches this figure.
SALARY_TARGET_CAD = 118_000

# Drop postings that publish a salary below the target. Postings that publish
# NO salary are never dropped by this: most employers omit compensation, so
# treating a missing figure as a failure would hide the majority of real leads.
# Set to False to keep below-target postings in the shortlist too.
EXCLUDE_BELOW_TARGET = True

# Values of the salary_status column in the exported shortlist.
SALARY_STATUS_MEETS = "meets_target"
SALARY_STATUS_UNKNOWN = "not_posted"
SALARY_STATUS_BELOW = "below_target"

# --- ADVANCED KEYWORD LOGIC ---
# Only genuinely off-target roles belong here. Seniority words (manager, lead,
# principal, director) are deliberately NOT vetoed: senior individual-contributor
# and player-coach data roles are the ones that clear the salary target, and
# vetoing them threw away the best-paying matches. A senior title with no data
# signal still gets dropped, because it matches neither list below.
BAD_KEYWORDS = [
    "intern", "co-op", "coop", "student", "summer", "placement", "junior",
    "sales", "customer service", "technician", "support", "clerk", "admin",
    "marketing", "account executive", "driver", "warehouse", "nurse",
    "receptionist", "cashier", "server", "cook", "security guard", "grad", "graduate"
]

STRONG_KEYWORDS = [
    "data scientist", "data science", "data engineer", "machine learning",
    "ai engineer", "analytics", "computer vision", "nlp", "business intelligence",
    "deep learning", "data analyst", "quantitative researcher", "quantitative analyst",
    "statistical modeling", "statistical modelling", "statistician", "biostatistician",
    "statistics", "statistical", "econometric", "decision scientist", "decision science",
    "applied scientist", "research scientist", "risk modeling", "risk modelling",
]

AMBIGUOUS_KEYWORDS = [
    "analyst", "insights", "consultant", "scientist", "researcher",
    "strategist", "specialist", "associate",
    # Seniority words: kept only when the description proves a technical role.
    "manager", "lead", "principal", "director", "head of",
]

TECH_KEYWORDS = [
    "sql", "python", "r-programming", "tableau", "power bi", "powerbi",
    "aws", "azure", "gcp", "snowflake", "etl", "pipeline", "modeling", "modelling",
    "models", "machine learning", "statistical", "statistics", "looker", "bigquery",
    "spark", "hadoop", "dataiku", "sas", "databricks", "scikit-learn", "pandas",
    "regression", "forecasting", "experimentation", "a/b test",
]

# Output schema. The scraped description is preserved verbatim; the optional
# summariser writes to description_summary so salary text is never destroyed.
OUTPUT_COLUMNS = [
    "title", "url", "company", "description", "description_summary", "salary",
    "salary_min_annual", "salary_max_annual", "salary_period",
    "meets_salary_target", "fit_score", "matched_skills",
    "qualifications", "scraped_at",
]

# --- STRICT FIELD VALIDATION ---
REQUIRE_COMPANY = True          # hard stop for saving/buffering
REQUIRE_SALARY = False          # optional (usually too strict)
DEBUG_EVERY_SKIP = True         # print reasons for skips


def norm(s):
    return str(s or "").strip()


def is_missing(s):
    s = norm(s)
    return (s == "" or s.lower() in {"n/a", "na", "none", "null"})


def fix_doubled_title(text):
    """Fix titles scraped twice, e.g. 'Data Scientist Data Scientist'.

    Kept as the name used throughout the scrapers; the implementation lives in
    sanitize_title so the scraper and the CSV cleaner cannot drift apart.
    """
    return sanitize_title(text)


def dbg(status, title=None, company=None, salary=None, url=None, reason=None):
    t = norm(title)[:80]
    c = norm(company)[:60]
    s = norm(salary)[:40]
    u = norm(url)[:90]
    msg = f"[{status}] title='{t}' | company='{c}' | salary='{s}' | url='{u}'"
    if reason:
        msg += f" | reason={reason}"
    print(msg)


def make_driver():
    if not SELENIUM_AVAILABLE:
        raise RuntimeError(
            "Selenium and webdriver-manager are required for scraping. "
            f"Install them with `pip install -r requirements.txt` ({SELENIUM_IMPORT_ERROR})."
        )
    opts = Options()
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver


def clean_salary_text(text: str) -> str:
    """Return the compensation snippet from a blob of text, or "N/A".

    Delegates to the validated parser so that unrelated dollar amounts
    ("$500 million in assets", "$2,000 wellness spend") are not mistaken for pay.
    """
    if not text:
        return "N/A"
    parsed = best_salary(text)
    return parsed.raw if parsed.found else "N/A"


# ----------------------------
# URL / signature helpers (shared by the scraper and the CSV cleaner)
# ----------------------------
def canonical_url(url) -> str:
    """Normalise a job URL so the same posting compares equal.

    Strips query strings, fragments and trailing slashes, lowercases the host,
    and maps placeholders to the empty string. Returning "" for a missing URL
    matters: empty URLs must never be treated as equal to one another.
    """
    if url is None:
        return ""
    text = str(url).strip()
    if not text or text.lower() in {"n/a", "na", "none", "null", "nan"}:
        return ""
    if not text.lower().startswith("http"):
        return ""
    parts = urlsplit(text)
    path = parts.path.rstrip("/")
    return f"{parts.scheme.lower()}://{parts.netloc.lower()}{path}"


def job_signature(title, company) -> str:
    """A punctuation- and case-insensitive identity for a posting."""
    t = re.sub(r"[^a-z0-9]", "", str(title or "").lower())
    c = re.sub(r"[^a-z0-9]", "", str(company or "").lower())
    return f"{c}_{t}"


def sanitize_title(text) -> str:
    """Strip listing noise and collapse titles that were scraped twice."""
    if not text:
        return ""

    clean = str(text)
    noise_phrases = ["with verification", " - Job", "(f/m/d)", "(m/f/d)"]
    for phrase in noise_phrases:
        clean = re.compile(re.escape(phrase), re.IGNORECASE).sub("", clean)

    clean = " ".join(clean.split())

    # "Data Scientist Data Scientist" -> "Data Scientist"
    words = clean.split()
    if len(words) >= 2 and len(words) % 2 == 0:
        mid = len(words) // 2
        if [w.lower() for w in words[:mid]] == [w.lower() for w in words[mid:]]:
            return " ".join(words[:mid])

    # "Data ScientistData Scientist" -> "Data Scientist"
    if len(clean) > 6 and len(clean) % 2 == 0:
        mid = len(clean) // 2
        if clean[:mid].lower() == clean[mid:].lower():
            return clean[:mid]

    return clean


# ----------------------------
# SimplyHired Parser
# ----------------------------
def parse_job_data(driver, card, prev_desc):
    data = {}

    # Parse card HTML for TRUSTED data (Title, URL, Company)
    try:
        card_html = card.get_attribute("outerHTML")
        soup = BeautifulSoup(card_html, "lxml")

        title_tag = (soup.find("a", class_=lambda x: x and "jobTitle" in x) or soup.find("a"))
        title = fix_doubled_title(title_tag.get_text(strip=True)) if title_tag else ""

        href = title_tag.get("href", "") if title_tag else ""
        url = ("https://www.simplyhired.ca" + href.split("?")[0]) if href and not href.startswith("http") else href

        company_tag = soup.find("span", attrs={"data-testid": "companyName"})
        company = company_tag.get_text(strip=True) if company_tag else ""

        data.update({"title": title, "company": company, "url": url})
    except Exception as e:
        dbg("FAIL_CARD_PARSE", reason=f"{type(e).__name__}")
        return None

    if is_missing(data["title"]) or is_missing(data["url"]):
        dbg("SKIP", title=data.get("title"), company=data.get("company"), url=data.get("url"),
            reason="missing title or url on card")
        return None

    if REQUIRE_COMPANY and is_missing(data["company"]):
        dbg("SKIP_SH", title=data["title"], company=data["company"], url=data["url"],
            reason="missing company on card (hard gate)")
        return None

    dbg("PROCESSING_SH", title=data["title"], company=data["company"], url=data["url"])

    # Click card to load right pane
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
        time.sleep(0.4)
        try:
            card.find_element(By.TAG_NAME, "a").click()
        except Exception:
            driver.execute_script("arguments[0].click();", card)
    except Exception as e:
        dbg("SKIP", title=data["title"], company=data["company"], url=data["url"],
            reason=f"click failed: {type(e).__name__}")
        return None

    # --- SYNCHRONIZATION: Wait for Pane to Match Card ---
    pane_matched = False
    
    # Selectors to find company/title in the Right Pane
    pane_checks = [
        ('span[data-testid="viewJobCompanyName"]', "company"),
        ('[data-testid="viewJobCompanyName"]', "company"),
        ('h2[data-testid="viewJobBodyJobTitle"]', "title"), # fallback
    ]

    for _ in range(15): # Try for ~3 seconds
        for sel, type_ in pane_checks:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                txt = el.text.strip()
                if not txt: continue
                
                # Loose matching to account for minor formatting diffs
                if type_ == "company":
                    # Check if card company is inside pane text or vice versa
                    if (data["company"].lower() in txt.lower()) or (txt.lower() in data["company"].lower()):
                        pane_matched = True
                        break
                elif type_ == "title":
                    if (data["title"][:10].lower() in txt.lower()):
                        pane_matched = True
                        break
            except Exception:
                pass
        
        if pane_matched:
            break
        time.sleep(0.2)

    if not pane_matched:
        dbg("SKIP_SYNC_FAIL", title=data["title"], company=data["company"], 
            reason="Right pane did not update to match card details")
        return None

    # Description - STRICT PREV_DESC CHECK
    desc_text = "N/A"
    
    # We will poll the description element.
    # It MUST be different from prev_desc to be accepted (unless prev_desc was N/A).
    start_desc_time = time.time()
    while (time.time() - start_desc_time) < 6.0:
        try:
            elem = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="viewJobBodyJobFullDescriptionContent"]')
            txt = elem.text.strip()
            
            # If we have text, and it is NOT the same as the previous job's description
            if txt and (txt != prev_desc or prev_desc == ""):
                desc_text = txt
                break
                
            # If prev_desc is not empty and txt IS equal to prev_desc, it means stale data.
            # We continue looping/waiting for it to change.
        except Exception:
            pass
        time.sleep(0.5)

    data["description"] = desc_text

    # Salary: prefer the dedicated compensation box, then fall back to the
    # description. The old fallback scanned the whole job pane for any "$",
    # which routinely captured revenue figures and perk amounts as pay.
    salary_box_text = ""
    try:
        salary_box_text = driver.find_element(
            By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"]'
        ).text
    except Exception:
        salary_box_text = ""

    data["salary"] = clean_salary_text(salary_box_text) if salary_box_text else "N/A"
    if is_missing(data["salary"]):
        data["salary"] = clean_salary_text(desc_text)

    if REQUIRE_SALARY and is_missing(data["salary"]):
        dbg("SKIP_SH", title=data["title"], company=data["company"], salary=data["salary"], url=data["url"],
            reason="missing salary (hard gate)")
        return None

    # Qualifications
    try:
        quals = driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="viewJobQualificationItem"]')
        data["qualifications"] = "; ".join(q.text.strip() for q in quals if q.text and q.text.strip()) or "N/A"
    except Exception:
        data["qualifications"] = "N/A"

    data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    annotate_job(data)

    dbg("SCRAPED_OK_SH", title=data["title"], company=data["company"], salary=data["salary"], url=data["url"])
    return data


def annotate_job(data: dict) -> dict:
    """Attach normalised salary and profile-fit columns to a scraped row.

    Salary is read from the salary field, the description and the
    qualifications, because most boards (LinkedIn in particular) never populate
    a structured compensation field.
    """
    parsed = best_salary(
        data.get("salary", ""),
        data.get("description", ""),
        data.get("qualifications", ""),
    )
    data["salary_min_annual"] = parsed.min_annual if parsed.found else ""
    data["salary_max_annual"] = parsed.max_annual if parsed.found else ""
    data["salary_period"] = parsed.period or ""
    data["meets_salary_target"] = parsed.meets(SALARY_TARGET_CAD)
    if is_missing(data.get("salary")) and parsed.found:
        data["salary"] = parsed.raw

    score, matched = score_profile_fit(
        data.get("title", ""),
        data.get("description", ""),
        data.get("qualifications", ""),
    )
    data["fit_score"] = score
    data["matched_skills"] = matched
    data.setdefault("description_summary", "")
    return data


# ----------------------------
# Summarizer (optional)
# ----------------------------
def summarize_new_jobs_buffer(new_jobs_list):
    if not new_jobs_list:
        return pd.DataFrame()

    import time
    import os

    total = len(new_jobs_list)
    print(f"\n--- Summarizing {total} NEW jobs ---")

    # Choose summariser: OpenAI (preferred if API key is set and USE_OPENAI_SUMMARIZER=1),
    # otherwise fall back to local transformers if installed. This keeps costs controllable
    # and allows switching without code edits.
    use_openai = bool(os.environ.get("USE_OPENAI_SUMMARIZER", "1").strip() in {"1", "true", "True"})
    openai_client = None
    if use_openai:
        try:
            # openai>=1.0 removed the old openai.ChatCompletion.create()
            # global-style call entirely (raises APIRemovedInV1) in favour of
            # an OpenAI() client instance -- calling the old interface used
            # to fail silently here and return the raw, unsummarized text for
            # every single row.
            from openai import OpenAI
            api_key = os.environ.get("OPENAI_API_KEY")
            if not api_key:
                print("OPENAI_API_KEY not set — falling back to local transformers (if available)")
                use_openai = False
            else:
                openai_client = OpenAI(api_key=api_key)
        except Exception as exc:
            print(f"OpenAI package not available ({exc}) — falling back to local transformers")
            use_openai = False

    # If not using OpenAI, try transformers (smaller model by default to reduce resource use)
    hf_tokenizer = hf_model = None
    if not use_openai:
        try:
            from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
            try:
                import torch
            except Exception:
                torch = None
            device = "cuda" if torch and torch.cuda.is_available() else "cpu"
            print(f"Running on device: {device.upper()}")
            # Use a smaller model to avoid excessive runtime/memory usage
            model_name = os.environ.get("HF_SUMMARIZER_MODEL", "google/flan-t5-small")
            hf_tokenizer = AutoTokenizer.from_pretrained(model_name)
            hf_model = AutoModelForSeq2SeqLM.from_pretrained(model_name).to(device)
        except Exception as exc:
            print("Transformers unavailable or failed to load:", exc)
            print("Skipping summarization and returning raw descriptions")
            return pd.DataFrame(new_jobs_list)

    df_rows = []
    start_time = time.time()
    processed = 0

    _openai_error_shown = False

    def summarize_with_openai(text, model_name="gpt-3.5-turbo"):
        nonlocal _openai_error_shown
        if not text or text == "N/A":
            return text
        prompt = (
            "Summarize technical skills and duties in this job text, and extract salary or salary range if present. "
            "Return a single concise paragraph. If no salary is present, return 'N/A' for salary.\n\n" + text
        )
        try:
            resp = openai_client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=256,
            )
            return resp.choices[0].message.content.strip()
        except Exception as exc:
            # Surface the failure once instead of silently returning the raw
            # text for every row -- a systemic failure (bad key, removed API,
            # rate limit) used to look identical to a successful no-op run.
            if not _openai_error_shown:
                print(f"[WARN] OpenAI summarization call failed ({type(exc).__name__}: {exc}); "
                      f"returning raw description for this and any other failing rows.")
                _openai_error_shown = True
            return text

    def summarize_with_hf(text):
        if not text or text == "N/A" or len(str(text).split()) < 80:
            return text
        try:
            words = str(text).split()
            chunk_size = 450
            chunks = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]
            intermediate = []
            for chunk in chunks:
                prompt = f"Summarize technical skills and duties in this job text, obtain salary / salary range, if available:\n\n{chunk}"
                inputs = hf_tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(hf_model.device)
                outputs = hf_model.generate(inputs["input_ids"], max_length=150)
                intermediate.append(hf_tokenizer.decode(outputs[0], skip_special_tokens=True))

            final_prompt = "Write a professional paragraph job summary listing tech stack and responsibilities, obtain salary / salary range, if available:\n\n" + " ".join(intermediate)
            inputs = hf_tokenizer(final_prompt, return_tensors="pt", truncation=True, max_length=1024).to(hf_model.device)
            outputs = hf_model.generate(inputs["input_ids"], max_length=300, min_length=60)
            return hf_tokenizer.decode(outputs[0], skip_special_tokens=True)
        except Exception:
            return text

    def _safe_str(v):
        # DataFrame.to_dict() turns empty/NaN cells into a float NaN, not "";
        # .get(key, default) only falls back on a MISSING key, so a present-
        # but-NaN "description" or "title" reaches here as a float and blows
        # up string concatenation downstream.
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)

    for i, job in enumerate(new_jobs_list, start=1):
        t0 = time.time()
        title = _safe_str(job.get("title", ""))
        short_title = (title[:60] + "...") if len(title) > 60 else title
        processed += 1
        # Summarize using chosen backend
        text = _safe_str(job.get("description", ""))
        if use_openai:
            model_choice = os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo")
            summary = summarize_with_openai(text, model_choice)
        else:
            summary = summarize_with_hf(text)

        job_out = dict(job)
        # NOT setdefault(): every row read back from the CSV already carries
        # a "description_summary" key (blank/NaN), so setdefault silently
        # no-ops and the summary just computed above gets discarded.
        job_out["description_summary"] = summary
        # Normalize salary to string and handle NaN/None/float
        sal = job_out.get("salary")
        if sal is None or (isinstance(sal, float) and pd.isna(sal)):
            job_out["salary"] = "N/A"
        else:
            job_out["salary"] = str(sal).strip() if str(sal).strip() else "N/A"
        df_rows.append(job_out)

        # Progress and ETA
        elapsed = time.time() - start_time
        avg = elapsed / processed
        remaining = total - processed
        eta = remaining * avg
        print(f"[SUM] {processed}/{total} {short_title} — took {time.time()-t0:.1f}s (avg {avg:.1f}s) ETA {eta:.0f}s")

    df = pd.DataFrame(df_rows)
    return df


# ----------------------------
# LinkedIn helpers (AUTH UI)
# ----------------------------
# LinkedIn's job-search frontend now emits hashed, per-deploy CSS classes
# (e.g. "_1274a681", "cf8121e6") in place of the old stable names like
# "job-card-container" -- every selector built on those old classes matches
# zero elements now, which is why the scraper stopped finding cards entirely.
# Two attributes survive the redesign and stay put across deploys:
#   - componentkey="job-card-component-ref-<jobId>" on each card
#   - id="JobDetails_AboutTheJob_<jobId>" on the opened description pane
# Both are keyed by the numeric job id, which also gives us a canonical
# /jobs/view/<id>/ URL without needing to find an <a href> at all (most cards
# in the list have no anchor tag -- only the pre-selected one does; the rest
# are click-handled via JS with no href present).
_LINKEDIN_EXTRACT_CARDS_JS = """
const out = [];
const seen = new Set();
document.querySelectorAll('[componentkey^="job-card-component-ref-"]').forEach(el => {
  const ck = el.getAttribute('componentkey');
  const jobId = ck.split('job-card-component-ref-')[1];
  if (!jobId || seen.has(jobId)) return;
  seen.add(jobId);
  const span = el.querySelector('span[aria-hidden="true"]');
  let title = '';
  if (span) {
    title = Array.from(span.childNodes)
      .filter(n => n.nodeType === Node.TEXT_NODE)
      .map(n => n.textContent)
      .join('')
      .trim();
  }
  const ps = el.querySelectorAll('p');
  const company = ps.length > 1 ? ps[1].textContent.trim() : '';
  const location = ps.length > 2 ? ps[2].textContent.trim() : '';
  out.push({job_id: jobId, title: title, company: company, location: location});
});
return out;
"""

_LINKEDIN_CLICK_CARD_JS = """
const jobId = arguments[0];
const el = document.querySelector('[componentkey="job-card-component-ref-' + jobId + '"]');
if (!el) return false;
el.scrollIntoView({block: 'center'});
el.click();
return true;
"""


def linkedin_extract_cards(driver):
    """Return [{job_id, title, company, location}, ...] for every job card
    currently rendered in the results list, keyed off componentkey rather
    than CSS classes (see comment above)."""
    try:
        return driver.execute_script(_LINKEDIN_EXTRACT_CARDS_JS) or []
    except Exception:
        return []


def linkedin_click_card(driver, job_id) -> bool:
    try:
        return bool(driver.execute_script(_LINKEDIN_CLICK_CARD_JS, job_id))
    except Exception:
        return False


def linkedin_wait_for_description(driver, job_id, timeout=18.0) -> str:
    """Poll the description pane for the specific job id we just clicked.

    Keying the wait on the job id (rather than comparing text against the
    previous job's description) means we know precisely when the right pane
    has caught up, instead of guessing from a timeout.
    """
    end = time.time() + timeout
    sel = f'[id="JobDetails_AboutTheJob_{job_id}"]'
    while time.time() < end:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            txt = el.text.strip()
            if txt:
                return re.sub(r"^About the job\s*\n*", "", txt).strip()
        except Exception:
            pass
        time.sleep(0.3)
    return ""


# ----------------------------
# LinkedIn Auth Scraper
# ----------------------------
def scrape_linkedin_authenticated(driver, seen_signatures, seen_urls, new_jobs_buffer):
    print("\n\n=== STARTING LINKEDIN CHECK (AUTHENTICATED) ===")

    for kw in KEYWORDS:
        print(f"\n--- LinkedIn Search: {kw} ---")

        search_url = (f"https://www.linkedin.com/jobs/search/?keywords={kw.replace(' ', '%20')}"
                      f"&location={LOCATION.replace(' ', '%20')}")
        driver.get(search_url)
        time.sleep(5)

        current_page_num = 1

        while True:
            if len(new_jobs_buffer) >= MAX_JOBS_TO_SCRAPE:
                print("   [MAX JOBS LIMIT REACHED]")
                return

            if current_page_num > MAX_PAGES_PER_KEYWORD:
                print("   [PAGE LIMIT REACHED] Stopping LinkedIn scan for this keyword.")
                break

            print(f"   Processing Page {current_page_num}...")

            # Give the (virtualized) list a moment to render, then scroll it so
            # LinkedIn mounts cards beyond the first screenful.
            end_time = time.time() + 10
            cards = []
            while time.time() < end_time:
                cards = linkedin_extract_cards(driver)
                if cards:
                    break
                time.sleep(0.3)

            if not cards:
                print("   [ERROR] No job cards found on this page.")
                break

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            cards = linkedin_extract_cards(driver) or cards

            print(f"      Found {len(cards)} visible cards.")

            for card in cards:
                if len(new_jobs_buffer) >= MAX_JOBS_TO_SCRAPE:
                    return

                try:
                    job_id = card["job_id"]
                    raw_title = fix_doubled_title(card["title"])
                    raw_company = norm(card["company"])
                    job_url = f"https://www.linkedin.com/jobs/view/{job_id}/"

                    if is_missing(raw_title):
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP", reason=f"missing title on card {job_id}")
                        continue

                    if canonical_url(job_url) in seen_urls:
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP_DUP_URL", title=raw_title, url=job_url, reason="url already seen")
                        continue

                    if REQUIRE_COMPANY and is_missing(raw_company):
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP", title=raw_title, url=job_url, reason="missing company on card")
                        continue

                    sig = (raw_title.lower().strip(), raw_company.lower().strip())
                    if sig in seen_signatures:
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP_DUP_SIG", title=raw_title, company=raw_company, url=job_url, reason="duplicate")
                        continue

                    title_lower = raw_title.lower()
                    if any(bad in title_lower for bad in BAD_KEYWORDS):
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP_BAD_KW", title=raw_title, company=raw_company, reason="bad keyword")
                        continue

                    relevance_type = "SKIP"
                    if any(s in title_lower for s in STRONG_KEYWORDS):
                        relevance_type = "KEEP_IMMEDIATE"
                    elif any(a in title_lower for a in AMBIGUOUS_KEYWORDS):
                        relevance_type = "CHECK_DESCRIPTION"

                    if relevance_type == "SKIP":
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP", title=raw_title, company=raw_company, reason="irrelevant title")
                        continue

                    dbg("LI_CARD", title=raw_title, company=raw_company, url=job_url)
                    print(f"      [CLICKING] {raw_title}")

                    if not linkedin_click_card(driver, job_id):
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_SKIP", title=raw_title, url=job_url, reason="click failed")
                        continue

                    description = linkedin_wait_for_description(driver, job_id)
                    if not description:
                        # Under a long, heavy scraping session LinkedIn's pane
                        # sometimes renders slower than the wait allows, or the
                        # first click doesn't register -- one re-click clears
                        # the vast majority of these instead of giving up.
                        if DEBUG_EVERY_SKIP:
                            dbg("LI_RETRY_DESC", title=raw_title, url=job_url, reason="description wait timed out, retrying click")
                        linkedin_click_card(driver, job_id)
                        description = linkedin_wait_for_description(driver, job_id)
                    description = description or "N/A"

                    should_save = False
                    if relevance_type == "KEEP_IMMEDIATE":
                        should_save = True
                        print(f"         [KEEP STRONG] {raw_title}")
                    elif relevance_type == "CHECK_DESCRIPTION":
                        if description != "N/A" and any(t in description.lower() for t in TECH_KEYWORDS):
                            should_save = True
                            print(f"         [KEEP VERIFIED] {raw_title}")
                        else:
                            if DEBUG_EVERY_SKIP:
                                dbg("LI_SKIP", title=raw_title, company=raw_company,
                                    reason="ambiguous, no tech keywords")

                    if should_save:
                        # LinkedIn rarely populates a structured compensation
                        # field; annotate_job() pulls the range out of the
                        # description text via best_salary() instead.
                        data = {
                            "title": raw_title,
                            "url": job_url,
                            "company": raw_company,
                            "description": description,
                            "salary": "N/A",
                            "qualifications": "N/A",
                            "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")
                        }
                        annotate_job(data)
                        new_jobs_buffer.append(data)
                        seen_urls.add(canonical_url(job_url))
                        seen_signatures.add(sig)
                        dbg("LI_BUFFERED", title=raw_title, company=raw_company,
                            salary=data["salary"], url=job_url)

                    time.sleep(random.uniform(0.4, 1.2))

                except Exception as e:
                    if DEBUG_EVERY_SKIP:
                        dbg("LI_ERROR_CARD", reason=f"{type(e).__name__}: {str(e)[:120]}")
                    continue

            # Next page click
            print(f"   Finished Page {current_page_num}. Looking for Page {current_page_num + 1}...")

            next_page_num = current_page_num + 1
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, f"button[aria-label='Page {next_page_num}']")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(0.4)
                next_button.click()
                print(f"   >>> Clicked Page {next_page_num}. Loading...")
                current_page_num += 1
                time.sleep(4)
            except NoSuchElementException:
                print("   [INFO] No next page button found. End of results.")
                break
            except Exception as e:
                print(f"   [ERROR] Could not click next page: {e}")
                break


# ==========================================
# VERIFY AND CLEAN EXISTING CSV
# ==========================================
def verify_and_clean_data():
    if not os.path.exists(OUTPUT_FILE):
        print(f"File {OUTPUT_FILE} not found.")
        return

    print(f"\n=== VERIFYING URLS IN {OUTPUT_FILE} ===")
    print("Method: Checking if CSV Job Title exists on the webpage (using Selenium)...")

    df = pd.read_csv(OUTPUT_FILE)
    if "url" not in df.columns or "title" not in df.columns:
        print("Error: CSV must have 'url' and 'title' columns.")
        return

    original_count = len(df)
    valid_mask = []
    
    driver = make_driver()

    try:
        for index, row in df.iterrows():
            url = row.get("url", "")
            raw_title = str(row.get("title", ""))
            csv_title = fix_doubled_title(raw_title).strip()

            if not isinstance(url, str) or not url.startswith("http") or not csv_title:
                valid_mask.append(False)
                continue

            try:
                driver.get(url)
                time.sleep(1.2) # Wait for DOM

                body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                current_url = driver.current_url.lower()
                csv_title_lower = csv_title.lower()

                # 1. Negative Checks
                is_dead = False
                if "simplyhired" in url and "/job/" in url and "/search" in current_url:
                     is_dead = True
                     print(f"   [REMOVING - Redirected] {csv_title}")

                dead_phrases = [
                    "this job posting is temporarily unavailable",
                    "job no longer available",
                    "this job has expired",
                    "no longer accepting applications",
                    "job is no longer available"
                ]
                if any(phrase in body_text for phrase in dead_phrases):
                    is_dead = True
                    print(f"   [REMOVING - Unavailable Msg] {csv_title}")

                if is_dead:
                    valid_mask.append(False)
                    continue

                # 2. Positive Check (Title Match)
                if csv_title_lower in body_text:
                    valid_mask.append(True)
                else:
                    print(f"   [REMOVING - Title Mismatch] CSV Title: '{csv_title}' not found on page.")
                    valid_mask.append(False)

            except Exception as e:
                print(f"   [ERROR verifying] {url}: {e}")
                valid_mask.append(True) # Keep on crash

            if (index + 1) % 10 == 0:
                print(f"   Checked {index + 1}/{original_count}...")

    finally:
        driver.quit()

    df["is_valid"] = valid_mask
    df_clean = df[df["is_valid"] == True].drop(columns=["is_valid"])
    removed_count = original_count - len(df_clean)

    print("\nFinished Checking.")
    print(f"   Original: {original_count}")
    print(f"   Valid:    {len(df_clean)}")
    print(f"   Removed:  {removed_count}")

    if removed_count > 0:
        backup_name = OUTPUT_FILE.replace(".csv", "_backup.csv")
        df.to_csv(backup_name, index=False)
        print(f"   Backup saved to: {backup_name}")
        df_clean.to_csv(OUTPUT_FILE, index=False)
        print(f"   Cleaned file saved to: {OUTPUT_FILE}")
    else:
        print("   No changes needed. File is clean.")


# ==========================================
# MAIN ENTRY POINT
# ==========================================
def run_scraper():
    seen_urls = set()
    seen_signatures = set()

    if os.path.exists(OUTPUT_FILE):
        try:
            old_df = pd.read_csv(OUTPUT_FILE)
            if "url" in old_df.columns:
                # Canonicalise, and drop blanks: an empty url is not an identity,
                # so keeping "" in this set would suppress every url-less posting.
                seen_urls = {
                    canonical_url(u) for u in old_df["url"].dropna().tolist()
                }
                seen_urls.discard("")
            if "title" in old_df.columns and "company" in old_df.columns:
                for _, row in old_df.iterrows():
                    t = str(row.get('title', '')).lower().strip()
                    c = str(row.get('company', '')).lower().strip()
                    if t and c:
                        seen_signatures.add((t, c))
            print(f"Found existing file with {len(seen_urls)} jobs.")
        except Exception as e:
            print(f"Could not read existing file: {e}. Starting fresh.")
    else:
        print("No existing file found. Starting fresh.")

    driver = make_driver()
    new_jobs_buffer = []
    total_saved_this_run = 0

    try:
        # --- 1. SIMPLYHIRED ---
        # SimplyHired scraping runs first in the interactive flow
        if True:
            for kw in KEYWORDS:
                print(f"\n=== SEARCHING (SimplyHired): {kw} ===")
                driver.get(
                    f"https://www.simplyhired.ca/search?q={kw.replace(' ', '+')}&l={LOCATION.replace(' ', '+')}&w={RADIUS}&so=d"
                )
                page_num = 1
                while page_num <= MAX_PAGES_PER_KEYWORD:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "job-list")))
                    cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                    if not cards:
                        cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")
                    print(f"Page {page_num}: Found {len(cards)} cards.")

                    prev_description = ""
                    for i in range(len(cards)):
                        if total_saved_this_run >= MAX_JOBS_TO_SCRAPE:
                            break

                        try:
                            # re-fetch (avoid stale)
                            cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                            if not cards:
                                cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")
                            card = cards[i]

                            # Duplicate URL check at card level
                            try:
                                temp_html = card.get_attribute("outerHTML")
                                temp_soup = BeautifulSoup(temp_html, "lxml")
                                temp_title_tag = (temp_soup.find("a", class_=lambda x: x and "jobTitle" in x) or temp_soup.find("a"))
                                raw_title_dbg = fix_doubled_title(temp_title_tag.get_text(strip=True)) if temp_title_tag else ""
                                raw_href = temp_title_tag.get("href", "") if temp_title_tag else ""
                                check_url = ("https://www.simplyhired.ca" + raw_href.split("?")[0]
                                             if raw_href and not raw_href.startswith("http") else raw_href)

                                temp_company_tag = temp_soup.find("span", attrs={"data-testid": "companyName"})
                                raw_company_dbg = temp_company_tag.get_text(strip=True) if temp_company_tag else ""

                                if canonical_url(check_url) and canonical_url(check_url) in seen_urls:
                                    if DEBUG_EVERY_SKIP:
                                        dbg("SKIP_DUP_URL_CARD", title=raw_title_dbg, company=raw_company_dbg,
                                            url=check_url, reason="already seen")
                                    continue
                            except Exception:
                                pass

                            # Title for relevance checks
                            try:
                                title_elem = card.find_element(By.CSS_SELECTOR, "a[class*='jobTitle']")
                                raw_title = fix_doubled_title(title_elem.text.strip())
                            except Exception:
                                raw_title = fix_doubled_title(norm(card.text.split("\n")[0]))

                            if is_missing(raw_title):
                                if DEBUG_EVERY_SKIP:
                                    dbg("SKIP_SH", reason="empty title on card")
                                continue

                            title_lower = raw_title.lower()

                            if any(bad in title_lower for bad in BAD_KEYWORDS):
                                if DEBUG_EVERY_SKIP:
                                    dbg("SKIP_BAD_KW_SH", title=raw_title, reason="bad keyword in title")
                                continue

                            relevance_type = "SKIP"
                            if any(s in title_lower for s in STRONG_KEYWORDS):
                                relevance_type = "KEEP_IMMEDIATE"
                            elif any(a in title_lower for a in AMBIGUOUS_KEYWORDS):
                                relevance_type = "CHECK_DESCRIPTION"

                            if relevance_type == "SKIP":
                                if DEBUG_EVERY_SKIP:
                                    dbg("SKIP_IRRELEVANT_SH", title=raw_title, reason="no matching keywords")
                                continue

                            job_data = parse_job_data(driver, card, prev_description)
                            if not job_data:
                                continue

                            if canonical_url(job_data["url"]) and canonical_url(job_data["url"]) in seen_urls:
                                if DEBUG_EVERY_SKIP:
                                    dbg("SKIP_DUP_URL_SH", title=job_data["title"], company=job_data["company"], url=job_data["url"])
                                continue

                            sig = (job_data["title"].lower().strip(), job_data["company"].lower().strip())
                            if sig in seen_signatures:
                                if DEBUG_EVERY_SKIP:
                                    dbg("SKIP_DUP_SIG_SH", title=job_data["title"], company=job_data["company"], reason="duplicate title+company")
                                continue

                            prev_description = job_data["description"]

                            should_save = False
                            if relevance_type == "KEEP_IMMEDIATE":
                                should_save = True
                                print(f"   [KEEP STRONG] {raw_title}")
                            elif relevance_type == "CHECK_DESCRIPTION":
                                if job_data["description"] != "N/A":
                                    desc_lower = job_data["description"].lower()
                                    if any(t in desc_lower for t in TECH_KEYWORDS):
                                        should_save = True
                                        print(f"   [KEEP VERIFIED] {raw_title}")
                                    else:
                                        if DEBUG_EVERY_SKIP:
                                            dbg("SKIP_NO_TECH_SH", title=raw_title, reason="ambiguous title, no tech keywords in description")
                                else:
                                    if DEBUG_EVERY_SKIP:
                                        dbg("SKIP_NO_DESC_SH", title=raw_title, reason="ambiguous title, no description")

                            if should_save:
                                new_jobs_buffer.append(job_data)
                                seen_urls.add(canonical_url(job_data["url"]))
                                seen_signatures.add(sig)
                                total_saved_this_run += 1
                                dbg("BUFFERED_SH", title=job_data["title"], company=job_data["company"],
                                    salary=job_data["salary"], url=job_data["url"])
                                # Periodically flush buffered scraped jobs to disk so progress
                                # is preserved if the process is interrupted. This writes raw
                                # scraped rows (no summarization) and clears the buffer.
                                if len(new_jobs_buffer) >= FLUSH_EVERY:
                                    print(f"Flushing {len(new_jobs_buffer)} buffered jobs to {OUTPUT_FILE}...")
                                    # Ensure backup exists for the original file before first write
                                    try:
                                        if os.path.exists(OUTPUT_FILE) and not os.path.exists(OUTPUT_FILE.replace('.csv', '_backup.csv')):
                                            os.replace(OUTPUT_FILE, OUTPUT_FILE.replace('.csv', '_backup.csv'))
                                            print(f"Backed up original {OUTPUT_FILE} to {OUTPUT_FILE.replace('.csv', '_backup.csv')}")
                                    except Exception:
                                        pass
                                    df_flush = pd.DataFrame(new_jobs_buffer)
                                    # Ensure required columns exist
                                    df_flush = df_flush.reindex(columns=OUTPUT_COLUMNS, fill_value="N/A")
                                    header = (not os.path.exists(OUTPUT_FILE)) or (os.path.getsize(OUTPUT_FILE) == 0)
                                    df_flush.to_csv(OUTPUT_FILE, mode="a", header=header, index=False, encoding="utf-8")
                                    print(f"Appended {len(df_flush)} rows to {OUTPUT_FILE} (checkpoint)")
                                    new_jobs_buffer.clear()

                        except Exception as e:
                            if DEBUG_EVERY_SKIP:
                                dbg("ERROR_CARD_SH", reason=f"{type(e).__name__}: {str(e)[:120]}")
                            continue

                    # Advance to the next page ONCE per page (this used to sit
                    # inside the `for` loop above, so it fired -- and could
                    # fail -- once per CARD. When it failed on the last real
                    # page, `break` only exited that inner `for`, never the
                    # `while page_num <= MAX_PAGES_PER_KEYWORD` loop, so the
                    # scraper reprocessed the same stuck page forever instead
                    # of moving to the next keyword.
                    if total_saved_this_run >= MAX_JOBS_TO_SCRAPE:
                        break

                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next page']")
                        driver.execute_script("arguments[0].click();", next_btn)
                        page_num += 1
                        time.sleep(3)
                    except Exception:
                        break

                    # MAX_JOBS_TO_SCRAPE is a run-wide cap. Without this the inner
                    # breaks only ended the current keyword, and the next keyword
                    # started scraping again past the limit.
                    if total_saved_this_run >= MAX_JOBS_TO_SCRAPE:
                        print("   [MAX JOBS LIMIT REACHED] Stopping SimplyHired scan.")
                        break

        # --- 2. PAUSE FOR LOGIN ---
        print("\n" + "=" * 50)
        print(">>> PAUSING FOR LINKEDIN LOGIN <<<")
        print("1. Browser opening LinkedIn.")
        print("2. Log in manually.")
        print("3. After logging in, return to this terminal and press ENTER to continue.")
        print("=" * 50)
        driver.get("https://www.linkedin.com/login")
        input(">>> Press ENTER once logged in and you see your LinkedIn feed...")

        # --- 3. SCRAPE LINKEDIN ---
        scrape_linkedin_authenticated(driver, seen_signatures, seen_urls, new_jobs_buffer)

    finally:
        driver.quit()

        # Final flush of any remaining buffered scraped jobs. Do NOT run the
        # summarizer here — summarization is a separate step and must be run
        # independently using `summarize_existing.py`.
        if new_jobs_buffer:
            print(f"\nScraping complete. Flushing remaining {len(new_jobs_buffer)} NEW jobs to {OUTPUT_FILE}.")
            try:
                if os.path.exists(OUTPUT_FILE) and not os.path.exists(OUTPUT_FILE.replace('.csv', '_backup.csv')):
                    os.replace(OUTPUT_FILE, OUTPUT_FILE.replace('.csv', '_backup.csv'))
                    print(f"Backed up original {OUTPUT_FILE} to {OUTPUT_FILE.replace('.csv', '_backup.csv')}")
            except Exception:
                pass

            df_flush = pd.DataFrame(new_jobs_buffer)
            df_flush = df_flush.reindex(columns=OUTPUT_COLUMNS, fill_value="N/A")
            header = (not os.path.exists(OUTPUT_FILE)) or (os.path.getsize(OUTPUT_FILE) == 0)
            df_flush.to_csv(OUTPUT_FILE, mode="a", header=header, index=False, encoding="utf-8")
            print(f"Appended {len(df_flush)} raw scraped rows to {OUTPUT_FILE} (no summarization).")
            new_jobs_buffer.clear()
        else:
            print("\nScraping complete. No new jobs found.")
def dedupe_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate postings, keeping every genuinely distinct job.

    The previous implementation ran drop_duplicates on the raw url column. Rows
    scraped without a url carry the literal string "N/A", so every url-less
    posting compared equal and all but one were deleted -- silently discarding
    real jobs. Here, blank urls are excluded from url-based matching entirely
    and identity falls back to the company+title signature.
    """
    if df.empty:
        return df

    df = df.copy()
    for col in ("title", "company", "url"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    df["title"] = df["title"].apply(sanitize_title)
    df["_sig"] = [job_signature(t, c) for t, c in zip(df["title"], df["company"])]
    df["_canon_url"] = df["url"].apply(canonical_url)

    # Prefer rows that have a url, then the shortest url, then the newest scrape.
    df["_has_url"] = (df["_canon_url"] != "").astype(int)
    df["_url_len"] = df["_canon_url"].str.len()

    sort_cols = ["_sig", "_has_url", "_url_len"]
    ascending = [True, False, True]
    if "scraped_at" in df.columns:
        sort_cols.append("scraped_at")
        ascending.append(False)
    df = df.sort_values(by=sort_cols, ascending=ascending, kind="mergesort")

    # 1. One row per company+title.
    out = df.drop_duplicates(subset=["_sig"], keep="first")

    # 2. One row per url, but only among rows that actually have a url.
    with_url = out[out["_canon_url"] != ""].drop_duplicates(
        subset=["_canon_url"], keep="first"
    )
    without_url = out[out["_canon_url"] == ""]
    out = pd.concat([with_url, without_url])

    if "scraped_at" in out.columns:
        out = out.sort_values(by="scraped_at", ascending=False, kind="mergesort")

    return out.drop(columns=["_sig", "_canon_url", "_has_url", "_url_len"]).reset_index(
        drop=True
    )


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """(Re)compute salary and profile-fit columns for every row.

    Safe to run on CSVs produced before these columns existed.
    """
    if df.empty:
        return df

    df = df.copy()
    for col in ("salary", "description", "qualifications", "title"):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    records = []
    for _, row in df.iterrows():
        parsed = best_salary(row["salary"], row["description"], row["qualifications"])
        score, matched = score_profile_fit(
            row["title"], row["description"], row["qualifications"]
        )
        records.append(
            {
                # Surface the matched snippet when the salary field was empty,
                # so the CSV shows where the figure came from.
                "salary": parsed.raw if (parsed.found and is_missing(row["salary"]))
                else row["salary"],
                "salary_min_annual": parsed.min_annual if parsed.found else "",
                "salary_max_annual": parsed.max_annual if parsed.found else "",
                "salary_period": parsed.period or "",
                "meets_salary_target": parsed.meets(SALARY_TARGET_CAD),
                "fit_score": score,
                "matched_skills": matched,
            }
        )

    for col, values in pd.DataFrame(records).items():
        df[col] = values.values
    return df


def export_unique_jobs(target: float = SALARY_TARGET_CAD):
    """Produce the final shortlist of unique postings.

    Most employers never publish a salary, so "no salary listed" is treated as
    unknown, not as a failure to meet the target. Those postings stay in the
    shortlist and are ranked by profile fit. Only postings that publish a
    salary AND fall below the target are dropped, and only when
    EXCLUDE_BELOW_TARGET is on.

    Every row carries a salary_status column so the file can be re-sorted or
    re-filtered in a spreadsheet without rerunning anything.
    """
    if not os.path.exists(OUTPUT_FILE):
        print(f"File {OUTPUT_FILE} not found. Run the scraper (option 1) first.")
        return

    print(f"\n=== EXPORTING UNIQUE JOBS (target: ${target:,.0f} CAD/year) ===")
    df = pd.read_csv(OUTPUT_FILE)
    original_count = len(df)

    df = dedupe_jobs(df)
    print(f"   {original_count} rows -> {len(df)} unique postings")

    df = enrich_dataframe(df)

    has_salary = df["salary_max_annual"].astype(str).str.strip() != ""
    meets = df["meets_salary_target"].astype(bool)

    df["salary_status"] = SALARY_STATUS_UNKNOWN
    df.loc[has_salary & meets, "salary_status"] = SALARY_STATUS_MEETS
    df.loc[has_salary & ~meets, "salary_status"] = SALARY_STATUS_BELOW

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    shortlist = df if not EXCLUDE_BELOW_TARGET else df[df["salary_status"] != SALARY_STATUS_BELOW]
    shortlist = shortlist.copy()

    # ml_relevant (from flag_relevant_jobs.py) outranks everything else when
    # present: hands-on modeling/ML postings first, then within each group,
    # confirmed salary matches before unpublished salaries, ranked by fit.
    sort_cols = []
    ascending = []
    drop_cols = []
    if "ml_relevant" in shortlist.columns:
        shortlist["_ml_rank"] = (~shortlist["ml_relevant"].fillna(False).astype(bool)).astype(int)
        sort_cols.append("_ml_rank")
        ascending.append(True)
        drop_cols.append("_ml_rank")

    shortlist["_rank"] = shortlist["salary_status"].map(
        {SALARY_STATUS_MEETS: 0, SALARY_STATUS_UNKNOWN: 1, SALARY_STATUS_BELOW: 2}
    ).fillna(3)
    sort_cols.append("_rank")
    ascending.append(True)
    drop_cols.append("_rank")

    for c in ("fit_score", "salary_max_annual"):
        if c in shortlist.columns:
            sort_cols.append(c)
            ascending.append(False)

    shortlist = shortlist.sort_values(by=sort_cols, ascending=ascending).drop(columns=drop_cols)

    shortlist.to_csv(UNIQUE_JOBS_FILE, index=False, encoding="utf-8")

    counts = df["salary_status"].value_counts()
    n_meets = int(counts.get(SALARY_STATUS_MEETS, 0))
    n_unknown = int(counts.get(SALARY_STATUS_UNKNOWN, 0))
    n_below = int(counts.get(SALARY_STATUS_BELOW, 0))

    print(f"   Salary >= target  : {n_meets:>4}  (in shortlist)")
    print(f"   Salary not posted : {n_unknown:>4}  (in shortlist, ranked by fit)")
    verb = "excluded" if EXCLUDE_BELOW_TARGET else "in shortlist"
    print(f"   Salary below tgt  : {n_below:>4}  ({verb})")
    print(f"\n   {len(shortlist)} jobs -> {UNIQUE_JOBS_FILE}")

    if not shortlist.empty:
        cols = [
            c for c in ("title", "company", "salary", "salary_status", "fit_score")
            if c in shortlist.columns
        ]
        print("\n   [Top of shortlist]")
        print(shortlist[cols].head(10).to_string(index=False))


def remove_csv_duplicates():
    """Clean titles and remove duplicate postings from the main CSV."""
    if not os.path.exists(OUTPUT_FILE):
        print(f"File {OUTPUT_FILE} not found.")
        return

    print("\n=== CLEANING TITLES & REMOVING DUPLICATES ===")
    df = pd.read_csv(OUTPUT_FILE)
    original_count = len(df)

    df_clean = dedupe_jobs(df)
    removed_count = original_count - len(df_clean)
    df_clean.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print("   Done.")
    print(f"   Original rows: {original_count}")
    print(f"   Cleaned rows:  {len(df_clean)}")
    print(f"   Removed:       {removed_count} duplicates.")

    if not df_clean.empty:
        print("\n   [Sample of cleaned titles]:")
        print(df_clean[["title", "company"]].head(5).to_string(index=False))


if __name__ == "__main__":
    print("What would you like to do?")
    print("1. SCRAPE new jobs (SimplyHired + LinkedIn)")
    print("2. VERIFY existing URLs are still live")
    print("3. REMOVE duplicates from the CSV")
    print(f"4. EXPORT unique jobs paying ${SALARY_TARGET_CAD:,} CAD or more")
    print("5. LINKEDIN ONLY (skip SimplyHired)")
    choice = input("Enter 1, 2, 3, 4 or 5: ").strip()

    if choice == "1":
        run_scraper()
        # A scrape is only useful once duplicates are gone and the shortlist
        # is rebuilt, so chain the export instead of making it a separate step.
        export_unique_jobs()
    elif choice == "2":
        verify_and_clean_data()
    elif choice == "3":
        remove_csv_duplicates()
    elif choice == "4":
        export_unique_jobs()
    elif choice == "5":
        # Run only the authenticated LinkedIn scraper (interactive)
        # We call the same run_scraper but skip SimplyHired by asking the user
        # to choose 5 at the main prompt and then pressing ENTER when logged in.
        run_scraper()
        export_unique_jobs()
    else:
        print("Invalid choice. Exiting.")
