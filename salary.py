"""
Salary extraction / normalisation and profile-fit scoring.

The scraper previously stored salary as a raw text blob matched by a very loose
regex, which happily captured any dollar amount in a job description (e.g.
"$500 million in assets under management"). Nothing ever converted that text
into a number, so there was no way to filter on compensation.

This module does three things:

1. Pulls compensation figures out of free text, rejecting dollar amounts that
   are plainly not pay.
2. Normalises hourly / daily / weekly / monthly pay to an annual CAD figure so
   postings can be compared and filtered on one scale.
3. Scores a posting against a candidate skill profile so results can be ranked
   by how well they actually fit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from typing import Optional

# ---------------------------------------------------------------------------
# Period normalisation
# ---------------------------------------------------------------------------

# Hours/year assuming a 40h week; the conventional full-time figure.
HOURS_PER_YEAR = 2080
WEEKS_PER_YEAR = 52
MONTHS_PER_YEAR = 12
WORKDAYS_PER_YEAR = 260

PERIOD_MULTIPLIERS = {
    "hour": HOURS_PER_YEAR,
    "day": WORKDAYS_PER_YEAR,
    "week": WEEKS_PER_YEAR,
    "month": MONTHS_PER_YEAR,
    "year": 1,
}

# Plausible pay bounds per period, used to throw out non-compensation numbers.
PERIOD_BOUNDS = {
    "hour": (15.0, 500.0),
    "day": (100.0, 5_000.0),
    "week": (500.0, 25_000.0),
    "month": (2_000.0, 120_000.0),
    "year": (20_000.0, 1_500_000.0),
}

_PERIOD_WORDS = [
    (r"\b(?:per\s+hour|an?\s+hour|hourly|/\s*h(?:r|our)?\b|p\.?h\.?)", "hour"),
    (r"\b(?:per\s+day|a\s+day|daily|/\s*day\b|per\s+diem)", "day"),
    (r"\b(?:per\s+week|a\s+week|weekly|/\s*w(?:k|eek)?\b)", "week"),
    (r"\b(?:per\s+month|a\s+month|monthly|/\s*mo(?:nth)?\b)", "month"),
    (
        r"\b(?:per\s+year|per\s+annum|a\s+year|annually|annual|yearly"
        r"|/\s*y(?:r|ear)?\b|p\.?a\.?)",
        "year",
    ),
]
PERIOD_RES = [(re.compile(p, re.IGNORECASE), name) for p, name in _PERIOD_WORDS]

# Words that mark a nearby dollar figure as compensation even when the posting
# omits an explicit period ("Salary: $135,000").
COMP_CUE_RE = re.compile(
    r"\b(?:salary|salaries|compensation|base\s+pay|base\s+salary|pay\s+range"
    r"|pay\s+scale|total\s+comp(?:ensation)?|remuneration|wage|wages"
    r"|target\s+earnings|on-target\s+earnings|ote|hiring\s+range"
    r"|salary\s+range|pay\s+rate|rate\s+of\s+pay)\b",
    re.IGNORECASE,
)

# Scale words that disqualify a figure (revenue, AUM, funding, etc.).
SCALE_RE = re.compile(r"^\s*(?:million|billion|trillion|m\b|bn\b|b\b)", re.IGNORECASE)

# Context that means the dollar figure is a perk/bonus, not base pay.
NON_PAY_CONTEXT_RE = re.compile(
    r"\b(?:revenue|assets?\s+under\s+management|aum|funding|raised|valuation"
    r"|portfolio|market\s+cap|in\s+sales|budget|spend(?:ing)?\s+account"
    r"|wellness|reimburse\w*|tuition|allowance|referral\s+bonus"
    r"|signing\s+bonus|discount|donation|scholarship)\b",
    re.IGNORECASE,
)

_MONEY = r"(?:CA\$|C\$|US\$|\$)\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d{1,2})?)\s*([kK])?"

# A figure, optionally followed by a separator and a second figure (a range).
RANGE_RE = re.compile(
    _MONEY + r"(?:\s*(?:-|–|—|\bto\b)\s*" + _MONEY + r")?",
    re.IGNORECASE,
)

# How far to look around a match for a period word / compensation cue.
TAIL_WINDOW = 40
HEAD_WINDOW = 80


@dataclass
class Salary:
    """A parsed compensation figure, normalised to annual CAD."""

    min_annual: Optional[float] = None
    max_annual: Optional[float] = None
    period: Optional[str] = None
    raw: str = ""

    @property
    def found(self) -> bool:
        return self.min_annual is not None

    def meets(self, target_annual: float) -> bool:
        """True when the top of the advertised range reaches the target.

        The top of the range is used deliberately: a posting advertised at
        "$110,000 - $135,000" is still worth applying to when the target is
        $118,000, because the upper band is negotiable.
        """
        if self.max_annual is None:
            return False
        return self.max_annual >= target_annual

    def as_dict(self) -> dict:
        return asdict(self)


def _to_number(digits: str, k_suffix: Optional[str]) -> Optional[float]:
    try:
        value = float(digits.replace(",", ""))
    except (TypeError, ValueError):
        return None
    if k_suffix:
        value *= 1000
    return value


_SENTENCE_BREAK = re.compile(r"[.;!?|•]")


def _head_scope(text: str) -> str:
    """The text preceding a figure, back to the start of its sentence.

    Context from an earlier sentence must not disqualify a figure: a posting
    that mentions a signing bonus and then states the salary band in the next
    sentence was previously rejected outright.
    """
    breaks = [m.end() for m in _SENTENCE_BREAK.finditer(text)]
    return text[breaks[-1]:] if breaks else text


def _tail_scope(text: str) -> str:
    """The text following a figure, up to the end of its sentence."""
    match = _SENTENCE_BREAK.search(text)
    return text[: match.start()] if match else text


def _detect_period(tail: str, head: str) -> Optional[str]:
    """Find the pay period stated after (preferred) or before a figure."""
    for regex, name in PERIOD_RES:
        if regex.search(tail):
            return name
    for regex, name in PERIOD_RES:
        if regex.search(head):
            return name
    return None


def _infer_period(value: float) -> Optional[str]:
    """Guess a period for a figure that has a salary cue but no period word."""
    if 15 <= value <= 500:
        return "hour"
    if 20_000 <= value <= 1_500_000:
        return "year"
    return None


def _plausible(value: float, period: str) -> bool:
    low, high = PERIOD_BOUNDS[period]
    return low <= value <= high


def parse_salary(text: str) -> Salary:
    """Extract the most plausible compensation figure from free text.

    Returns an empty Salary when nothing convincing is present. Being wrong in
    the "found nothing" direction is much cheaper than inventing a salary,
    because an invented figure silently corrupts the filtered output.
    """
    if not text:
        return Salary()

    flat = " ".join(str(text).split())
    if not flat:
        return Salary()

    best: Optional[Salary] = None

    for match in RANGE_RE.finditer(flat):
        low_raw, low_k, high_raw, high_k = match.groups()
        raw_tail = flat[match.end(): match.end() + TAIL_WINDOW]
        tail = _tail_scope(raw_tail)
        head = _head_scope(flat[max(0, match.start() - HEAD_WINDOW): match.start()])

        # "$500 million in AUM" is not a salary.
        if SCALE_RE.match(raw_tail):
            continue
        if NON_PAY_CONTEXT_RE.search(head) or NON_PAY_CONTEXT_RE.search(tail):
            continue

        low = _to_number(low_raw, low_k)
        high = _to_number(high_raw, high_k) if high_raw else None
        if low is None:
            continue

        period = _detect_period(tail, head)
        if period is None:
            # No explicit period: only trust the figure if a compensation cue
            # sits nearby, then infer the period from the magnitude.
            if not COMP_CUE_RE.search(head) and not COMP_CUE_RE.search(tail):
                continue
            period = _infer_period(high if high is not None else low)
            if period is None:
                continue

        if not _plausible(low, period):
            continue
        if high is not None and not _plausible(high, period):
            high = None

        # A range written as "$120,000 - $150" is a parse artefact, not a range.
        if high is not None and high < low:
            high = None

        multiplier = PERIOD_MULTIPLIERS[period]
        candidate = Salary(
            min_annual=round(low * multiplier, 2),
            max_annual=round((high if high is not None else low) * multiplier, 2),
            period=period,
            raw=match.group(0).strip(),
        )

        # Prefer the highest credible figure; postings often mention a small
        # bonus before the actual band.
        if best is None or (candidate.max_annual or 0) > (best.max_annual or 0):
            best = candidate

    return best or Salary()


def best_salary(*texts: str) -> Salary:
    """Parse several fields (salary box, description, qualifications) and keep
    the strongest hit. LinkedIn never populates a salary field, so the
    description is usually the only place the number appears."""
    best = Salary()
    for text in texts:
        found = parse_salary(text)
        if found.found and (found.max_annual or 0) > (best.max_annual or 0):
            best = found
    return best


# ---------------------------------------------------------------------------
# Profile fit
# ---------------------------------------------------------------------------

# Skills weighted by how central they are to the target profile: a senior
# data/analytics practitioner working in Python, SQL, Dataiku, ML and stats,
# with Tableau and Power BI on the BI side.
PROFILE_SKILLS = {
    "python": (r"\bpython\b", 3),
    "sql": (r"\bsql\b|\bt-sql\b|\bpl/sql\b|\bbigquery\b|\bsnowflake\b", 3),
    "statistics": (
        r"\bstatistic(?:s|al)\b|\bregression\b|\bhypothesis test\w*\b"
        r"|\bexperimental design\b|\ba/b test\w*\b|\binference\b|\beconometric\w*\b",
        3,
    ),
    "machine learning": (
        r"\bmachine learning\b|\bml\b|\bpredictive model\w*\b|\bscikit-?learn\b"
        r"|\bxgboost\b|\bdeep learning\b",
        3,
    ),
    "dataiku": (r"\bdataiku\b", 4),
    "tableau": (r"\btableau\b", 2),
    "power bi": (r"\bpower\s*bi\b|\bpowerbi\b", 2),
    "git": (r"\bgit\b|\bgithub\b|\bgitlab\b|\bversion control\b", 1),
    "cloud": (r"\baws\b|\bazure\b|\bgcp\b|\bgoogle cloud\b|\bdatabricks\b", 1),
    "banking": (
        r"\bbank\w*\b|\bfinancial services\b|\bcredit risk\b|\bfintech\b"
        r"|\bcapital markets\b|\binsurance\b",
        2,
    ),
    "leadership": (
        r"\blead\w*\b|\bmentor\w*\b|\bmanage\w*\b|\bstakeholder\w*\b|\bsenior\b",
        2,
    ),
}

PROFILE_SKILL_RES = {
    name: (re.compile(pattern, re.IGNORECASE), weight)
    for name, (pattern, weight) in PROFILE_SKILLS.items()
}

MAX_FIT_SCORE = sum(weight for _, weight in PROFILE_SKILLS.values())


def score_profile_fit(*texts: str) -> tuple[int, str]:
    """Score a posting against the candidate profile.

    Returns the raw weighted score and a semicolon-joined list of the skills
    that matched, so a row can be explained rather than just ranked.
    """
    blob = " ".join(str(t) for t in texts if t and str(t) != "N/A").lower()
    if not blob:
        return 0, ""

    matched = []
    score = 0
    for name, (regex, weight) in PROFILE_SKILL_RES.items():
        if regex.search(blob):
            matched.append(name)
            score += weight
    return score, "; ".join(sorted(matched))
