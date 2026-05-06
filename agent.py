"""
Job Fit Scoring Agent

Scrapes job postings with Jina.ai, scores them with Gemini, stores results in
Supabase, and sends ntfy.sh notifications.
"""

import json
import os
import re
import tempfile
import time
from datetime import datetime, timedelta
from html import unescape
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import google.generativeai as genai
import requests


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC") or "team7-jobagent-2026"


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _nonnegative_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


MAX_BATCH_JOBS = _positive_int_env("MAX_BATCH_JOBS", 3)
HTTP_TIMEOUT_SECONDS = _positive_int_env("HTTP_TIMEOUT_SECONDS", 10)
LINKEDIN_TIMEOUT_SECONDS = _positive_int_env("LINKEDIN_TIMEOUT_SECONDS", 8)
BATCH_JOB_DELAY_SECONDS = _nonnegative_float_env("BATCH_JOB_DELAY_SECONDS", 0)
GEMINI_RETRY_ATTEMPTS = _positive_int_env("GEMINI_RETRY_ATTEMPTS", 1)
GEMINI_RATE_LIMIT_WAIT_SECONDS = _positive_int_env("GEMINI_RATE_LIMIT_WAIT_SECONDS", 2)
ALLOW_FALLBACK_SCORING = os.environ.get("ALLOW_FALLBACK_SCORING", "true").lower() != "false"


if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not set. Load your .env or set it in PowerShell first.")

genai.configure(api_key=GEMINI_API_KEY)

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
MODEL_FALLBACKS = [
    name
    for name in [
        GEMINI_MODEL,
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-1.5-flash-latest",
        "gemini-1.5-flash",
    ]
    if name
]
_active_model_name = None
_gemini_unavailable_error = None


def _is_model_availability_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "is not found" in message
        or "not supported for generatecontent" in message
        or "not found for api version" in message
    )


def _is_gemini_configuration_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "api key not found" in message
        or "api_key_invalid" in message
        or "api key invalid" in message
        or "api has not been used" in message
        or "service_disabled" in message
        or "permission_denied" in message
    )


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if this is a 429 quota/rate-limit error."""
    msg = str(exc).lower()
    return "429" in msg or "quota" in msg or "rate" in msg or "resource_exhausted" in msg


def generate_with_gemini(contents):
    """Generate content using the first available Flash model, with rate-limit retry."""
    global _active_model_name, _gemini_unavailable_error

    if _gemini_unavailable_error:
        raise RuntimeError(_gemini_unavailable_error)

    candidates = [_active_model_name] if _active_model_name else []
    candidates.extend(name for name in MODEL_FALLBACKS if name not in candidates)

    last_error = None
    for model_name in candidates:
        for attempt in range(GEMINI_RETRY_ATTEMPTS):
            try:
                response = genai.GenerativeModel(model_name).generate_content(contents)
                _active_model_name = model_name
                return response
            except Exception as exc:
                last_error = exc
                if _is_gemini_configuration_error(exc):
                    _gemini_unavailable_error = (
                        "Gemini is not available for the configured API key. "
                        "Check GEMINI_API_KEY, enable the Gemini API, or create a new key."
                    )
                    raise RuntimeError(_gemini_unavailable_error) from exc
                if _is_rate_limit_error(exc):
                    wait = GEMINI_RATE_LIMIT_WAIT_SECONDS * (attempt + 1)
                    print(
                        f"[rate limit] 429 hit on {model_name}, waiting {wait}s before retry "
                        f"{attempt + 1}/{GEMINI_RETRY_ATTEMPTS}..."
                    )
                    time.sleep(wait)
                    continue
                elif _is_model_availability_error(exc):
                    break  # try next model
                else:
                    raise
        else:
            continue  # all retries exhausted for this model, try next

    _gemini_unavailable_error = (
        "No configured Gemini Flash model is available for generateContent. "
        "Set GEMINI_MODEL in .env to a model listed for your API key."
    )
    raise RuntimeError(_gemini_unavailable_error) from last_error


CANDIDATE_PROFILE = """
Name: Nishanth
Degree: MBA / MS, Kelley School of Business, Indiana University (Spring 2026)
Visa Status: F-1 student, will need OPT/H1B sponsorship. This is critical.
Target roles: Data Analyst, Business Analyst, Product Analyst, AI/ML Strategy
Skills: Python, SQL, Tableau, Excel, Gemini API, basic ML, business strategy
Years of experience: 2 years pre-MBA
Preferred locations: NYC, Chicago, SF, Seattle, Austin, Remote
Salary floor: $85,000
Preferred company stage: Mid-size to large (Series B+, public companies)
Must-haves: H1B sponsorship history OR known OPT-friendly, strong data culture
Deal-breakers: Requires US citizen/green card only, pure sales roles
"""

VERDICT_ORDER = ["Apply Tonight", "Apply This Weekend", "Low Priority", "Skip"]


HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}


LINKEDIN_HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def db_insert(table: str, data: dict) -> dict:
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=data)
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else {}


def db_select(table: str, filters: str = "") -> list:
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{filters}", headers=HEADERS)
    r.raise_for_status()
    return r.json()


def db_update(table: str, row_id: int, data: dict):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{table}?id=eq.{row_id}",
        headers=HEADERS,
        json=data,
    )
    r.raise_for_status()


def get_resume() -> dict | None:
    rows = db_select("resume", "id=eq.1&select=*")
    return rows[0] if rows else None


def save_resume(parsed_text: str, filename: str = "resume.pdf") -> dict:
    now = datetime.utcnow().isoformat()
    data = {
        "id": 1,
        "filename": filename,
        "parsed_text": parsed_text,
        "uploaded_at": now,
        "updated_at": now,
    }

    existing = get_resume()
    if existing:
        db_update("resume", 1, data)
    else:
        db_insert("resume", data)

    return get_resume() or data


def get_candidate_profile() -> str:
    """Prefer the uploaded resume; fall back to the built-in profile."""
    try:
        resume = get_resume()
        if resume and resume.get("parsed_text"):
            return resume["parsed_text"]
    except requests.HTTPError:
        # The resume table may not exist yet. Scoring should still work.
        pass
    return CANDIDATE_PROFILE


def _wait_for_uploaded_file(uploaded_file):
    state_name = getattr(getattr(uploaded_file, "state", None), "name", "")
    while state_name == "PROCESSING":
        time.sleep(1)
        uploaded_file = genai.get_file(uploaded_file.name)
        state_name = getattr(getattr(uploaded_file, "state", None), "name", "")

    if state_name == "FAILED":
        raise RuntimeError("Gemini could not process the uploaded resume PDF.")

    return uploaded_file


def parse_resume_pdf(pdf_bytes: bytes, filename: str = "resume.pdf") -> str:
    """Ask Gemini to turn the uploaded PDF into a structured profile string."""
    safe_filename = unquote(filename or "resume.pdf").replace("\\", "_").replace("/", "_")
    uploaded_file = None
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(pdf_bytes)
            temp_path = tmp.name

        uploaded_file = genai.upload_file(
            temp_path,
            mime_type="application/pdf",
            display_name=safe_filename,
        )
        uploaded_file = _wait_for_uploaded_file(uploaded_file)

        prompt = """
Extract the candidate's resume into a concise, structured plain-text profile.
Do not invent missing facts. Preserve specific skills, tools, degrees,
companies, roles, dates, projects, certifications, and visa/work authorization
details if present. Include these sections when available:

Name
Education
Work experience
Projects
Skills
Certifications
Work authorization / constraints
Target role signals

Return only the parsed profile text, not JSON.
"""
        response = generate_with_gemini([prompt, uploaded_file])
        parsed_text = response.text.strip()
        if not parsed_text:
            raise RuntimeError("Gemini returned an empty resume parse.")
        return parsed_text
    finally:
        if uploaded_file is not None:
            try:
                genai.delete_file(uploaded_file.name)
            except Exception:
                pass
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass


def scrape_url(url: str, max_chars: int = 6000) -> str:
    """Use Jina.ai reader. Free, no API key needed."""
    r = requests.get(
        f"https://r.jina.ai/{url}",
        headers={"Accept": "text/plain", "X-No-Cache": "true"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    r.raise_for_status()
    return r.text[:max_chars]


def linkedin_job_id(url: str) -> str | None:
    parsed = urlparse(url)
    match = re.search(r"(\d{6,})", parsed.path)
    return match.group(1) if match else None


def scrape_linkedin_guest_job(url: str, max_chars: int = 6000) -> str:
    job_id = linkedin_job_id(url)
    if not job_id:
        raise ValueError("LinkedIn job URL does not contain a job id.")

    r = requests.get(
        f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}",
        headers=LINKEDIN_HEADERS,
        timeout=LINKEDIN_TIMEOUT_SECONDS,
    )
    r.raise_for_status()
    return r.text[:max_chars]


def scrape_job(url: str) -> str:
    if "linkedin.com/jobs/view/" in url.lower():
        try:
            return scrape_linkedin_guest_job(url, max_chars=6000)
        except Exception as exc:
            status = _http_status(exc)
            reason = f"HTTP {status}" if status else str(exc)
            print(f"[warning] LinkedIn guest job scrape failed ({reason}); trying Jina Reader fallback.")
    return scrape_url(url, max_chars=6000)


def _http_status(exc: Exception) -> int | None:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code
    return None


def scrape_linkedin_search(search_url: str) -> str:
    text_parts = []

    parsed = urlparse(search_url)
    query = parse_qs(parsed.query)
    keywords = (query.get("keywords") or [""])[0]
    location = (query.get("location") or [""])[0]

    if keywords or location:
        for guest_url in build_linkedin_guest_search_urls(search_url, pages=2):
            try:
                r = requests.get(
                    guest_url,
                    headers=LINKEDIN_HEADERS,
                    timeout=LINKEDIN_TIMEOUT_SECONDS,
                )
                r.raise_for_status()
                text_parts.append(r.text[:80000])
            except Exception:
                continue

    if not text_parts:
        try:
            text_parts.append(scrape_url(search_url, max_chars=80000))
        except Exception as exc:
            status = _http_status(exc)
            reason = f"HTTP {status}" if status else str(exc)
            print(f"[warning] Jina Reader failed for LinkedIn search ({reason}).")

    return "\n".join(text_parts)


def build_linkedin_search_url(keywords: str, location: str) -> str:
    params = {
        "keywords": keywords.strip(),
        "location": location.strip(),
        "f_TPR": "r86400",
    }
    return f"https://www.linkedin.com/jobs/search/?{urlencode(params)}"


def build_linkedin_guest_search_urls(search_url: str, pages: int = 3) -> list[str]:
    parsed = urlparse(search_url)
    query = parse_qs(parsed.query)
    params = {
        "keywords": (query.get("keywords") or [""])[0],
        "location": (query.get("location") or [""])[0],
        "f_TPR": (query.get("f_TPR") or ["r86400"])[0],
    }
    return [
        "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"
        + urlencode({**params, "start": start})
        for start in range(0, pages * 25, 25)
    ]


def _clean_linkedin_job_url(url: str) -> str | None:
    url = unquote(url).strip().rstrip(".,)]}'\"")
    parsed = urlparse(url)
    if not parsed.scheme:
        parsed = urlparse(f"https://{url}")
    if "linkedin.com" not in parsed.netloc.lower():
        return None
    if "/jobs/view/" not in parsed.path:
        return None

    id_match = re.search(r"(\d{6,})", parsed.path)
    if not id_match:
        return None
    return urlunparse(("https", "www.linkedin.com", f"/jobs/view/{id_match.group(1)}", "", "", ""))


def extract_linkedin_job_urls(text: str) -> list[str]:
    """Extract individual LinkedIn job URLs or job IDs from Jina search text."""
    urls: list[str] = []
    seen: set[str] = set()

    def add_url(url: str | None):
        if not url or not re.search(r"\d{8,}", url) or url in seen:
            return
        seen.add(url)
        urls.append(url)

    url_pattern = re.compile(
        r"https?://(?:www\.)?linkedin\.com/jobs/view/[^\s\]\)\"'<>]+",
        re.IGNORECASE,
    )
    for match in url_pattern.findall(text):
        cleaned = _clean_linkedin_job_url(match)
        add_url(cleaned)

    id_patterns = [
        r"data-entity-urn=[\"']urn:li:jobPosting:(\d+)",
        r"jobPosting/(\d+)",
        r"(?:currentJobId|jobId|jobs/view)[=/](\d{6,})",
        r"linkedin\.com/jobs/view/[^/\s\]\)\"'<>]*?(\d{6,})",
        r"jobPosting[:/](\d{6,})",
        r"jobPostingId[\"'\s:=]+(\d{6,})",
        r"data-entity-urn=[\"']urn:li:jobPosting:(\d{6,})",
        r"/jobs-guest/jobs/api/jobPosting/(\d{6,})",
    ]
    for pattern in id_patterns:
        for job_id in re.findall(pattern, text, flags=re.IGNORECASE):
            add_url(f"https://www.linkedin.com/jobs/view/{job_id}")

    return urls


def _parse_gemini_json(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        if raw.lstrip().startswith("json"):
            raw = raw.lstrip()[4:]

    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1:
        raw = raw[start : end + 1]

    return json.loads(raw.strip())


def _plain_text_from_html(text: str) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _first_match(patterns: list[str], text: str, default: str) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = _plain_text_from_html(match.group(1)).strip(" -|")
            if value:
                return value
    return default


def _clean_fallback_title(title: str, company: str) -> str:
    title = unescape(title or "").strip()
    title = re.sub(r"\s+", " ", title)
    title = re.sub(r"\s*\|\s*LinkedIn\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*-\s*LinkedIn\s*$", "", title, flags=re.IGNORECASE)
    if " hiring " in title.lower():
        title = re.split(r"\s+hiring\s+", title, flags=re.IGNORECASE, maxsplit=1)[-1]
    if company and title.lower().endswith(f" at {company}".lower()):
        title = title[: -len(f" at {company}")].strip()
    title = re.split(r"\s+in\s+[A-Z][A-Za-z .'-]+,\s*[A-Z]{2}\b", title, flags=re.IGNORECASE, maxsplit=1)[0]
    return title or "Unknown role"


def _fallback_summary(
    role: str,
    company: str,
    location: str,
    overall_score: int,
    analyst_match: bool,
    internship: bool,
    visa_positive: bool,
    visa_negative: bool,
) -> str:
    fit_bits = []
    if analyst_match:
        fit_bits.append("the posting shows useful data/technical keywords")
    if internship:
        fit_bits.append("the seniority looks internship-friendly")
    if not fit_bits:
        fit_bits.append("the role details are limited, so this is a cautious match")

    if visa_positive:
        visa_note = "Visa signals look positive from the posting."
    elif visa_negative:
        visa_note = "Visa sponsorship looks risky based on the authorization language."
    else:
        visa_note = "Visa sponsorship is unclear, so verify before investing too much time."

    company_text = company if company != "Unknown company" else "the company"
    location_text = f" in {location}" if location != "Unknown location" else ""
    return (
        f"{role} at {company_text}{location_text} looks like a {overall_score}/100 fit because "
        f"{', and '.join(fit_bits)}. {visa_note}"
    )


def fallback_score_job(job_text: str, url: str, reason: str) -> dict:
    plain_text = _plain_text_from_html(job_text) if job_text else ""
    company = _first_match(
        [
            r'"companyName"\s*:\s*"([^"]+)"',
            r'"name"\s*:\s*"([^"]+)"\s*,\s*"sameAs"',
            r"companyName[^>]*>\s*([^<]+)",
            r"topcard__org-name-link[^>]*>\s*([^<]+)",
            r"topcard__flavor[^>]*>\s*([^<]+)",
        ],
        job_text,
        "Unknown company",
    )
    location = _first_match(
        [
            r'"jobLocation"\s*:\s*"([^"]+)"',
            r"topcard__flavor--bullet[^>]*>\s*([^<]+)",
            r"formattedLocation[^>]*>\s*([^<]+)",
        ],
        job_text,
        "Unknown location",
    )
    title = _clean_fallback_title(
        _first_match(
            [
                r"<title[^>]*>(.*?)</title>",
                r'"title"\s*:\s*"([^"]+)"',
                r"jobTitle[^>]*>\s*([^<]+)",
                r"topcard__title[^>]*>\s*([^<]+)",
                r"show\-more\-less\-html__markup[^>]*>\s*([^<]{6,80})",
            ],
            job_text,
            "Unknown role",
        ),
        company,
    )

    lower = plain_text.lower()
    visa_positive = any(term in lower for term in ["h-1b", "h1b", "visa sponsorship", "will sponsor", "opt"])
    visa_negative = any(
        term in lower
        for term in ["no visa sponsorship", "without sponsorship", "must be authorized", "u.s. citizen", "us citizen"]
    )
    analyst_match = any(term in lower for term in ["sql", "python", "tableau", "analytics", "data", "machine learning"])
    internship = "intern" in lower or "internship" in lower

    skill_match = 65 if analyst_match else 45
    visa_friendliness = 75 if visa_positive else 20 if visa_negative else 45
    seniority_fit = 70 if internship else 55
    company_quality = 55
    overall_score = round((skill_match + visa_friendliness + seniority_fit + company_quality) / 4)
    verdict = "Apply This Weekend" if overall_score >= 70 else "Low Priority" if overall_score >= 45 else "Skip"
    summary = _fallback_summary(
        title,
        company,
        location,
        overall_score,
        analyst_match,
        internship,
        visa_positive,
        visa_negative,
    )

    return {
        "company": company,
        "role": title,
        "location": location,
        "overall_score": overall_score,
        "skill_match": skill_match,
        "visa_friendliness": visa_friendliness,
        "seniority_fit": seniority_fit,
        "company_quality": company_quality,
        "verdict": verdict,
        "summary": summary,
        "red_flags": [
            "AI scoring was unavailable, so this was scored with the local fallback rules.",
            "Visa sponsorship should be verified manually." if not visa_positive else "Confirm visa sponsorship details.",
        ],
        "green_flags": ["Job was captured and queued despite the scoring service issue."],
        "salary_range": "Unknown",
        "sponsors_visa": "Yes" if visa_positive else "No" if visa_negative else "Unknown",
        "follow_up_days": 14,
        "url": url,
        "scored_at": datetime.utcnow().isoformat(),
        "scoring_source": "fallback",
        "scoring_note": reason[:300],
    }


def score_job(job_text: str, url: str) -> dict:
    candidate_profile = get_candidate_profile()
    prompt = f"""
You are a brutally honest career coach helping an international student on F-1 visa prioritize job applications.

CANDIDATE PROFILE:
{candidate_profile}

JOB POSTING:
{job_text}

Analyze this job posting and return ONLY a JSON object with these exact keys:
{{
  "company": "company name",
  "role": "exact job title",
  "location": "city, state or Remote",
  "overall_score": <integer 0-100>,
  "skill_match": <integer 0-100>,
  "visa_friendliness": <integer 0-100>,
  "seniority_fit": <integer 0-100>,
  "company_quality": <integer 0-100>,
  "verdict": "Apply Tonight" | "Apply This Weekend" | "Low Priority" | "Skip",
  "summary": "<2-3 sentences: honest assessment of fit, call out visa risk explicitly if any>",
  "red_flags": ["list", "of", "concerns"],
  "green_flags": ["list", "of", "strengths"],
  "salary_range": "extracted range or Unknown",
  "sponsors_visa": "Yes" | "No" | "Unknown",
  "follow_up_days": <7 | 14 | 21>
}}

Be harsh. If visa sponsorship is unclear or the company has no H1B history, flag it. Score honestly.
Return ONLY the JSON, no other text.
"""
    last_error = None
    for attempt in range(2):
        try:
            response = generate_with_gemini(prompt)
        except Exception as exc:
            if ALLOW_FALLBACK_SCORING:
                return fallback_score_job(job_text, url, str(exc))
            raise
        try:
            result = _parse_gemini_json(response.text)
            break
        except json.JSONDecodeError as exc:
            last_error = exc
            print(f"[warning] Gemini returned invalid JSON for {url}; retry {attempt + 1}/2.")
            prompt += "\n\nReturn valid JSON only. Do not include markdown, comments, or prose."
    else:
        if ALLOW_FALLBACK_SCORING:
            return fallback_score_job(job_text, url, f"Gemini returned invalid JSON after retry: {last_error}")
        raise RuntimeError(f"Gemini returned invalid JSON after retry: {last_error}")

    result["url"] = url
    result["scored_at"] = datetime.utcnow().isoformat()
    return result


def _score_payload_for_db(score: dict) -> dict:
    return {
        "url": score["url"],
        "company": score.get("company"),
        "role": score.get("role"),
        "location": score.get("location"),
        "overall_score": score.get("overall_score"),
        "skill_match": score.get("skill_match"),
        "visa_friendliness": score.get("visa_friendliness"),
        "seniority_fit": score.get("seniority_fit"),
        "company_quality": score.get("company_quality"),
        "verdict": score.get("verdict"),
        "summary": score.get("summary"),
        "red_flags": json.dumps(score.get("red_flags") or []),
        "green_flags": json.dumps(score.get("green_flags") or []),
        "salary_range": score.get("salary_range") or "Unknown",
        "sponsors_visa": score.get("sponsors_visa") or "Unknown",
        "status": "Scored",
        "scored_at": score.get("scored_at") or datetime.utcnow().isoformat(),
        "followup_days": score.get("follow_up_days") or score.get("followup_days") or 7,
    }


def save_score(score: dict) -> dict:
    row = db_insert("jobs", _score_payload_for_db(score))
    return {**score, "id": row.get("id")}


NTFY_PRIORITY_VALUES = {
    "min": 1,
    "low": 2,
    "default": 3,
    "high": 4,
    "urgent": 5,
    "max": 5,
}


def ntfy_priority_value(priority: str | int) -> int:
    if isinstance(priority, int):
        return max(1, min(5, priority))
    return NTFY_PRIORITY_VALUES.get(str(priority).lower(), 3)


def publish_ntfy(title: str, message: str, priority: str | int = "default", tags=None, click: str | None = None):
    payload = {
        "topic": NTFY_TOPIC,
        "title": title,
        "message": message,
        "priority": ntfy_priority_value(priority),
        "tags": tags or [],
    }
    if click:
        payload["click"] = click
    headers = {"Click": click} if click else None

    response = requests.post(
        "https://ntfy.sh",
        json=payload,
        headers=headers,
        timeout=15,
    )
    if not response.ok:
        raise requests.HTTPError(
            f"{response.status_code} ntfy error: {response.text}",
            response=response,
        )


def send_notification(score: dict):
    priority_map = {
        "Apply Tonight": "urgent",
        "Apply This Weekend": "high",
        "Low Priority": "default",
        "Skip": "low",
    }
    emoji_map = {
        "Apply Tonight": "\U0001f525",
        "Apply This Weekend": "\u2705",
        "Low Priority": "\U0001f7e1",
        "Skip": "\u274c",
    }

    verdict = score.get("verdict", "Low Priority")
    company = score.get("company") or "Unknown company"
    role = score.get("role") or "Unknown role"
    overall_score = score.get("overall_score", 0)
    sponsors_visa = score.get("sponsors_visa") or "Unknown"
    summary = score.get("summary") or ""
    score_label = "Estimated score" if score.get("scoring_source") == "fallback" else "Score"

    body = (
        f"{role} | {score_label}: {overall_score}/100 | Visa: {sponsors_visa}\n"
        f"{summary[:350]}"
    )

    publish_ntfy(
        title=f"{emoji_map.get(verdict, '')} {verdict} \u2014 {company}",
        message=body,
        priority=priority_map.get(verdict, "default"),
        tags=["briefcase"],
        click=score.get("url"),
    )


def send_failure_notification(error: dict):
    url = error.get("url")
    message = error.get("error") or "Unknown error"
    publish_ntfy(
        title="Job scoring failed",
        message=f"{url or 'Unknown job URL'}\n{message[:500]}",
        priority="default",
        tags=["warning"],
        click=url,
    )


def summarize_verdicts(scores: list[dict]) -> dict:
    summary = {verdict: 0 for verdict in VERDICT_ORDER}
    for score in scores:
        verdict = score.get("verdict")
        if verdict in summary:
            summary[verdict] += 1
    return summary


def group_results_by_verdict(scores: list[dict]) -> dict:
    grouped = {verdict: [] for verdict in VERDICT_ORDER}
    for score in scores:
        verdict = score.get("verdict")
        if verdict not in grouped:
            verdict = "Low Priority"
        grouped[verdict].append(score)
    return grouped


def send_batch_notification(summary: dict, scored_count: int, failed_count: int = 0):
    body = (
        f"\U0001f525 {summary.get('Apply Tonight', 0)} Apply Tonight\n"
        f"\u2705 {summary.get('Apply This Weekend', 0)} Apply This Weekend\n"
        f"\U0001f7e1 {summary.get('Low Priority', 0)} Low Priority\n"
        f"\u274c {summary.get('Skip', 0)} Skip"
    )
    if failed_count:
        body += f"\nFailed: {failed_count}"

    publish_ntfy(
        title=f"\U0001f4ca Batch complete \u2014 {scored_count} scored, {failed_count} failed",
        message=body,
        priority="high" if summary.get("Apply Tonight", 0) else "default",
        tags=["briefcase"],
    )


def schedule_followup(job_id: int, days: int):
    followup_date = (datetime.utcnow() + timedelta(days=days)).isoformat()
    db_update("jobs", job_id, {"followup_at": followup_date, "status": "Applied"})


def process_job(url: str, send_notification_flag: bool = True) -> dict:
    print(f"[1/4] Scraping {url}...")
    job_text = scrape_job(url)

    print("[2/4] Scoring with Gemini...")
    score = score_job(job_text, url)

    print("[3/4] Saving to database...")
    saved = save_score(score)

    if send_notification_flag:
        print("[4/4] Sending push notification...")
        send_notification(score)

    print(f"Done. Score: {score['overall_score']}/100 - {score['verdict']}")
    return saved


def process_batch(
    search_url: str,
    send_notification_flag: bool = True,
    progress_callback=None,
    max_jobs: int | None = None,
) -> dict:
    def progress(**kwargs):
        if progress_callback:
            progress_callback(kwargs)

    if max_jobs is not None and max_jobs < 1:
        max_jobs = None

    progress(status="scraping", current=0, total=0, message="Finding LinkedIn jobs...")
    search_text = scrape_linkedin_search(search_url)
    print(f"[debug] Scraped LinkedIn search text first 500 chars:\n{search_text[:500]}")
    job_urls = extract_linkedin_job_urls(search_text)
    print(f"[debug] Found {len(job_urls)} LinkedIn job URLs after extraction.")

    if not job_urls:
        raise ValueError("No LinkedIn job URLs were found on that search page.")

    if max_jobs and len(job_urls) > max_jobs:
        found_count = len(job_urls)
        job_urls = job_urls[:max_jobs]
        print(f"[debug] Limiting batch to first {len(job_urls)} of {found_count} LinkedIn job URLs.")
        progress(
            status="scraping",
            current=0,
            total=len(job_urls),
            message=f"Found {found_count} jobs. Scoring first {len(job_urls)}...",
        )

    total = len(job_urls)
    results = []
    errors = []
    scored_scores = []
    notification_errors = []

    for index, job_url in enumerate(job_urls, start=1):
        progress(
            status="scoring",
            current=index,
            total=total,
            message=f"Scoring job {index} of {total}...",
            url=job_url,
        )
        try:
            try:
                job_text = scrape_job(job_url)
            except Exception as exc:
                if not ALLOW_FALLBACK_SCORING:
                    raise
                job_text = ""
                score = fallback_score_job("", job_url, f"Job scrape failed: {exc}")
            else:
                try:
                    score = score_job(job_text, job_url)
                except Exception as exc:
                    if not ALLOW_FALLBACK_SCORING:
                        raise
                    score = fallback_score_job(job_text, job_url, str(exc))

            try:
                saved = save_score(score)
            except Exception as exc:
                if not ALLOW_FALLBACK_SCORING:
                    raise
                saved = {**score, "id": None, "save_error": str(exc)}
                notification_errors.append({"url": job_url, "error": f"Save failed: {exc}"})
            scored_scores.append(saved)
            results.append(saved)
            if BATCH_JOB_DELAY_SECONDS:
                time.sleep(BATCH_JOB_DELAY_SECONDS)
        except Exception as exc:
            print(f"[ERROR] Failed to score {job_url}: {exc}")
            errors.append({"url": job_url, "error": str(exc)})

    if len(errors) > 0:
        for e in errors:
            print(f"[BATCH ERROR] {e['url']}: {e['error']}")

    summary = summarize_verdicts(scored_scores)
    grouped_results = group_results_by_verdict(results)
    if send_notification_flag:
        progress(
            status="notifying",
            current=total,
            total=total,
            message=f"Sending notifications for {len(scored_scores)} scored and {len(errors)} failed jobs...",
        )
        for score in scored_scores:
            try:
                send_notification(score)
            except Exception as exc:
                notification_errors.append({"url": score.get("url"), "error": str(exc)})
        for error in errors:
            try:
                send_failure_notification(error)
            except Exception as exc:
                notification_errors.append({"url": error.get("url"), "error": str(exc)})
        send_batch_notification(summary, len(scored_scores), len(errors))

    failed_count = len(errors)
    complete_message = f"Finished scoring {len(scored_scores)} of {total} jobs."
    if failed_count:
        complete_message += f" {failed_count} failed."

    progress(
        status="complete",
        current=total,
        total=total,
        message=complete_message,
    )
    return {
        "search_url": search_url,
        "total": total,
        "scored": len(scored_scores),
        "failed": len(errors),
        "summary": summary,
        "grouped_results": grouped_results,
        "results": results,
        "errors": errors,
        "notification_errors": notification_errors,
    }


def process_search_batch(
    keywords: str,
    location: str,
    send_notification_flag: bool = True,
    progress_callback=None,
    max_jobs: int | None = MAX_BATCH_JOBS,
) -> dict:
    keywords = keywords.strip()
    location = location.strip()
    if not keywords:
        raise ValueError("Job title / keywords is required.")
    if not location:
        raise ValueError("Location is required.")

    search_url = build_linkedin_search_url(keywords, location)
    result = process_batch(
        search_url,
        send_notification_flag=send_notification_flag,
        progress_callback=progress_callback,
        max_jobs=max_jobs,
    )
    result["keywords"] = keywords
    result["location"] = location
    return result


def check_followups():
    today = datetime.utcnow().date().isoformat()
    due = db_select("jobs", f"followup_at=lte.{today}T23:59:59&status=eq.Applied&select=*")

    for job in due:
        body = (
            f"{job['role']} @ {job['company']}\n"
            f"Applied {job['followup_days']} days ago - send a follow-up email or LinkedIn message."
        )

        publish_ntfy(
            title=f"Follow-up due: {job['company']}",
            message=body,
            priority="high",
            tags=["alarm_clock"],
        )
        db_update("jobs", job["id"], {"status": "Followed Up"})
        print(f"Sent follow-up reminder for {job['role']} @ {job['company']}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python agent.py <job_url>")
        print("       python agent.py --batch <linkedin_search_url>")
        print("       python agent.py --search <keywords> <location>")
        print("       python agent.py --check-followups")
        sys.exit(1)

    if sys.argv[1] == "--check-followups":
        check_followups()
    elif sys.argv[1] == "--batch":
        if len(sys.argv) < 3:
            print("Usage: python agent.py --batch <linkedin_search_url>")
            sys.exit(1)
        print(json.dumps(process_batch(sys.argv[2]), indent=2))
    elif sys.argv[1] == "--search":
        if len(sys.argv) < 4:
            print("Usage: python agent.py --search <keywords> <location>")
            sys.exit(1)
        print(json.dumps(process_search_batch(sys.argv[2], sys.argv[3]), indent=2))
    else:
        print(json.dumps(process_job(sys.argv[1]), indent=2))
