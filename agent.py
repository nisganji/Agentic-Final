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
from urllib.parse import parse_qs, unquote, urlencode, urlparse, urlunparse

import google.generativeai as genai
import requests


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC") or "team7-jobagent-2026"

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


def _is_model_availability_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "is not found" in message
        or "not supported for generatecontent" in message
        or "not found for api version" in message
    )


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if this is a 429 quota/rate-limit error."""
    msg = str(exc).lower()
    return "429" in msg or "quota" in msg or "rate" in msg or "resource_exhausted" in msg


def generate_with_gemini(contents):
    """Generate content using the first available Flash model, with rate-limit retry."""
    global _active_model_name

    candidates = [_active_model_name] if _active_model_name else []
    candidates.extend(name for name in MODEL_FALLBACKS if name not in candidates)

    last_error = None
    for model_name in candidates:
        # Retry up to 3 times on rate-limit errors with exponential backoff
        for attempt in range(3):
            try:
                response = genai.GenerativeModel(model_name).generate_content(contents)
                _active_model_name = model_name
                return response
            except Exception as exc:
                last_error = exc
                if _is_rate_limit_error(exc):
                    wait = 20 * (attempt + 1)  # 20s, 40s, 60s
                    print(f"[rate limit] 429 hit on {model_name}, waiting {wait}s before retry {attempt + 1}/3...")
                    time.sleep(wait)
                    continue
                elif _is_model_availability_error(exc):
                    break  # try next model
                else:
                    raise
        else:
            continue  # all retries exhausted for this model, try next

    raise RuntimeError(
        "No configured Gemini Flash model is available for generateContent. "
        "Set GEMINI_MODEL in .env to a model listed for your API key."
    ) from last_error


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
        timeout=45,
    )
    r.raise_for_status()
    return r.text[:max_chars]


def scrape_job(url: str) -> str:
    return scrape_url(url, max_chars=6000)


def scrape_linkedin_search(search_url: str) -> str:
    text_parts = [scrape_url(search_url, max_chars=80000)]

    parsed = urlparse(search_url)
    query = parse_qs(parsed.query)
    keywords = (query.get("keywords") or [""])[0]
    location = (query.get("location") or [""])[0]

    if keywords or location:
        for guest_url in build_linkedin_guest_search_urls(search_url, pages=2):
            try:
                r = requests.get(
                    guest_url,
                    headers={
                        "Accept": "text/html,application/xhtml+xml",
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    },
                    timeout=45,
                )
                r.raise_for_status()
                text_parts.append(r.text[:80000])
            except Exception:
                continue

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
    urls: set[str] = set()

    url_pattern = re.compile(
        r"https?://(?:www\.)?linkedin\.com/jobs/view/[^\s\]\)\"'<>]+",
        re.IGNORECASE,
    )
    for match in url_pattern.findall(text):
        cleaned = _clean_linkedin_job_url(match)
        if cleaned:
            urls.add(cleaned)

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
            urls.add(f"https://www.linkedin.com/jobs/view/{job_id}")

    urls = {u for u in urls if re.search(r'\d{8,}', u)}
    return sorted(urls)


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
    response = generate_with_gemini(prompt)
    result = _parse_gemini_json(response.text)
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

    body = (
        f"{role} | Score: {overall_score}/100 | Visa: {sponsors_visa}\n"
        f"{summary[:350]}"
    )

    publish_ntfy(
        title=f"{emoji_map.get(verdict, '')} {verdict} \u2014 {company}",
        message=body,
        priority=priority_map.get(verdict, "default"),
        tags=["briefcase"],
        click=score.get("url"),
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


def send_batch_notification(summary: dict, scored_count: int):
    body = (
        f"\U0001f525 {summary.get('Apply Tonight', 0)} Apply Tonight\n"
        f"\u2705 {summary.get('Apply This Weekend', 0)} Apply This Weekend\n"
        f"\U0001f7e1 {summary.get('Low Priority', 0)} Low Priority\n"
        f"\u274c {summary.get('Skip', 0)} Skip"
    )
    publish_ntfy(
        title=f"\U0001f4ca Batch complete \u2014 {scored_count} jobs scored",
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
) -> dict:
    def progress(**kwargs):
        if progress_callback:
            progress_callback(kwargs)

    progress(status="scraping", current=0, total=0, message="Finding LinkedIn jobs...")
    search_text = scrape_linkedin_search(search_url)
    print(f"[debug] Scraped LinkedIn search text first 500 chars:\n{search_text[:500]}")
    job_urls = extract_linkedin_job_urls(search_text)
    print(f"[debug] Found {len(job_urls)} LinkedIn job URLs after extraction.")
    if not job_urls:
        for guest_url in build_linkedin_guest_search_urls(search_url):
            try:
                guest_text = scrape_linkedin_search(guest_url)
                job_urls.extend(extract_linkedin_job_urls(guest_text))
            except Exception:
                continue
        job_urls = sorted(set(job_urls))
        print(f"[debug] Found {len(job_urls)} LinkedIn job URLs after guest fallback extraction.")

    if not job_urls:
        raise ValueError("No LinkedIn job URLs were found on that search page.")

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
            job_text = scrape_job(job_url)
            score = score_job(job_text, job_url)
            scored_scores.append(score)
            if send_notification_flag:
                try:
                    send_notification(score)
                except Exception as exc:
                    notification_errors.append({"url": job_url, "error": str(exc)})
            saved = save_score(score)
            results.append(saved)
            time.sleep(5)  # avoid rate limits between jobs
        except Exception as exc:
            print(f"[ERROR] Failed to score {job_url}: {exc}")
            errors.append({"url": job_url, "error": str(exc)})

    if len(errors) > 0:
        for e in errors:
            print(f"[BATCH ERROR] {e['url']}: {e['error']}")

    if not scored_scores and errors:
        raise RuntimeError(f"No jobs were scored. First error: {errors[0]['error']}")

    summary = summarize_verdicts(scored_scores)
    grouped_results = group_results_by_verdict(results)
    if send_notification_flag:
        send_batch_notification(summary, len(scored_scores))

    progress(
        status="complete",
        current=total,
        total=total,
        message=f"Finished scoring {len(scored_scores)} of {total} jobs.",
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
