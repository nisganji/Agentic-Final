"""
FastAPI server for the Job Fit Agent dashboard.
"""

import json
import threading
import uuid
from datetime import datetime
from urllib.parse import unquote

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from agent import (
    db_select,
    db_update,
    get_resume,
    parse_resume_pdf,
    process_job,
    process_search_batch,
    save_resume,
)


app = FastAPI(title="Job Fit Agent API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class JobRequest(BaseModel):
    url: str
    send_sms: bool = True


class BatchRequest(BaseModel):
    keywords: str
    location: str
    send_sms: bool = True


class StatusUpdate(BaseModel):
    status: str


batch_states: dict[str, dict] = {}
batch_lock = threading.Lock()
latest_batch_id: str | None = None


def _json_lists(job: dict) -> dict:
    for field in ["red_flags", "green_flags"]:
        if isinstance(job.get(field), str):
            try:
                job[field] = json.loads(job[field])
            except json.JSONDecodeError:
                job[field] = []
    return job


def _set_batch_state(batch_id: str, **updates) -> dict:
    with batch_lock:
        current = batch_states.get(batch_id, {})
        current.update(updates)
        current["updated_at"] = datetime.utcnow().isoformat()
        batch_states[batch_id] = current
        return dict(current)


def _run_batch(batch_id: str, keywords: str, location: str, send_sms: bool):
    def progress(update: dict):
        _set_batch_state(batch_id, **update)

    try:
        _set_batch_state(
            batch_id,
            status="running",
            message="Finding LinkedIn jobs...",
            current=0,
            total=0,
        )
        result = process_search_batch(
            keywords,
            location,
            send_notification_flag=send_sms,
            progress_callback=progress,
        )
        _set_batch_state(
            batch_id,
            status="complete",
            message=f"Finished scoring {result['scored']} of {result['total']} jobs.",
            current=result["total"],
            total=result["total"],
            summary=result["summary"],
            grouped_results=result["grouped_results"],
            result=result,
            errors=result["errors"],
        )
    except Exception as exc:
        _set_batch_state(
            batch_id,
            status="failed",
            message=str(exc),
            error=str(exc),
        )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/resume")
def resume_status():
    try:
        resume = get_resume()
    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        return {
            "uploaded": False,
            "message": "Resume table is not ready. Run the resume SQL in schema.sql.",
            "detail": detail,
        }

    if not resume:
        return {"uploaded": False}

    return {
        "uploaded": True,
        "filename": resume.get("filename"),
        "uploaded_at": resume.get("uploaded_at"),
        "updated_at": resume.get("updated_at"),
        "preview": (resume.get("parsed_text") or "")[:1200],
    }


@app.post("/resume/upload")
async def upload_resume(request: Request):
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="Upload a PDF resume first.")

    filename = unquote(request.headers.get("x-filename", "resume.pdf"))
    content_type = request.headers.get("content-type", "").lower()
    if "pdf" not in content_type and not filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Resume upload must be a PDF.")

    try:
        parsed_text = parse_resume_pdf(body, filename)
        resume = save_resume(parsed_text, filename)
        return {
            "ok": True,
            "uploaded": True,
            "message": "Resume uploaded ✅",
            "filename": resume.get("filename"),
            "uploaded_at": resume.get("uploaded_at"),
            "preview": (resume.get("parsed_text") or "")[:1200],
        }
    except requests.HTTPError as exc:
        detail = exc.response.text if exc.response is not None else str(exc)
        raise HTTPException(status_code=500, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/jobs/score")
def score_new_job(req: JobRequest):
    try:
        result = process_job(req.url, send_notification_flag=req.send_sms)
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/jobs/search-score")
def start_search_score(req: BatchRequest):
    global latest_batch_id
    if not req.keywords.strip():
        raise HTTPException(status_code=400, detail="Job title / keywords is required.")
    if not req.location.strip():
        raise HTTPException(status_code=400, detail="Location is required.")

    batch_id = str(uuid.uuid4())
    latest_batch_id = batch_id
    _set_batch_state(
        batch_id,
        id=batch_id,
        status="queued",
        message="Queued batch scoring...",
        current=0,
        total=0,
        keywords=req.keywords.strip(),
        location=req.location.strip(),
        summary=None,
        grouped_results=None,
        errors=[],
        created_at=datetime.utcnow().isoformat(),
    )

    thread = threading.Thread(
        target=_run_batch,
        args=(batch_id, req.keywords, req.location, req.send_sms),
        daemon=True,
    )
    thread.start()
    return _set_batch_state(batch_id, status="running", message="Finding LinkedIn jobs...")


@app.get("/jobs/batch-status")
def get_batch_status(batch_id: str = None):
    lookup_id = batch_id or latest_batch_id
    if not lookup_id:
        raise HTTPException(status_code=404, detail="No batch has been started.")
    with batch_lock:
        state = batch_states.get(lookup_id)
    if not state:
        raise HTTPException(status_code=404, detail="Batch not found.")
    return state


@app.get("/jobs")
def list_jobs(status: str = None, min_score: int = 0):
    filters = f"overall_score=gte.{min_score}&order=scored_at.desc"
    if status:
        filters += f"&status=eq.{status}"
    jobs = db_select("jobs", filters)
    return [_json_lists(job) for job in jobs]


@app.get("/jobs/{job_id}")
def get_job(job_id: int):
    jobs = db_select("jobs", f"id=eq.{job_id}")
    if not jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _json_lists(jobs[0])


@app.patch("/jobs/{job_id}/status")
def update_status(job_id: int, body: StatusUpdate):
    db_update("jobs", job_id, {"status": body.status})
    return {"ok": True}


@app.get("/stats")
def get_stats():
    all_jobs = db_select("jobs", "order=scored_at.desc")
    if not all_jobs:
        return {
            "total": 0,
            "applied": 0,
            "avg_score": 0,
            "apply_tonight": 0,
            "offers": 0,
            "recent": [],
        }

    scores = [j["overall_score"] for j in all_jobs if j.get("overall_score")]
    applied = [j for j in all_jobs if j.get("status") in ["Applied", "Followed Up", "Offer"]]
    fire = [j for j in all_jobs if j.get("verdict") == "Apply Tonight"]

    return {
        "total": len(all_jobs),
        "applied": len(applied),
        "avg_score": round(sum(scores) / len(scores)) if scores else 0,
        "apply_tonight": len(fire),
        "offers": len([j for j in all_jobs if j.get("status") == "Offer"]),
        "recent": [_json_lists(job) for job in all_jobs[:5]],
    }
