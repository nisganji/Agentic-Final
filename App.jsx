import { useEffect, useMemo, useState } from "react";

const API = import.meta.env.VITE_API_URL || "http://localhost:8000";

const STATUS_OPTIONS = ["Scored", "Applied", "Followed Up", "Offer", "Rejected", "Archived"];

const VERDICTS = {
  "Apply Tonight": { color: "#fb7185", bg: "rgba(251, 113, 133, 0.12)" },
  "Apply This Weekend": { color: "#34d399", bg: "rgba(52, 211, 153, 0.12)" },
  "Low Priority": { color: "#facc15", bg: "rgba(250, 204, 21, 0.13)" },
  Skip: { color: "#94a3b8", bg: "rgba(148, 163, 184, 0.12)" },
};

const VERDICT_GROUPS = [
  { key: "Apply Tonight", label: "🔥 Apply Tonight" },
  { key: "Apply This Weekend", label: "✅ Apply This Weekend" },
  { key: "Low Priority", label: "🟡 Low Priority" },
  { key: "Skip", label: "❌ Skip" },
];

const FILTERS = [
  { key: "all", label: "All jobs" },
  { key: "fire", label: "Apply Tonight" },
  { key: "applied", label: "Applied" },
  { key: "visa", label: "Sponsors visa" },
];

function parseList(value) {
  if (Array.isArray(value)) return value;
  if (!value) return [];
  try {
    return JSON.parse(value);
  } catch {
    return [];
  }
}

function scoreColor(score = 0) {
  if (score >= 78) return "#34d399";
  if (score >= 58) return "#facc15";
  return "#fb7185";
}

function formatDate(value) {
  if (!value) return "Not scored yet";
  return new Date(value).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

async function getErrorMessage(response) {
  try {
    const data = await response.json();
    return data.detail || data.message || JSON.stringify(data);
  } catch {
    return response.text();
  }
}

function GlobalStyles() {
  return (
    <style>{`
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

      * { box-sizing: border-box; }
      html, body, #root {
        width: 100%;
        max-width: none;
        min-height: 100%;
        margin: 0;
        border: 0;
        background: #080a0f;
        color: #e5e7eb;
        text-align: left;
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        letter-spacing: 0;
      }
      body { overflow-x: hidden; }
      #root { display: block; }
      button, input, select, textarea { font: inherit; }
      button { border: 0; }
      a { color: inherit; }

      .app-shell {
        min-height: 100vh;
        display: grid;
        grid-template-columns: 264px minmax(0, 1fr);
        background:
          radial-gradient(circle at top left, rgba(20, 184, 166, 0.12), transparent 34rem),
          linear-gradient(135deg, #080a0f 0%, #10131b 44%, #0c0f15 100%);
      }

      .sidebar {
        position: sticky;
        top: 0;
        height: 100vh;
        padding: 26px 18px;
        border-right: 1px solid rgba(148, 163, 184, 0.14);
        background: rgba(7, 10, 16, 0.78);
        backdrop-filter: blur(18px);
        display: flex;
        flex-direction: column;
      }

      .logo {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 0 8px 28px;
      }

      .logo-mark {
        width: 42px;
        height: 42px;
        border-radius: 12px;
        background: linear-gradient(135deg, #14b8a6, #38bdf8);
        color: #031014;
        font-weight: 800;
        display: grid;
        place-items: center;
      }

      .logo-title {
        font-size: 16px;
        font-weight: 800;
        color: #f8fafc;
      }

      .logo-subtitle {
        margin-top: 2px;
        font-size: 12px;
        color: #64748b;
      }

      .nav {
        display: grid;
        gap: 8px;
      }

      .nav-button {
        width: 100%;
        padding: 12px 13px;
        border-radius: 10px;
        background: transparent;
        color: #94a3b8;
        text-align: left;
        cursor: pointer;
        font-weight: 650;
        transition: background 180ms ease, color 180ms ease, transform 180ms ease;
      }

      .nav-button:hover {
        background: rgba(148, 163, 184, 0.10);
        color: #e2e8f0;
      }

      .nav-button.active {
        background: rgba(20, 184, 166, 0.15);
        color: #f8fafc;
      }

      .sidebar-footer {
        margin-top: auto;
        padding: 14px;
        border-radius: 12px;
        border: 1px solid rgba(148, 163, 184, 0.14);
        background: rgba(15, 23, 42, 0.58);
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.5;
      }

      .main {
        min-width: 0;
        padding: 32px;
      }

      .page {
        max-width: 1180px;
        margin: 0 auto;
      }

      .page-header {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 18px;
        margin-bottom: 24px;
      }

      .eyebrow {
        color: #14b8a6;
        font-size: 12px;
        font-weight: 800;
        text-transform: uppercase;
      }

      .title {
        margin: 7px 0 0;
        color: #f8fafc;
        font-size: 34px;
        line-height: 1.05;
        font-weight: 800;
      }

      .muted {
        color: #94a3b8;
      }

      .panel {
        border: 1px solid rgba(148, 163, 184, 0.15);
        background: rgba(15, 23, 42, 0.76);
        border-radius: 14px;
        box-shadow: 0 22px 70px rgba(0, 0, 0, 0.22);
      }

      .panel-pad {
        padding: 20px;
      }

      .input-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto;
        gap: 10px;
        align-items: center;
      }

      .search-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
        gap: 10px;
        align-items: center;
      }

      .field {
        width: 100%;
        min-height: 46px;
        padding: 12px 14px;
        border-radius: 10px;
        border: 1px solid rgba(148, 163, 184, 0.22);
        background: rgba(2, 6, 23, 0.78);
        color: #f8fafc;
        outline: none;
      }

      .field:focus {
        border-color: rgba(20, 184, 166, 0.72);
        box-shadow: 0 0 0 4px rgba(20, 184, 166, 0.12);
      }

      .primary-button, .secondary-button {
        min-height: 46px;
        padding: 0 18px;
        border-radius: 10px;
        font-weight: 800;
        cursor: pointer;
        white-space: nowrap;
        transition: transform 160ms ease, opacity 160ms ease, background 160ms ease;
      }

      .primary-button {
        background: #14b8a6;
        color: #031014;
      }

      .secondary-button {
        background: rgba(148, 163, 184, 0.14);
        color: #e2e8f0;
        border: 1px solid rgba(148, 163, 184, 0.18);
      }

      .primary-button:hover, .secondary-button:hover {
        transform: translateY(-1px);
      }

      .primary-button:disabled, .secondary-button:disabled {
        cursor: not-allowed;
        opacity: 0.55;
        transform: none;
      }

      .toggle-row {
        margin-top: 12px;
        display: flex;
        align-items: center;
        gap: 10px;
        color: #94a3b8;
        font-size: 13px;
      }

      .sections-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(320px, 0.55fr);
        gap: 16px;
        margin-bottom: 20px;
      }

      .section-title {
        margin: 0 0 14px;
        color: #f8fafc;
        font-size: 15px;
        font-weight: 800;
      }

      .stats-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        margin: 0 0 20px;
      }

      .stat {
        padding: 17px;
      }

      .stat-label {
        color: #94a3b8;
        font-size: 12px;
        font-weight: 650;
      }

      .stat-value {
        margin-top: 8px;
        color: #f8fafc;
        font-size: 30px;
        font-weight: 800;
        line-height: 1;
      }

      .filters {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-bottom: 14px;
      }

      .filter-button {
        padding: 9px 13px;
        border-radius: 999px;
        background: rgba(148, 163, 184, 0.10);
        color: #94a3b8;
        cursor: pointer;
        font-size: 13px;
        font-weight: 750;
      }

      .filter-button.active {
        background: #e2e8f0;
        color: #0f172a;
      }

      .jobs-list {
        display: grid;
        gap: 12px;
      }

      .job-card {
        overflow: hidden;
        border-radius: 14px;
        border: 1px solid rgba(148, 163, 184, 0.15);
        background: rgba(15, 23, 42, 0.78);
        transition: border-color 180ms ease, transform 180ms ease, box-shadow 180ms ease;
      }

      .job-card:hover {
        border-color: rgba(20, 184, 166, 0.36);
        transform: translateY(-1px);
        box-shadow: 0 18px 50px rgba(0, 0, 0, 0.22);
      }

      .job-top {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr) auto auto;
        gap: 16px;
        align-items: center;
        padding: 18px;
        cursor: pointer;
      }

      .score-circle {
        width: 62px;
        height: 62px;
        border-radius: 999px;
        border: 3px solid var(--score-color);
        color: var(--score-color);
        display: grid;
        place-items: center;
        font-weight: 800;
        background: color-mix(in srgb, var(--score-color) 13%, transparent);
        flex-shrink: 0;
      }

      .score-circle span {
        font-size: 22px;
        line-height: 1;
      }

      .score-circle small {
        display: block;
        margin-top: -7px;
        color: #94a3b8;
        font-size: 10px;
        font-weight: 700;
      }

      .job-role {
        color: #f8fafc;
        font-size: 16px;
        font-weight: 800;
        line-height: 1.25;
      }

      .job-meta {
        margin-top: 6px;
        color: #94a3b8;
        font-size: 13px;
        line-height: 1.45;
      }

      .pill-row {
        display: flex;
        flex-wrap: wrap;
        gap: 7px;
        margin-top: 10px;
      }

      .pill {
        display: inline-flex;
        align-items: center;
        min-height: 26px;
        padding: 0 10px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
      }

      .status-select {
        min-height: 38px;
        padding: 0 10px;
        border-radius: 9px;
        border: 1px solid rgba(148, 163, 184, 0.22);
        background: rgba(2, 6, 23, 0.8);
        color: #e2e8f0;
        outline: none;
      }

      .expand-button {
        width: 36px;
        height: 36px;
        border-radius: 9px;
        background: rgba(148, 163, 184, 0.12);
        color: #e2e8f0;
        cursor: pointer;
        font-size: 20px;
        line-height: 1;
      }

      .job-details {
        max-height: 0;
        opacity: 0;
        overflow: hidden;
        padding: 0 18px;
        transition: max-height 340ms ease, opacity 220ms ease, padding 340ms ease;
      }

      .job-card.expanded .job-details {
        max-height: 820px;
        opacity: 1;
        padding: 0 18px 18px;
      }

      .summary {
        margin: 0 0 18px;
        color: #cbd5e1;
        line-height: 1.65;
        font-size: 14px;
      }

      .detail-grid {
        display: grid;
        grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr);
        gap: 22px;
      }

      .score-bar {
        margin-bottom: 12px;
      }

      .score-bar-top {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 7px;
        color: #94a3b8;
        font-size: 12px;
        font-weight: 700;
      }

      .bar-track {
        height: 7px;
        border-radius: 999px;
        overflow: hidden;
        background: rgba(148, 163, 184, 0.14);
      }

      .bar-fill {
        height: 100%;
        border-radius: inherit;
        background: var(--bar-color);
        transition: width 500ms ease;
      }

      .flag-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }

      .flag-box {
        border-radius: 12px;
        padding: 13px;
        background: rgba(2, 6, 23, 0.45);
      }

      .flag-title {
        margin-bottom: 9px;
        color: #f8fafc;
        font-size: 12px;
        font-weight: 800;
        text-transform: uppercase;
      }

      .flag-item {
        margin: 0 0 7px;
        color: #cbd5e1;
        font-size: 13px;
        line-height: 1.45;
      }

      .open-link {
        display: inline-flex;
        margin-top: 16px;
        color: #67e8f9;
        font-size: 13px;
        font-weight: 800;
        text-decoration: none;
      }

      .progress-wrap {
        margin-top: 14px;
      }

      .progress-label {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        color: #cbd5e1;
        font-size: 13px;
        font-weight: 750;
        margin-bottom: 8px;
      }

      .progress-track {
        height: 10px;
        border-radius: 999px;
        overflow: hidden;
        background: rgba(148, 163, 184, 0.15);
      }

      .progress-fill {
        height: 100%;
        border-radius: inherit;
        background: linear-gradient(90deg, #14b8a6, #38bdf8);
        transition: width 260ms ease;
      }

      .grouped-results {
        margin: 0 0 20px;
      }

      .verdict-group {
        margin-top: 12px;
      }

      .verdict-heading {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 10px;
        color: #f8fafc;
        font-size: 14px;
        font-weight: 800;
      }

      .verdict-count {
        color: #94a3b8;
        font-size: 12px;
        font-weight: 750;
      }

      .message {
        margin-top: 12px;
        padding: 12px 14px;
        border-radius: 10px;
        font-size: 13px;
        font-weight: 700;
      }

      .message.error {
        color: #fecdd3;
        background: rgba(251, 113, 133, 0.12);
        border: 1px solid rgba(251, 113, 133, 0.20);
      }

      .message.success {
        color: #bbf7d0;
        background: rgba(52, 211, 153, 0.12);
        border: 1px solid rgba(52, 211, 153, 0.20);
      }

      .empty-state {
        padding: 44px 20px;
        text-align: center;
        color: #64748b;
      }

      .upload-row {
        display: grid;
        grid-template-columns: auto minmax(0, 1fr) auto;
        gap: 12px;
        align-items: center;
      }

      .file-label {
        min-height: 46px;
        display: inline-flex;
        align-items: center;
        padding: 0 16px;
        border-radius: 10px;
        background: rgba(148, 163, 184, 0.14);
        color: #e2e8f0;
        cursor: pointer;
        font-weight: 800;
        border: 1px solid rgba(148, 163, 184, 0.18);
      }

      .file-name {
        min-width: 0;
        color: #cbd5e1;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }

      .resume-preview {
        margin-top: 18px;
        padding: 16px;
        border-radius: 12px;
        color: #cbd5e1;
        background: rgba(2, 6, 23, 0.48);
        white-space: pre-wrap;
        line-height: 1.55;
        font-size: 13px;
        max-height: 360px;
        overflow: auto;
      }

      .settings-grid {
        display: grid;
        gap: 14px;
      }

      .setting-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 18px;
        padding: 15px 0;
        border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      }

      .switch {
        position: relative;
        width: 52px;
        height: 30px;
      }

      .switch input {
        opacity: 0;
        width: 0;
        height: 0;
      }

      .slider {
        position: absolute;
        inset: 0;
        cursor: pointer;
        border-radius: 999px;
        background: rgba(148, 163, 184, 0.28);
        transition: background 180ms ease;
      }

      .slider:before {
        content: "";
        position: absolute;
        width: 22px;
        height: 22px;
        left: 4px;
        top: 4px;
        border-radius: 50%;
        background: #f8fafc;
        transition: transform 180ms ease;
      }

      .switch input:checked + .slider {
        background: #14b8a6;
      }

      .switch input:checked + .slider:before {
        transform: translateX(22px);
      }

      @media (max-width: 980px) {
        .app-shell { grid-template-columns: 1fr; }
        .sidebar {
          position: relative;
          height: auto;
          padding: 18px;
        }
        .nav { grid-template-columns: repeat(3, minmax(0, 1fr)); }
        .sidebar-footer { display: none; }
        .main { padding: 22px; }
        .sections-grid, .stats-grid, .detail-grid, .flag-grid {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 680px) {
        .main { padding: 16px; }
        .page-header { align-items: flex-start; flex-direction: column; }
        .input-grid, .search-grid, .upload-row { grid-template-columns: 1fr; }
        .job-top { grid-template-columns: auto minmax(0, 1fr); }
        .status-select, .expand-button { grid-column: 2; }
        .title { font-size: 28px; }
      }
    `}</style>
  );
}

function Sidebar({ activeView, onViewChange, resumeUploaded }) {
  const nav = ["Dashboard", "Resume", "Settings"];
  return (
    <aside className="sidebar">
      <div className="logo">
        <div className="logo-mark">JA</div>
        <div>
          <div className="logo-title">JobAgent</div>
          <div className="logo-subtitle">Visa-aware scoring</div>
        </div>
      </div>
      <nav className="nav">
        {nav.map((item) => {
          const id = item.toLowerCase();
          return (
            <button
              key={id}
              className={`nav-button ${activeView === id ? "active" : ""}`}
              onClick={() => onViewChange(id)}
            >
              {item}
            </button>
          );
        })}
      </nav>
      <div className="sidebar-footer">
        <strong style={{ color: "#e2e8f0" }}>Profile source</strong>
        <div style={{ marginTop: 6 }}>
          {resumeUploaded ? "Uploaded resume is active." : "Using fallback candidate profile."}
        </div>
      </div>
    </aside>
  );
}

function PageHeader({ eyebrow, title, right }) {
  return (
    <div className="page-header">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1 className="title">{title}</h1>
      </div>
      {right}
    </div>
  );
}

function StatCard({ label, value, color }) {
  return (
    <div className="panel stat">
      <div className="stat-label">{label}</div>
      <div className="stat-value" style={{ color: color || "#f8fafc" }}>
        {value}
      </div>
    </div>
  );
}

function ScoreBar({ label, value, color }) {
  const safeValue = Math.max(0, Math.min(100, Number(value) || 0));
  return (
    <div className="score-bar">
      <div className="score-bar-top">
        <span>{label}</span>
        <span style={{ color }}>{safeValue}</span>
      </div>
      <div className="bar-track">
        <div className="bar-fill" style={{ width: `${safeValue}%`, "--bar-color": color }} />
      </div>
    </div>
  );
}

function Pill({ children, color, bg }) {
  return (
    <span className="pill" style={{ color, background: bg }}>
      {children}
    </span>
  );
}

function JobCard({ job, onStatusChange }) {
  const [expanded, setExpanded] = useState(false);
  const verdict = VERDICTS[job.verdict] || VERDICTS["Low Priority"];
  const flags = parseList(job.red_flags);
  const greens = parseList(job.green_flags);
  const color = scoreColor(job.overall_score);

  return (
    <article className={`job-card ${expanded ? "expanded" : ""}`}>
      <div className="job-top" onClick={() => setExpanded((value) => !value)}>
        <div className="score-circle" style={{ "--score-color": color }}>
          <div>
            <span>{job.overall_score ?? 0}</span>
            <small>/100</small>
          </div>
        </div>

        <div style={{ minWidth: 0 }}>
          <div className="job-role">{job.role || "Untitled role"}</div>
          <div className="job-meta">
            {job.company || "Unknown company"} · {job.location || "Unknown location"} · {formatDate(job.scored_at)}
          </div>
          <div className="pill-row">
            <Pill color={verdict.color} bg={verdict.bg}>
              {job.verdict || "Low Priority"}
            </Pill>
            <Pill
              color={job.sponsors_visa === "Yes" ? "#34d399" : job.sponsors_visa === "No" ? "#fb7185" : "#facc15"}
              bg={
                job.sponsors_visa === "Yes"
                  ? "rgba(52, 211, 153, 0.12)"
                  : job.sponsors_visa === "No"
                    ? "rgba(251, 113, 133, 0.12)"
                    : "rgba(250, 204, 21, 0.12)"
              }
            >
              Visa: {job.sponsors_visa || "Unknown"}
            </Pill>
            {job.salary_range && job.salary_range !== "Unknown" && (
              <Pill color="#67e8f9" bg="rgba(103, 232, 249, 0.12)">
                {job.salary_range}
              </Pill>
            )}
          </div>
        </div>

        <select
          className="status-select"
          value={job.status || "Scored"}
          onClick={(event) => event.stopPropagation()}
          onChange={(event) => {
            event.stopPropagation();
            onStatusChange(job.id, event.target.value);
          }}
        >
          {STATUS_OPTIONS.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>

        <button className="expand-button" type="button" aria-label={expanded ? "Collapse" : "Expand"}>
          {expanded ? "-" : "+"}
        </button>
      </div>

      <div className="job-details">
        <p className="summary">{job.summary || "No summary available."}</p>

        <div className="detail-grid">
          <div>
            <ScoreBar label="Skill match" value={job.skill_match} color="#38bdf8" />
            <ScoreBar label="Visa friendliness" value={job.visa_friendliness} color={scoreColor(job.visa_friendliness)} />
            <ScoreBar label="Seniority fit" value={job.seniority_fit} color="#a78bfa" />
            <ScoreBar label="Company quality" value={job.company_quality} color="#34d399" />
          </div>

          <div className="flag-grid">
            <div className="flag-box">
              <div className="flag-title">Green flags</div>
              {greens.length ? (
                greens.map((item, index) => (
                  <p className="flag-item" key={`${item}-${index}`}>
                    + {item}
                  </p>
                ))
              ) : (
                <p className="flag-item muted">None found</p>
              )}
            </div>
            <div className="flag-box">
              <div className="flag-title">Red flags</div>
              {flags.length ? (
                flags.map((item, index) => (
                  <p className="flag-item" key={`${item}-${index}`}>
                    - {item}
                  </p>
                ))
              ) : (
                <p className="flag-item muted">None found</p>
              )}
            </div>
          </div>
        </div>

        {job.url && (
          <a className="open-link" href={job.url} target="_blank" rel="noreferrer">
            Open job posting
          </a>
        )}
      </div>
    </article>
  );
}

function BatchProgress({ batch }) {
  if (!batch) return null;
  const total = Number(batch.total) || 0;
  const current = Number(batch.current) || 0;
  const percent = total ? Math.round((current / total) * 100) : batch.status === "running" ? 18 : 0;
  const label =
    total && batch.status !== "complete"
      ? `Scoring job ${Math.min(current, total)} of ${total}...`
      : batch.message || "Preparing batch scoring...";

  return (
    <div className="progress-wrap">
      <div className="progress-label">
        <span>{label}</span>
        <span>{total ? `${percent}%` : ""}</span>
      </div>
      <div className="progress-track">
        <div className="progress-fill" style={{ width: `${Math.max(6, Math.min(100, percent))}%` }} />
      </div>
      {batch.status === "complete" && batch.summary && (
        <div className="message success">
          Batch complete. Apply Tonight: {batch.summary["Apply Tonight"] || 0}, Apply This Weekend:{" "}
          {batch.summary["Apply This Weekend"] || 0}, Low Priority: {batch.summary["Low Priority"] || 0}, Skip:{" "}
          {batch.summary.Skip || 0}
        </div>
      )}
      {batch.status === "failed" && <div className="message error">{batch.message || "Batch scoring failed."}</div>}
    </div>
  );
}

function GroupedBatchResults({ batch, onStatusChange }) {
  const grouped = batch?.grouped_results || batch?.result?.grouped_results;
  if (batch?.status !== "complete" || !grouped) return null;

  return (
    <section className="panel panel-pad grouped-results">
      <h2 className="section-title">Latest search results</h2>
      {VERDICT_GROUPS.map((group) => {
        const items = grouped[group.key] || [];
        return (
          <div className="verdict-group" key={group.key}>
            <div className="verdict-heading">
              <span>{group.label}</span>
              <span className="verdict-count">{items.length} jobs</span>
            </div>
            {items.length ? (
              <div className="jobs-list">
                {items.map((job) => (
                  <JobCard key={job.id || job.url} job={job} onStatusChange={onStatusChange} />
                ))}
              </div>
            ) : (
              <div className="panel empty-state" style={{ padding: 18 }}>
                No jobs in this group.
              </div>
            )}
          </div>
        );
      })}
    </section>
  );
}

function DashboardView({
  jobs,
  stats,
  loading,
  error,
  filter,
  setFilter,
  url,
  setUrl,
  searchKeywords,
  setSearchKeywords,
  searchLocation,
  setSearchLocation,
  scoring,
  batching,
  batchProgress,
  sendNotifications,
  setSendNotifications,
  onScore,
  onBatchScore,
  onStatusChange,
}) {
  const filteredJobs = useMemo(() => {
    return jobs.filter((job) => {
      if (filter === "all") return true;
      if (filter === "fire") return job.verdict === "Apply Tonight";
      if (filter === "applied") return ["Applied", "Followed Up", "Offer"].includes(job.status);
      if (filter === "visa") return job.sponsors_visa === "Yes";
      return true;
    });
  }, [jobs, filter]);

  return (
    <div className="page">
      <PageHeader eyebrow="Dashboard" title="Job scoring workspace" />

      <div className="sections-grid">
        <section className="panel panel-pad">
          <h2 className="section-title">Score a job URL</h2>
          <div className="input-grid">
            <input
              className="field"
              value={url}
              onChange={(event) => setUrl(event.target.value)}
              onKeyDown={(event) => event.key === "Enter" && onScore()}
              placeholder="Paste a job URL"
            />
            <button className="primary-button" type="button" onClick={onScore} disabled={scoring || !url.trim()}>
              {scoring ? "Scoring..." : "Score job"}
            </button>
          </div>
          <label className="toggle-row">
            <input
              type="checkbox"
              checked={sendNotifications}
              onChange={(event) => setSendNotifications(event.target.checked)}
            />
            Send ntfy notification
          </label>
          {error && <div className="message error">{error}</div>}
        </section>

        <section className="panel panel-pad">
          <h2 className="section-title">Find & score LinkedIn jobs</h2>
          <div className="search-grid">
            <input
              className="field"
              value={searchKeywords}
              onChange={(event) => setSearchKeywords(event.target.value)}
              placeholder="Job title / keywords"
            />
            <input
              className="field"
              value={searchLocation}
              onChange={(event) => setSearchLocation(event.target.value)}
              placeholder="Location"
            />
            <button
              className="secondary-button"
              type="button"
              onClick={onBatchScore}
              disabled={batching || !searchKeywords.trim() || !searchLocation.trim()}
            >
              {batching ? "Running..." : "Find & Score Jobs"}
            </button>
          </div>
          <BatchProgress batch={batchProgress} />
        </section>
      </div>

      <GroupedBatchResults batch={batchProgress} onStatusChange={onStatusChange} />

      <div className="stats-grid">
        <StatCard label="Jobs scored" value={stats?.total ?? 0} />
        <StatCard label="Applied" value={stats?.applied ?? 0} color="#38bdf8" />
        <StatCard label="Average score" value={`${stats?.avg_score ?? 0}/100`} color="#a78bfa" />
        <StatCard label="Apply Tonight" value={stats?.apply_tonight ?? 0} color="#fb7185" />
      </div>

      <div className="filters">
        {FILTERS.map((item) => (
          <button
            key={item.key}
            className={`filter-button ${filter === item.key ? "active" : ""}`}
            type="button"
            onClick={() => setFilter(item.key)}
          >
            {item.label}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="panel empty-state">Loading jobs...</div>
      ) : filteredJobs.length ? (
        <div className="jobs-list">
          {filteredJobs.map((job) => (
            <JobCard key={job.id} job={job} onStatusChange={onStatusChange} />
          ))}
        </div>
      ) : (
        <div className="panel empty-state">{jobs.length ? "No jobs match this filter." : "No scored jobs yet."}</div>
      )}
    </div>
  );
}

function ResumeView({ resume, resumeFile, setResumeFile, uploading, uploadMessage, uploadError, onUpload }) {
  return (
    <div className="page">
      <PageHeader eyebrow="Resume" title="Candidate profile" />
      <section className="panel panel-pad">
        <h2 className="section-title">Upload resume PDF</h2>
        <div className="upload-row">
          <label className="file-label" htmlFor="resume-file">
            Choose PDF
          </label>
          <input
            id="resume-file"
            type="file"
            accept="application/pdf"
            style={{ display: "none" }}
            onChange={(event) => setResumeFile(event.target.files?.[0] || null)}
          />
          <div className="file-name">{resumeFile?.name || resume?.filename || "No file selected"}</div>
          <button className="primary-button" type="button" onClick={onUpload} disabled={uploading || !resumeFile}>
            {uploading ? "Uploading..." : "Upload resume"}
          </button>
        </div>

        {resume?.uploaded && <div className="message success">Resume uploaded ✅</div>}
        {uploadMessage && <div className="message success">{uploadMessage}</div>}
        {uploadError && <div className="message error">{uploadError}</div>}

        {resume?.preview && (
          <div className="resume-preview">
            <strong style={{ color: "#f8fafc" }}>Parsed resume preview</strong>
            {"\n\n"}
            {resume.preview}
          </div>
        )}
      </section>
    </div>
  );
}

function SettingsView({ sendNotifications, setSendNotifications, resumeUploaded, refreshAll }) {
  return (
    <div className="page">
      <PageHeader eyebrow="Settings" title="Application settings" />
      <section className="panel panel-pad settings-grid">
        <div className="setting-row">
          <div>
            <div className="section-title" style={{ margin: 0 }}>
              ntfy notifications
            </div>
            <div className="muted" style={{ fontSize: 13, marginTop: 4 }}>
              Topic: team7-jobagent-2026
            </div>
          </div>
          <label className="switch">
            <input
              type="checkbox"
              checked={sendNotifications}
              onChange={(event) => setSendNotifications(event.target.checked)}
            />
            <span className="slider" />
          </label>
        </div>

        <div className="setting-row">
          <div>
            <div className="section-title" style={{ margin: 0 }}>
              API endpoint
            </div>
            <div className="muted" style={{ fontSize: 13, marginTop: 4 }}>
              {API}
            </div>
          </div>
          <button className="secondary-button" type="button" onClick={refreshAll}>
            Refresh
          </button>
        </div>

        <div className="setting-row" style={{ borderBottom: 0 }}>
          <div>
            <div className="section-title" style={{ margin: 0 }}>
              Resume profile
            </div>
            <div className="muted" style={{ fontSize: 13, marginTop: 4 }}>
              {resumeUploaded ? "Uploaded resume is used for scoring." : "Fallback profile is used for scoring."}
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

export default function App() {
  const [activeView, setActiveView] = useState("dashboard");
  const [jobs, setJobs] = useState([]);
  const [stats, setStats] = useState(null);
  const [resume, setResume] = useState(null);
  const [resumeFile, setResumeFile] = useState(null);
  const [url, setUrl] = useState("");
  const [searchKeywords, setSearchKeywords] = useState("");
  const [searchLocation, setSearchLocation] = useState("");
  const [filter, setFilter] = useState("all");
  const [loading, setLoading] = useState(false);
  const [scoring, setScoring] = useState(false);
  const [batching, setBatching] = useState(false);
  const [batchProgress, setBatchProgress] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [sendNotifications, setSendNotifications] = useState(true);
  const [error, setError] = useState("");
  const [uploadError, setUploadError] = useState("");
  const [uploadMessage, setUploadMessage] = useState("");

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const [jobsRes, statsRes] = await Promise.all([fetch(`${API}/jobs`), fetch(`${API}/stats`)]);
      if (!jobsRes.ok) throw new Error(await getErrorMessage(jobsRes));
      if (!statsRes.ok) throw new Error(await getErrorMessage(statsRes));
      setJobs(await jobsRes.json());
      setStats(await statsRes.json());
    } catch (caught) {
      setError(`Backend error: ${caught.message}`);
    } finally {
      setLoading(false);
    }
  };

  const fetchResume = async () => {
    try {
      const response = await fetch(`${API}/resume`);
      if (response.ok) setResume(await response.json());
    } catch {
      setResume({ uploaded: false });
    }
  };

  const refreshAll = async () => {
    await Promise.all([fetchJobs(), fetchResume()]);
  };

  useEffect(() => {
    refreshAll();
  }, []);

  const handleScore = async () => {
    if (!url.trim()) return;
    setScoring(true);
    setError("");
    try {
      const response = await fetch(`${API}/jobs/score`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: url.trim(), send_sms: sendNotifications }),
      });
      if (!response.ok) throw new Error(await getErrorMessage(response));
      setUrl("");
      await fetchJobs();
    } catch (caught) {
      setError(`Scoring failed: ${caught.message}`);
    } finally {
      setScoring(false);
    }
  };

  const handleBatchScore = async () => {
    if (!searchKeywords.trim() || !searchLocation.trim()) return;
    setBatching(true);
    setError("");
    setBatchProgress({ status: "queued", current: 0, total: 0, message: "Finding jobs..." });

    try {
      const start = await fetch(`${API}/jobs/search-score`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          keywords: searchKeywords.trim(),
          location: searchLocation.trim(),
          send_sms: sendNotifications,
        }),
      });
      if (!start.ok) throw new Error(await getErrorMessage(start));
      let state = await start.json();
      setBatchProgress(state);

      while (!["complete", "failed"].includes(state.status)) {
        await new Promise((resolve) => setTimeout(resolve, 2000));
        const poll = await fetch(`${API}/jobs/batch-status?batch_id=${encodeURIComponent(state.id)}`);
        if (!poll.ok) throw new Error(await getErrorMessage(poll));
        state = await poll.json();
        setBatchProgress(state);
      }

      if (state.status === "failed") throw new Error(state.message || "Batch scoring failed.");
      setSearchKeywords("");
      setSearchLocation("");
      await fetchJobs();
    } catch (caught) {
      setBatchProgress((current) => ({
        ...(current || {}),
        status: "failed",
        message: caught.message,
      }));
    } finally {
      setBatching(false);
    }
  };

  const handleResumeUpload = async () => {
    if (!resumeFile) return;
    setUploading(true);
    setUploadError("");
    setUploadMessage("");

    try {
      const response = await fetch(`${API}/resume/upload`, {
        method: "POST",
        headers: {
          "Content-Type": resumeFile.type || "application/pdf",
          "X-Filename": encodeURIComponent(resumeFile.name),
        },
        body: resumeFile,
      });
      if (!response.ok) throw new Error(await getErrorMessage(response));
      const data = await response.json();
      setUploadMessage(data.message || "Resume uploaded ✅");
      setResume({ ...data, uploaded: true });
      setResumeFile(null);
      await fetchResume();
    } catch (caught) {
      setUploadError(`Upload failed: ${caught.message}`);
    } finally {
      setUploading(false);
    }
  };

  const handleStatusChange = async (jobId, status) => {
    setJobs((current) => current.map((job) => (job.id === jobId ? { ...job, status } : job)));
    const response = await fetch(`${API}/jobs/${jobId}/status`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });
    if (!response.ok) {
      setError(`Status update failed: ${await getErrorMessage(response)}`);
      await fetchJobs();
    }
  };

  const resumeUploaded = Boolean(resume?.uploaded);

  return (
    <>
      <GlobalStyles />
      <div className="app-shell">
        <Sidebar activeView={activeView} onViewChange={setActiveView} resumeUploaded={resumeUploaded} />
        <main className="main">
          {activeView === "dashboard" && (
            <DashboardView
              jobs={jobs}
              stats={stats}
              loading={loading}
              error={error}
              filter={filter}
              setFilter={setFilter}
              url={url}
              setUrl={setUrl}
              searchKeywords={searchKeywords}
              setSearchKeywords={setSearchKeywords}
              searchLocation={searchLocation}
              setSearchLocation={setSearchLocation}
              scoring={scoring}
              batching={batching}
              batchProgress={batchProgress}
              sendNotifications={sendNotifications}
              setSendNotifications={setSendNotifications}
              onScore={handleScore}
              onBatchScore={handleBatchScore}
              onStatusChange={handleStatusChange}
            />
          )}
          {activeView === "resume" && (
            <ResumeView
              resume={resume}
              resumeFile={resumeFile}
              setResumeFile={setResumeFile}
              uploading={uploading}
              uploadMessage={uploadMessage}
              uploadError={uploadError}
              onUpload={handleResumeUpload}
            />
          )}
          {activeView === "settings" && (
            <SettingsView
              sendNotifications={sendNotifications}
              setSendNotifications={setSendNotifications}
              resumeUploaded={resumeUploaded}
              refreshAll={refreshAll}
            />
          )}
        </main>
      </div>
    </>
  );
}
