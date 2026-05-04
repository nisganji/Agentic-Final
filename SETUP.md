# JobAgent — Complete Setup Guide

# From zero to live in ~45 minutes

## What you're building

A real agentic system that:

1.  Scrapes any job URL (via Jina.ai — free)
2.  Scores it 0–100 using Gemini AI (free)
3.  Logs everything to Supabase (free)
4.  Sends push notifications via ntfy (free)
5.  Reminds you to follow up automatically

* * *

## STEP 1 — Get your API keys

### 1a. Gemini API — COMPLETELY FREE

1.  Go to aistudio.google.com
2.  Sign in with your Google account
3.  Click "Get API Key" → "Create API key"
4.  Copy the key (starts with AIza...)
5.  Free tier: 15 requests/min, 1,500/day — more than enough

### 1b. Supabase (database) — COMPLETELY FREE

1.  Go to supabase.com → "Start your project"
2.  Sign up with GitHub
3.  "New project" → name it "jobagent" → pick a password → region: US East
4.  Wait ~2 min for it to spin up
5.  Settings → API → copy:
    -   "Project URL" (looks like [https://xxxx.supabase.co](https://xxxx.supabase.co))
    -   "service\_role" key under "Project API keys" (NOT the anon key)
6.  Go to SQL Editor → paste contents of schema.sql → Run

### 1c. ntfy — COMPLETELY FREE, no account needed

1.  Install the ntfy app on your phone:
    -   iPhone: search "ntfy" on the App Store
    -   Android: search "ntfy" on the Play Store
2.  Open the app → tap "+" → Subscribe to topic
3.  Type: team7-jobagent-2026
4.  Tap Subscribe — done!

No scraping API needed — Jina.ai works with zero signup.

* * *

## STEP 2 — Your .env file

Create a file called .env in your project folder:

```
GEMINI_API_KEY=AIza...
SUPABASE_URL=https://fvbawabjschufqvdhhzo.supabase.co
SUPABASE_KEY=eyJ...
NTFY_TOPIC=team7-jobagent-2026
```

* * *

## STEP 3 — Set environment variables in PowerShell

Run these every time you open a new PowerShell window:

```powershell
$env:GEMINI_API_KEY="AIza..."
$env:SUPABASE_URL="https://fvbawabjschufqvdhhzo.supabase.co"
$env:SUPABASE_KEY="eyJ..."
$env:NTFY_TOPIC="team7-jobagent-2026"
```

* * *

## STEP 4 — Install dependencies and run

```powershell
pip install google-generativeai requests fastapi uvicorn
```

Start the backend:

```powershell
python -m uvicorn server:app --reload --port 8000
```

Test with a job URL (open a second PowerShell, set $env: vars again):

```powershell
python agent.py "https://www.linkedin.com/jobs/view/XXXXXXX"
```

You should see:  
\[1/4\] Scraping...  
\[2/4\] Scoring with Gemini...  
\[3/4\] Saving to database...  
\[4/4\] Sending push notification...  
✅ Done! Score: 74/100 — Apply This Weekend

* * *

## STEP 5 — Run the frontend locally

```powershell
cd frontend
npm install
echo "VITE_API_URL=http://localhost:8000" > .env
npm run dev
```

Open [http://localhost:5173](http://localhost:5173) — paste a job URL — watch it score live.

* * *

## STEP 6 — Deploy (so it works from your phone anywhere)

### Deploy backend → Railway (free)

1.  Go to railway.app → sign up with GitHub
2.  "New Project" → "Deploy from GitHub repo"
3.  Select your repo → pick your project folder
4.  Add environment variables in Railway's Variables tab:
    -   GEMINI\_API\_KEY
    -   SUPABASE\_URL
    -   SUPABASE\_KEY
    -   NTFY\_TOPIC
5.  Railway gives you a public URL like [https://jobagent-backend.up.railway.app](https://jobagent-backend.up.railway.app)

### Deploy frontend → Vercel (free)

1.  Go to vercel.com → sign up with GitHub
2.  "New Project" → import your repo → set Root Directory to frontend folder
3.  Add: VITE\_API\_URL = your Railway backend URL
4.  Deploy → get a URL like [https://jobagent.vercel.app](https://jobagent.vercel.app)

* * *

## STEP 7 — GitHub Actions for daily follow-up reminders

Update .github/workflows/followups.yml env block:

```yaml
- name: Install dependencies
  run: pip install google-generativeai requests

- name: Run follow-up checker
  env:
    GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}
    SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
    SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
    NTFY_TOPIC: ${{ secrets.NTFY_TOPIC }}
  run: python agent.py --check-followups
```

Add these 4 keys as GitHub Repository Secrets.

* * *

## Demo script for your presentation

1.  Open jobagent.vercel.app on your laptop
2.  Paste a REAL job URL live
3.  Hit "Score it" — ~10-15 seconds
4.  Show the score appear on dashboard
5.  Hold up your phone — show the ntfy push notification arriving live
6.  Tap the notification — it opens the job posting directly
7.  Show score breakdown: skill match, visa friendliness
8.  Show a "Skip" job with red flags
9.  Show applied jobs with follow-up dates
10.  Pull up GitHub Actions — show the cron run

Total demo time: ~4 minutes. Leaves 6 minutes for architecture + business case.

* * *

## API cost summary

Service

Cost

Gemini 1.5 Flash

FREE (1,500 req/day)

Jina.ai scraping

FREE (unlimited)

Supabase

FREE

ntfy notifications

FREE

**Total**

**$0**

* * *

## Customizing your scoring profile

Edit CANDIDATE\_PROFILE in agent.py. Change:

-   Your degree/graduation date
-   Specific skills
-   Target cities
-   Salary floor

The more specific it is, the more accurate the scoring.