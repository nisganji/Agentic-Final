-- Run this once in your Supabase SQL editor
-- Dashboard: supabase.com → your project → SQL Editor

create table if not exists jobs (
  id              bigserial primary key,
  url             text not null,
  company         text,
  role            text,
  location        text,
  overall_score   integer,
  skill_match     integer,
  visa_friendliness integer,
  seniority_fit   integer,
  company_quality integer,
  verdict         text,
  summary         text,
  red_flags       text,   -- stored as JSON string
  green_flags     text,   -- stored as JSON string
  salary_range    text,
  sponsors_visa   text,
  status          text default 'Scored',
  scored_at       timestamptz default now(),
  followup_at     timestamptz,
  followup_days   integer default 7,
  created_at      timestamptz default now()
);

-- Enable Row Level Security (keep data private)
alter table jobs enable row level security;

-- Allow full access via service role key (your backend uses this)
drop policy if exists "service role full access" on jobs;

create policy "service role full access" on jobs
  for all using (true);

create table if not exists resume (
  id           integer primary key default 1,
  filename     text,
  parsed_text  text not null,
  uploaded_at  timestamptz default now(),
  updated_at   timestamptz default now(),
  created_at   timestamptz default now(),
  constraint single_resume_row check (id = 1)
);

alter table resume enable row level security;

drop policy if exists "service role full access" on resume;

create policy "service role full access" on resume
  for all using (true);
