import os
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from supabase import create_client

# ================= CONFIGURATION =================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Search parameters – adjust as needed
SEARCH_TERMS = [
    "fresher software engineer",
    "graduate engineer trainee",
    "entry level developer",
    "fresher data analyst",
    "trainee engineer",
    "fresher devops",
    "fresher cybersecurity",
    "fresher qa"
]
CITIES = ["Hyderabad", "Bangalore", "Chennai", "Pune", "Mumbai", "Noida"]
DAYS_BACK = 3                      # Only jobs from last 3 days
RESULTS_PER_PAGE = 20              # Number of results per API call (free tier friendly)

# Keyword sets
FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto", "staff"}

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ================= HELPER FUNCTIONS =================
def is_fresher_job(title: str, description: str) -> bool:
    """Return True if job is for fresher (not senior)."""
    text = (title + " " + description).lower()
    has_fresher = any(word in text for word in FRESHER_WORDS)
    has_senior = any(word in text for word in SENIOR_WORDS)
    return has_fresher and not has_senior

async def fetch_jobs(session: aiohttp.ClientSession, query: str, city: str) -> list:
    """Fetch one page of jobs from JSearch API."""
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} {city}",
        "page": 1,
        "num_pages": 1,
        "country": "in",
        "date_posted": "week",
        "results_per_page": RESULTS_PER_PAGE
    }
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    # Strip any leftover whitespace/newline characters
    headers = {k: v.replace('\n', '').replace('\r', '').strip() for k, v in headers.items()}
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as resp:
            if resp.status != 200:
                print(f"API error {resp.status} for {query} in {city}")
                return []
            data = await resp.json()
            return data.get("data", [])
    except Exception as e:
        print(f"Request failed for {query} in {city}: {e}")
        return []

async def send_telegram_message(session: aiohttp.ClientSession, text: str):
    """Send a message to the configured Telegram chat."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing – skipping notification.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        await session.post(url, json=payload)
    except Exception as e:
        print(f"Telegram send error: {e}")

# ================= MAIN WORKFLOW =================
async def main():
    print("🚀 ApplyMore – parallel job scraper started")
    start_time = datetime.now(timezone.utc)

    # 1. Fetch existing URLs from DB (to avoid duplicates)
    existing_response = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_response.data} if existing_response.data else set()
    print(f"📊 Existing jobs in DB: {len(existing_urls)}")

    # 2. Prepare all API calls (parallel)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for term in SEARCH_TERMS:
            for city in CITIES:
                tasks.append(fetch_jobs(session, term, city))
        # Run all requests concurrently
        all_results = await asyncio.gather(*tasks)

    # 3. Process results
    seen_urls = set()
    new_jobs = []
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    for job_list in all_results:
        for job in job_list:
            title = job.get("job_title")
            company = job.get("employer_name")
            url = job.get("job_apply_link")
            description = job.get("job_description", "")
            posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")

            # Basic validation
            if not title or not company or not url:
                continue

            # Duplicate check
            if url in existing_urls or url in seen_urls:
                continue

            # Date filter (if date is available)
            if posted_str:
                try:
                    posted_dt = datetime.fromisoformat(posted_str.replace('Z', '+00:00'))
                    if posted_dt < cutoff_date:
                        continue
                except:
                    pass  # If date parsing fails, keep the job (better safe than sorry)

            # Fresher filter
            if not is_fresher_job(title, description):
                continue

            # All checks passed – add to new jobs list
            new_jobs.append({
                "title": title,
                "company": company,
                "location": "India",   # City extraction would require extra parsing; keep simple
                "url": url,
                "description": description,
                "posted_date": posted_str or datetime.now(timezone.utc).isoformat(),
                "created_at": datetime.now(timezone.utc).isoformat()
            })
            seen_urls.add(url)

    # 4. Insert new jobs into Supabase (batch)
    print(f"✨ New fresher jobs found: {len(new_jobs)}")
    if new_jobs:
        for i in range(0, len(new_jobs), 50):
            batch = new_jobs[i:i+50]
            supabase.table("ApplyMore").insert(batch).execute()
            print(f"   Inserted batch {i//50 + 1} ({len(batch)} jobs)")

        # 5. Send Telegram alert with clickable job list
        async with aiohttp.ClientSession() as session:
            # Build message (first 10 jobs, rest summary)
            lines = [f"✅ <b>ApplyMore – {len(new_jobs)} new fresher jobs</b>\n"]
            for idx, job in enumerate(new_jobs[:10], 1):
                lines.append(
                    f"{idx}. <b>{job['title']}</b>\n"
                    f"   🏢 {job['company']}\n"
                    f"   🔗 <a href='{job['url']}'>Apply now</a>"
                )
            if len(new_jobs) > 10:
                lines.append(f"\n... and {len(new_jobs)-10} more. <a href='https://applymore.vercel.app'>View all on ApplyMore</a>")
            else:
                lines.append(f"\n🌐 <a href='https://applymore.vercel.app'>Browse all jobs</a>")
            await send_telegram_message(session, "\n\n".join(lines))
    else:
        print("ℹ️ No new jobs to insert.")
        async with aiohttp.ClientSession() as session:
            await send_telegram_message(session, "⚠️ ApplyMore scraper ran but found no new fresher jobs.")

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"✅ Finished in {elapsed:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
