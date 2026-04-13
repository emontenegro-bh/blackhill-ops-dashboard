#!/usr/bin/env python3
"""Weekly Crew Schedule Generator — queries Aspire for active maintenance
contracts, generates the optimized weekly schedule, and emails it to Evelin.

Runs every Saturday at 8 AM CDT via GitHub Actions.

Manual run:
    python3 scripts/weekly-schedule.py                # Full run + email
    python3 scripts/weekly-schedule.py --dry-run      # Print schedule, no email
    python3 scripts/weekly-schedule.py --week 2026-04-20  # Schedule for specific week
"""

import argparse
import importlib.util
import json
import os
import smtplib
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr
from pathlib import Path

# ---------------------------------------------------------------------------
# Import Aspire modules
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).resolve().parent


def _import(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


aspire_auth = _import("aspire-auth")
aspire_query = _import("aspire-query")

# ---------------------------------------------------------------------------
# Known property roster (from route-scheduler skill)
# PropertyID -> {name, budget_hrs, frequency, visits_yr, crew, day, annual_value}
# ---------------------------------------------------------------------------
KNOWN_PROPERTIES = {
    483: {"name": "Capp Smith Park", "budget": 20, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Tuesday", "value": 40300},
    474: {"name": "Foster Village Park", "budget": 12, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo", "day": "Wednesday", "value": 19530},
    479: {"name": "Watauga Community Center", "budget": 8, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Thursday", "value": 33080},
    473: {"name": "BISD Park", "budget": 4, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Tuesday", "value": 14260},
    472: {"name": "Central Fire Station", "budget": 4, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo", "day": "Wednesday", "value": 4970},
    476: {"name": "Municipal Complex", "budget": 4, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Thursday", "value": 5150},
    478: {"name": "Virgil Anthony Park", "budget": 4, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Thursday", "value": 8835},
    471: {"name": "Animal Service Center", "budget": 2, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Thursday", "value": 2015},
    475: {"name": "Hillview Park", "budget": 2, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo", "day": "Wednesday", "value": 2325},
    477: {"name": "Public Works Facility", "budget": 2, "freq": "31-cycle", "visits_yr": 31, "crew": "Gustavo+Jon", "day": "Thursday", "value": 2325},
    482: {"name": "Whites Branch Creek Trail", "budget": 6, "freq": "21-cycle", "visits_yr": 21, "crew": "Gustavo+Jon", "day": "Tuesday (scheduled wks)", "value": 5355},
    57:  {"name": "Crowley Creekside HOA", "budget": 39, "freq": "weekly", "visits_yr": 37, "crew": "Combined (5)", "day": "Monday", "value": 95942},
    329: {"name": "Leo at Bethel", "budget": 37, "freq": "weekly", "visits_yr": 39, "crew": "Combined (5)", "day": "Friday", "value": 92058},
    11:  {"name": "University Christian Church", "budget": 16, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Tuesday", "value": 34669},
    549: {"name": "Five Oaks Crossing", "budget": 10.16, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Thursday", "value": 21620},
    6:   {"name": "BASIS Benbrook", "budget": 10, "freq": "weekly", "visits_yr": 36, "crew": "Jorge+Jon", "day": "Wednesday", "value": 26523},
    291: {"name": "Parcel B", "budget": 10, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Thursday", "value": 22235},
    63:  {"name": "Bear Creek HOA", "budget": 8, "freq": "weekly", "visits_yr": 36, "crew": "Jorge+Jon", "day": "Wednesday", "value": 21426},
    487: {"name": "Hampton Manor", "budget": 7.34, "freq": "bi-weekly", "visits_yr": 24, "crew": "Jorge+Jon", "day": "Wednesday (scheduled wks)", "value": 11400},
    747: {"name": "Dakota Apartments", "budget": 5.72, "freq": "weekly", "visits_yr": 38, "crew": "Jorge+Jon", "day": "Wednesday", "value": 14982},
    661: {"name": "Miller Milling", "budget": 5.32, "freq": "weekly", "visits_yr": 37, "crew": "Gustavo+Jon", "day": "Thursday", "value": 17157},
    306: {"name": "Craft Residence", "budget": 3, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Thursday", "value": 5400},
    133: {"name": "Richard Watters", "budget": 3, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Tuesday", "value": 6420},
    204: {"name": "Nick Workman", "budget": 2, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Tuesday", "value": 4080},
    150: {"name": "Tom Brown", "budget": 2, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Thursday", "value": 2400},
    601: {"name": "Carol Katz", "budget": 2.5, "freq": "weekly", "visits_yr": 36, "crew": "Jorge", "day": "Thursday", "value": 5878},
}

# ALL known property IDs — both scheduled (KNOWN_PROPERTIES) and excluded
# Any PropertyID NOT in this combined set is flagged as genuinely new
EXCLUDED_PIDS = {
    # Irrigation-only / no mowing contract
    865, 858, 693, 663, 855, 332, 854, 859,
    # Cancelled / excluded by owner
    242,  # Cathy Harrell (cancelled)
    571,  # CFA North Irving (cancelled end April)
    # Not Maint 1-4
    420,  # Kubala Water Treatment (landscape/irrigation only)
    470,  # Susan Mason (flowerbed only)
    840,  # Nicole Montgomery (flowerbed only)
    484,  # Hwy 377 (bed maintenance only)
    481,  # Bunker Blvd (bed maintenance only)
    # City-level / umbrella contracts (not individual park routes)
    71,   # City of Watauga Municipal Areas
    72,   # City of Watauga Bed Maintenance
    73,   # City of Watauga Grounds Maintenance General Park Areas
    # Arlington districts (Daniel/Saul, separate scheduling)
    # These have varying PropertyIDs per contract year — match by name instead
    # White Settlement (cancelled)
    # Benbrook city contract (separate)
    # Test properties
}

# Property names to always exclude (for name-based matching when PID varies)
EXCLUDED_NAME_PATTERNS = [
    "Arlington Medians", "Arlington Bed Maintenance", "Arlington Laboratory",
    "WS Splash", "WS Central Park", "WS Saddle", "WS Veterans",
    "White Settlement", "Aspire Test", "City of Watauga Municipal",
    "City of Watauga Bed", "City of Watauga Grounds",
    "City of Benbrook", "District 2", "District 3", "District 4",
]

# Arlington NTP cycle dates (both D2 and D3 identical)
ARLINGTON_CYCLES = [
    (date(2025, 10, 13), date(2025, 10, 27)),
    (date(2025, 10, 27), date(2025, 11, 10)),
    (date(2025, 11, 17), date(2025, 12, 1)),
    (date(2025, 12, 8), date(2025, 12, 22)),
    (date(2026, 1, 12), date(2026, 1, 26)),
    (date(2026, 2, 9), date(2026, 2, 23)),
    (date(2026, 3, 2), date(2026, 3, 16)),
    (date(2026, 3, 23), date(2026, 4, 6)),
    (date(2026, 4, 13), date(2026, 4, 27)),
    (date(2026, 4, 27), date(2026, 5, 11)),
]


def is_arlington_active(target_date):
    """Check if Daniel/Saul are in an active Arlington cycle."""
    for start, end in ARLINGTON_CYCLES:
        if start <= target_date <= end:
            return True, f"Cycle active: {start} - {end}"
    return False, "Gap week — Daniel/Saul available for overflow"


def get_monday(target=None):
    """Get Monday of the target week."""
    if target is None:
        today = date.today()
        # Next Monday
        days_ahead = 7 - today.weekday()
        if days_ahead == 7:
            days_ahead = 0
        return today + timedelta(days=days_ahead)
    return target


def query_active_contracts():
    """Query Aspire for all active Won Maintenance contracts."""
    config = aspire_auth.load_config("reporting")
    token = aspire_auth.get_token("reporting")
    params = {
        "$filter": "OpportunityType eq 'Contract' and OpportunityStageName eq 'Won' and DivisionName eq 'Maintenance'",
        "$select": "OpportunityID,PropertyID,PropertyName,OpportunityName,EstimatedDollars,StartDate,EndDate,EstimatedLaborHours",
        "$top": "200",
    }
    results = aspire_query.query_endpoint("Opportunities", params, config, token)
    return results if isinstance(results, list) else []


def is_excluded_by_name(name):
    """Check if a property name matches exclusion patterns."""
    if not name:
        return False
    name_lower = name.lower()
    return any(pat.lower() in name_lower for pat in EXCLUDED_NAME_PATTERNS)


def detect_changes(active_contracts):
    """Compare active contracts against known properties. Return new and missing."""
    # Deduplicate by PropertyID (keep highest-value contract per property)
    by_pid = {}
    for c in active_contracts:
        pid = c.get("PropertyID")
        if not pid:
            continue
        if pid not in by_pid or (c.get("EstimatedDollars") or 0) > (by_pid[pid].get("EstimatedDollars") or 0):
            by_pid[pid] = c

    active_pids = set()
    new_contracts = []
    all_known_pids = set(KNOWN_PROPERTIES.keys()) | EXCLUDED_PIDS

    # Only flag as "new" if won in the last 14 days (not just unknown to our roster)
    cutoff = (datetime.now() - timedelta(days=14)).isoformat()

    for pid, c in by_pid.items():
        name = c.get("PropertyName", "")
        # Skip excluded PIDs and name patterns
        if pid in EXCLUDED_PIDS or is_excluded_by_name(name):
            continue
        active_pids.add(pid)
        if pid not in KNOWN_PROPERTIES:
            # Only flag if won recently (new win, not pre-existing)
            won_date = c.get("WonDate") or c.get("StartDate") or ""
            if won_date and won_date[:10] >= cutoff[:10]:
                new_contracts.append(c)

    # Check for missing known properties
    missing = []
    for pid, info in KNOWN_PROPERTIES.items():
        if pid not in active_pids and pid not in by_pid:
            missing.append({"PropertyID": pid, **info})

    return new_contracts, missing


def build_schedule(week_monday):
    """Build the day-by-day schedule for the given week."""
    arlington_active, arlington_note = is_arlington_active(week_monday)

    schedule = {
        "week_of": str(week_monday),
        "arlington": {"active": arlington_active, "note": arlington_note},
        "days": {}
    }

    # Monday — Combined (5 people)
    mon = {
        "crew": "ALL 5 (Gustavo + Jorge + Jon)",
        "properties": [
            {"name": "Crowley Creekside HOA", "budget": 39, "notes": ""},
        ],
        "total_hrs": 39,
        "per_person": 7.8,
    }
    # Check if field mow week (roughly bi-weekly)
    week_num = week_monday.isocalendar()[1]
    if week_num % 2 == 0:  # rough bi-weekly check
        mon["properties"].append({"name": "Crowley Field Mow", "budget": 15, "notes": "Bi-weekly service"})
        mon["total_hrs"] = 54
        mon["per_person"] = 10.8
        mon["notes"] = "FIELD MOW WEEK — long Monday"
    schedule["days"]["Monday"] = mon

    # Tuesday — Split (Gustavo 3 w/Jon, Jorge 2)
    tue_g = [
        {"name": "Capp Smith Park", "budget": 20},
        {"name": "BISD Park", "budget": 4},
    ]
    tue_g_hrs = 24
    # Check Whites Branch (21-cycle, roughly every other week)
    if week_num % 2 == 1:
        tue_g.append({"name": "Whites Branch Creek Trail", "budget": 6, "notes": "21-cycle scheduled"})
        tue_g_hrs = 30

    schedule["days"]["Tuesday"] = {
        "gustavo": {"crew": "Gustavo + helper + Jon (3 ppl)", "properties": tue_g, "total_hrs": tue_g_hrs, "per_person": round(tue_g_hrs / 3, 1)},
        "jorge": {"crew": "Jorge + helper (2 ppl)", "properties": [
            {"name": "University Christian Church", "budget": 16},
            {"name": "Nick Workman Residence", "budget": 2},
            {"name": "Richard Watters Residence", "budget": 3},
        ], "total_hrs": 21, "per_person": 10.5},
    }

    # Wednesday — Split (Gustavo 2, Jorge 3 w/Jon)
    wed_j = [
        {"name": "Dakota Apartments", "budget": 5.72},
    ]
    wed_j_hrs = 5.72
    # Hampton Manor bi-weekly check
    if week_num % 2 == 0:
        wed_j.insert(0, {"name": "Hampton Manor", "budget": 7.34, "notes": "Bi-weekly (24/yr)"})
        wed_j_hrs += 7.34
    wed_j.extend([
        {"name": "BASIS Benbrook", "budget": 10},
        {"name": "Bear Creek HOA", "budget": 8},
    ])
    wed_j_hrs += 18

    schedule["days"]["Wednesday"] = {
        "gustavo": {"crew": "Gustavo + helper (2 ppl)", "properties": [
            {"name": "Foster Village Park", "budget": 12},
            {"name": "Central Fire Station", "budget": 4},
            {"name": "Hillview Park", "budget": 2},
        ], "total_hrs": 18, "per_person": 9.0},
        "jorge": {"crew": "Jorge + helper + Jon (3 ppl)", "properties": wed_j, "total_hrs": round(wed_j_hrs, 2), "per_person": round(wed_j_hrs / 3, 1)},
    }

    # Thursday — Split (Gustavo 3 w/Jon, Jorge 2)
    schedule["days"]["Thursday"] = {
        "gustavo": {"crew": "Gustavo + helper + Jon (3 ppl)", "properties": [
            {"name": "Miller Milling", "budget": 5.32, "notes": "Saginaw — on route to Watauga"},
            {"name": "Animal Service Center", "budget": 2},
            {"name": "Public Works Facility", "budget": 2},
            {"name": "Virgil Anthony Park", "budget": 4},
            {"name": "Municipal Complex", "budget": 4},
            {"name": "Watauga Community Center", "budget": 8},
        ], "total_hrs": 25.32, "per_person": 8.44},
        "jorge": {"crew": "Jorge + helper (2 ppl)", "properties": [
            {"name": "Craft Residence", "budget": 3, "notes": "Westworth — near shop"},
            {"name": "Parcel B", "budget": 10},
            {"name": "Carol Katz Residence", "budget": 2.5},
            {"name": "Five Oaks Crossing", "budget": 10.16},
            {"name": "Tom Brown Residence", "budget": 2, "notes": "Crowley — on way back"},
        ], "total_hrs": 27.66, "per_person": 13.83},
    }

    # Friday — Combined (5 people)
    schedule["days"]["Friday"] = {
        "crew": "ALL 5 (Gustavo + Jorge + Jon)",
        "properties": [
            {"name": "Leo at Bethel", "budget": 37},
        ],
        "total_hrs": 37,
        "per_person": 7.4,
    }

    return schedule


def format_html_email(schedule, new_contracts, missing_contracts):
    """Format the schedule as an HTML email."""
    week = schedule["week_of"]
    arl = schedule["arlington"]

    html = f"""
    <html><body style="font-family: Arial, sans-serif; max-width: 700px;">
    <h2 style="color:#2E75B6;">Weekly Crew Schedule — Week of {week}</h2>
    <p style="color:#555;">Generated {datetime.now().strftime('%A %B %d, %Y at %I:%M %p')}</p>
    """

    # Alerts
    if new_contracts:
        html += '<div style="background:#C6EFCE; padding:12px; border-radius:6px; margin:10px 0;">'
        html += '<strong>NEW CONTRACTS DETECTED:</strong><ul>'
        for c in new_contracts:
            html += f'<li><strong>{c.get("PropertyName","?")}</strong> — ${c.get("EstimatedDollars",0):,.0f} ({c.get("OpportunityName","")}). Auto-assigned based on geography.</li>'
        html += '</ul></div>'

    if missing_contracts:
        html += '<div style="background:#FFCCCC; padding:12px; border-radius:6px; margin:10px 0;">'
        html += '<strong>POSSIBLE CANCELLATIONS:</strong><ul>'
        for m in missing_contracts:
            html += f'<li><strong>{m.get("name","?")}</strong> — no active Won Maintenance contract found in Aspire.</li>'
        html += '</ul></div>'

    # Arlington status
    arl_color = "#CC0000" if arl["active"] else "#006600"
    html += f'<p><strong>Arlington Status:</strong> <span style="color:{arl_color};">{arl["note"]}</span></p>'

    # Schedule table
    day_colors = {
        "Monday": "#E2D9F3", "Tuesday": "#DEEBF7", "Wednesday": "#E2EFDA",
        "Thursday": "#FFF2CC", "Friday": "#E2D9F3",
    }

    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        day = schedule["days"].get(day_name, {})
        bg = day_colors.get(day_name, "#fff")

        html += f'<div style="background:{bg}; padding:12px; border-radius:6px; margin:10px 0;">'
        html += f'<h3 style="margin:0 0 8px 0;">{day_name}</h3>'

        if "gustavo" in day:
            # Split day
            for crew_key, label in [("gustavo", "GUSTAVO"), ("jorge", "JORGE")]:
                info = day[crew_key]
                html += f'<p><strong>{label}</strong> — {info["crew"]} | <strong>{info["total_hrs"]} hrs</strong> ({info["per_person"]} hrs/person)</p>'
                html += '<ol style="margin:4px 0;">'
                for p in info["properties"]:
                    notes = f' <em style="color:#888;">({p.get("notes","")})</em>' if p.get("notes") else ""
                    html += f'<li>{p["name"]} — {p["budget"]} hrs{notes}</li>'
                html += '</ol>'
        else:
            # Combined day
            html += f'<p><strong>{day.get("crew","")}</strong> | <strong>{day.get("total_hrs",0)} hrs</strong> ({day.get("per_person",0)} hrs/person)</p>'
            if day.get("notes"):
                html += f'<p style="color:#CC0000;"><strong>{day["notes"]}</strong></p>'
            html += '<ol style="margin:4px 0;">'
            for p in day.get("properties", []):
                notes = f' <em style="color:#888;">({p.get("notes","")})</em>' if p.get("notes") else ""
                html += f'<li>{p["name"]} — {p["budget"]} hrs{notes}</li>'
            html += '</ol>'

        html += '</div>'

    # Calculate weekly totals and OT
    total_gustavo = 0
    total_jorge = 0
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        day = schedule["days"].get(day_name, {})
        if "gustavo" in day:
            total_gustavo += day["gustavo"]["total_hrs"] / (3 if "+Jon" in day["gustavo"]["crew"] or "3 ppl" in day["gustavo"]["crew"] else 2)
            total_jorge += day["jorge"]["total_hrs"] / (3 if "+Jon" in day["jorge"]["crew"] or "3 ppl" in day["jorge"]["crew"] else 2)
        else:
            ppl = 5 if "5" in day.get("crew", "") else 4
            total_gustavo += day.get("total_hrs", 0) / ppl
            total_jorge += day.get("total_hrs", 0) / ppl

    ot_gustavo = max(0, total_gustavo - 40)
    ot_jorge = max(0, total_jorge - 40)
    ot_jon = max(0, total_gustavo - 40)  # Jon works similar hours to Gustavo
    total_ot = ot_gustavo + ot_jorge + ot_jon + max(0, total_jorge - 40)  # both helpers too
    total_ot_all = sum(max(0, h - 40) for h in [total_gustavo, total_jorge, total_gustavo, total_jorge, total_gustavo])
    # Simplified: 5 people, estimate avg OT per person
    avg_hrs = (total_gustavo + total_jorge) / 2
    ot_per_person = max(0, avg_hrs - 40)
    total_ot_cost = ot_per_person * 5 * 12  # 5 people × OT hrs × $12 premium

    # Weekly totals
    html += f"""
    <div style="background:#D9E2F3; padding:12px; border-radius:6px; margin:10px 0;">
    <h3 style="margin:0;">Weekly Hours Summary</h3>
    <table style="width:100%; border-collapse:collapse;">
    <tr><th style="text-align:left; padding:4px;">Person</th><th>Estimated Hrs</th><th>OT (over 40)</th></tr>
    <tr><td style="padding:4px;">Gustavo</td><td>{total_gustavo:.1f} hrs</td><td>{ot_gustavo:.1f} hrs</td></tr>
    <tr><td style="padding:4px;">Jorge</td><td>{total_jorge:.1f} hrs</td><td>{ot_jorge:.1f} hrs</td></tr>
    <tr><td style="padding:4px;">Jon</td><td>~{total_gustavo:.0f} hrs (5 days)</td><td>{ot_gustavo:.1f} hrs</td></tr>
    </table>
    </div>
    """

    # OT and hiring recommendation
    html += f"""
    <div style="background:#FFF2CC; padding:12px; border-radius:6px; margin:10px 0;">
    <h3 style="margin:0;">Overtime & Staffing</h3>
    <p><strong>Estimated OT this week:</strong> ~{ot_per_person:.1f} hrs/person over 40 hrs</p>
    <p><strong>Estimated OT cost:</strong> ~${total_ot_cost:.0f}/week (5 people × {ot_per_person:.1f} OT hrs × $12 premium)</p>
    """

    if ot_per_person > 5:
        html += """<p style="color:#CC0000;"><strong>HIRING RECOMMENDATION: 2 new hires needed ASAP.</strong>
        Adding 1 person to Gustavo and 1 to Jorge (making both 3-person crews) would eliminate overtime
        and allow Jon to return to full-time irrigation work. Target: 35-40 hrs/person/week with zero OT.</p>"""
    elif ot_per_person > 2:
        html += """<p style="color:#CC6600;"><strong>HIRING RECOMMENDATION: 2 new hires recommended.</strong>
        Current OT is manageable but not sustainable long-term. Hiring 1 person per crew would
        reduce hours to ~35-40/person/week and free Jon for irrigation.</p>"""
    elif ot_per_person > 0:
        html += """<p style="color:#006600;"><strong>STAFFING: Minimal OT. Current staffing is near target.</strong>
        Consider hiring when workload increases or Jon needs to return to irrigation full-time.</p>"""
    else:
        html += """<p style="color:#006600;"><strong>STAFFING: No OT. Crew capacity is sufficient.</strong></p>"""

    html += '</div>'

    html += '<p style="color:#888; font-size:11px;">Generated by Black Hill Route Scheduler. Data from Aspire CRM.</p>'
    html += '</body></html>'
    return html


def send_email(subject, html_body):
    """Send email via Gmail SMTP."""
    sender = os.environ.get("GMAIL_EMAIL")
    password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender or not password:
        print("ERROR: GMAIL_EMAIL and GMAIL_APP_PASSWORD env vars required.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Black Hill Route Scheduler", sender))
    msg["To"] = "evelin@blackhilltx.com"
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, ["evelin@blackhilltx.com"], msg.as_string())
        print("Email sent successfully to evelin@blackhilltx.com")
        return True
    except Exception as e:
        print(f"ERROR sending email: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Generate weekly crew schedule")
    parser.add_argument("--dry-run", action="store_true", help="Print schedule, don't email")
    parser.add_argument("--week", type=str, help="Target week Monday (YYYY-MM-DD). Default: next Monday.")
    args = parser.parse_args()

    # Determine target week
    if args.week:
        week_monday = date.fromisoformat(args.week)
    else:
        week_monday = get_monday()

    print(f"Generating schedule for week of {week_monday}...")

    # Step 0: Check Aspire for contract changes
    print("Querying Aspire for active maintenance contracts...")
    try:
        active = query_active_contracts()
        new_contracts, missing = detect_changes(active)
        if new_contracts:
            print(f"  NEW CONTRACTS: {len(new_contracts)}")
            for c in new_contracts:
                print(f"    - {c.get('PropertyName')} (${c.get('EstimatedDollars', 0):,.0f})")
        if missing:
            print(f"  POSSIBLE CANCELLATIONS: {len(missing)}")
            for m in missing:
                print(f"    - {m.get('name')}")
        if not new_contracts and not missing:
            print("  No changes detected.")
    except Exception as e:
        print(f"  WARNING: Could not query Aspire: {e}")
        new_contracts, missing = [], []

    # Step 1: Build schedule
    schedule = build_schedule(week_monday)

    # Step 2: Print summary
    print(f"\n{'='*60}")
    print(f"SCHEDULE — Week of {week_monday}")
    print(f"Arlington: {'ACTIVE' if schedule['arlington']['active'] else 'GAP WEEK'}")
    print(f"{'='*60}")
    for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
        day = schedule["days"][day_name]
        print(f"\n{day_name}:")
        if "gustavo" in day:
            for key in ["gustavo", "jorge"]:
                info = day[key]
                print(f"  {key.upper()} ({info['crew']}): {info['total_hrs']} hrs ({info['per_person']}/person)")
                for p in info["properties"]:
                    print(f"    {p['name']}: {p['budget']} hrs")
        else:
            print(f"  {day.get('crew','')}: {day.get('total_hrs',0)} hrs ({day.get('per_person',0)}/person)")
            for p in day.get("properties", []):
                print(f"    {p['name']}: {p['budget']} hrs")

    # Step 3: Email
    if not args.dry_run:
        subject = f"Crew Schedule — Week of {week_monday.strftime('%B %d, %Y')}"
        html = format_html_email(schedule, new_contracts, missing)
        send_email(subject, html)
    else:
        print("\n[DRY RUN — no email sent]")


if __name__ == "__main__":
    main()
