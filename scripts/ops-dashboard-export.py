#!/usr/bin/env python3
"""Ops Dashboard Export — pulls WorkTicket data from Aspire API,
aggregates by crew leader and route, outputs JSON for the crew-facing dashboard.

Output: ~/projects/crew-dashboard/data.json

Schedule: daily at 5:15 AM via launchd (after CFO data export).

Manual run:
    python3 ~/projects/scripts/ops-dashboard-export.py             # Full run
    python3 ~/projects/scripts/ops-dashboard-export.py --dry-run    # Print summary only
    python3 ~/projects/scripts/ops-dashboard-export.py --output p.json  # Custom path
"""

import argparse
import importlib.util
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Import Aspire modules from skills directory
# ---------------------------------------------------------------------------
ASPIRE_SCRIPTS = Path(__file__).resolve().parent


def _import_aspire_module(name):
    spec = importlib.util.spec_from_file_location(name, ASPIRE_SCRIPTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


aspire_auth = _import_aspire_module("aspire-auth")
aspire_query = _import_aspire_module("aspire-query")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_OUTPUT = str(Path(__file__).resolve().parent.parent / "dashboard" / "data.json")
TODAY = date.today()
BONUS_THRESHOLD = 97.0  # ≤97% labor variance = bonus eligible
MIN_HOURS_THRESHOLD = 20.0  # Minimum estimated hours to qualify for leaderboard
EXCLUDED_CREW_IDS = {6}  # Evelin Montenegro — owner, not a crew leader

# Additional crew-to-route mappings (crew leaders who work a route but aren't the primary in Aspire)
EXTRA_CREW_ROUTE = {
    1834: 1820,  # Jose Guadalupe Sandoval Ramos → same route as Edgar Herrera Palafox (Land 1 - Green)
}


# ---------------------------------------------------------------------------
# Aspire helpers (reused from cfo-data-export.py)
# ---------------------------------------------------------------------------
def get_aspire_connection():
    """Return (config, token) for the Aspire Reporting client."""
    config = aspire_auth.load_config("reporting")
    token = aspire_auth.get_token("reporting")
    return config, token


def fetch_all(endpoint, params, config, token, page_size=100):
    """Paginate through all records from an Aspire OData endpoint."""
    all_records = []
    skip = 0
    while True:
        p = dict(params)
        p["$top"] = str(page_size)
        p["$skip"] = str(skip)
        try:
            data = aspire_query.query_endpoint(endpoint, p, config, token)
        except SystemExit:
            break
        records = data if isinstance(data, list) else [data] if data else []
        if not records:
            break
        all_records.extend(records)
        if len(records) < page_size:
            break
        skip += page_size
    return all_records


def safe_date(val):
    """Parse an ISO date string from Aspire into a date object, or return None."""
    if not val:
        return None
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None


def safe_float(val, default=0.0):
    """Coerce a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Date range helpers
# ---------------------------------------------------------------------------
def get_prev_week_range(ref_date):
    """Return (monday, sunday) for the most recent completed week before ref_date."""
    weekday = ref_date.weekday()  # Monday=0
    # Go back to last Sunday (end of previous week)
    days_since_sunday = (weekday + 1) % 7  # Mon→1, Tue→2, ..., Sun→0
    if days_since_sunday == 0:
        days_since_sunday = 7  # If today is Sunday, go back to LAST Sunday
    last_sunday = ref_date - timedelta(days=days_since_sunday)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def get_week_before_range(ref_date):
    """Return (monday, sunday) for 2 weeks ago (the week before the previous week)."""
    prev_monday, _ = get_prev_week_range(ref_date)
    return get_prev_week_range(prev_monday)


def get_day_label(d):
    """Return a day label like 'Mon 3/10' for display."""
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    return f"{days[d.weekday()]} {d.month}/{d.day}"


def get_week_label(start, end):
    """Return a week label like '3/10-3/14'."""
    return f"{start.month}/{start.day}-{end.month}/{end.day}"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------
def fetch_work_tickets(config, token, start_date, end_date):
    """Fetch completed WorkTickets within the date range."""
    start_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
    end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    params = {
        "$filter": (
            f"ScheduledStartDate ge {start_str} and ScheduledStartDate lt {end_str}"
            f" and (WorkTicketStatus eq 'Complete' or WorkTicketStatus eq 'Reviewed')"
        ),
    }
    return fetch_all("WorkTickets", params, config, token)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def target_hours(ticket):
    """Return the crew's target hours: HoursScheduled if set, else HoursEst."""
    sched = safe_float(ticket.get("HoursScheduled"))
    return sched if sched > 0 else safe_float(ticket.get("HoursEst"))


def build_leaderboard(current_tickets, prev_tickets):
    """Aggregate by CrewLeaderName, rank, and compute trends."""
    def aggregate_by_crew(tickets):
        crews = defaultdict(lambda: {"actual": 0.0, "estimated": 0.0})
        for t in tickets:
            leader = t.get("CrewLeaderName", "").strip()
            if not leader:
                continue
            crews[leader]["actual"] += safe_float(t.get("HoursAct"))
            crews[leader]["estimated"] += target_hours(t)
        return crews

    current = aggregate_by_crew(current_tickets)
    prev = aggregate_by_crew(prev_tickets)

    # Calculate efficiency score and filter by minimum hours
    # Efficiency = Estimated / Actual * 100 (higher = better, beat the estimate)
    entries = []
    for leader, data in current.items():
        if data["estimated"] < MIN_HOURS_THRESHOLD:
            continue
        efficiency = (data["estimated"] / data["actual"]) * 100 if data["actual"] > 0 else 0
        entries.append({
            "crew_leader": leader,
            "actual_hours": round(data["actual"], 1),
            "estimated_hours": round(data["estimated"], 1),
            "labor_variance_pct": round(efficiency, 1),
            "bonus_eligible": efficiency >= BONUS_THRESHOLD,
        })

    # Sort by efficiency (higher is better)
    entries.sort(key=lambda e: e["labor_variance_pct"], reverse=True)

    # Calculate previous period ranks for trend
    prev_entries = []
    for leader, data in prev.items():
        if data["estimated"] < MIN_HOURS_THRESHOLD:
            continue
        efficiency = (data["estimated"] / data["actual"]) * 100 if data["actual"] > 0 else 0
        prev_entries.append({"crew_leader": leader, "variance": efficiency})
    prev_entries.sort(key=lambda e: e["variance"], reverse=True)
    prev_ranks = {e["crew_leader"]: i + 1 for i, e in enumerate(prev_entries)}

    # Assign ranks and trends
    for i, entry in enumerate(entries):
        entry["rank"] = i + 1
        prev_rank = prev_ranks.get(entry["crew_leader"])
        if prev_rank is None:
            entry["trend"] = "new"
            entry["rank_change"] = 0
        else:
            change = prev_rank - entry["rank"]  # positive = improved
            entry["trend"] = "up" if change > 0 else "down" if change < 0 else "same"
            entry["rank_change"] = change

    return entries


def fetch_routes(config, token):
    """Fetch active routes and build a crew-leader-to-route mapping."""
    routes = fetch_all("Routes", {"$filter": "Active eq true"}, config, token)
    # Map CrewLeaderContactID → route info
    crew_to_route = {}
    for r in routes:
        crew_id = r.get("CrewLeaderContactID")
        if crew_id:
            crew_to_route[crew_id] = {
                "route_name": r.get("RouteName", "Unknown"),
                "division": r.get("DivisionName", ""),
                "route_size": r.get("RouteSize", 0),
            }
    return crew_to_route


def fetch_opportunity_properties(config, token):
    """Fetch OpportunityID → PropertyName mapping."""
    opps = fetch_all(
        "Opportunities",
        {"$select": "OpportunityID,PropertyName"},
        config, token,
    )
    return {o["OpportunityID"]: (o.get("PropertyName") or "Unknown") for o in opps if o.get("OpportunityID")}


def fetch_service_names(config, token):
    """Fetch OpportunityServiceID → DisplayName mapping."""
    svcs = fetch_all(
        "OpportunityServices",
        {"$select": "OpportunityServiceID,DisplayName"},
        config, token,
    )
    return {s["OpportunityServiceID"]: (s.get("DisplayName") or "Service") for s in svcs if s.get("OpportunityServiceID")}


def fetch_tm_opportunity_ids(config, token):
    """Fetch OpportunityIDs that are billed as T&M (Time & Materials).

    Returns a set of OpportunityIDs to exclude from efficiency calculations,
    since comparing estimated vs actual hours is not meaningful for T&M work.
    """
    opps = fetch_all(
        "Opportunities",
        {"$select": "OpportunityID,InvoiceType"},
        config, token,
    )
    return {
        o["OpportunityID"]
        for o in opps
        if o.get("OpportunityID") and "T&M" in (o.get("InvoiceType") or "")
    }


def get_week_ranges(num_weeks, ref_date):
    """Return list of (monday, friday, label) tuples for the last N completed weeks."""
    weeks = []
    current = ref_date
    for _ in range(num_weeks):
        monday, friday = get_prev_week_range(current)
        label = get_week_label(monday, friday)
        weeks.append((monday, friday, label))
        current = monday  # step back to get the week before
    weeks.reverse()  # chronological order (oldest first)
    return weeks


def build_ops_scorecard(all_tickets, crew_to_route, weeks):
    """Build Capsa-style weekly matrix: routes as rows, weeks as columns.

    Returns dict with:
      - weeks: list of week labels
      - rows: list of {name, total, weekly_values}
      - total_row: {total, weekly_values}
      - overall_efficiency: single number
    """
    # Bucket tickets by week and route
    # route -> week_label -> {actual, estimated}
    matrix = defaultdict(lambda: defaultdict(lambda: {"actual": 0.0, "estimated": 0.0}))
    # Also track totals per week
    week_totals = defaultdict(lambda: {"actual": 0.0, "estimated": 0.0})
    grand_actual = 0.0
    grand_estimated = 0.0

    for t in all_tickets:
        crew_id = t.get("CrewLeaderContactID")
        route_info = crew_to_route.get(crew_id) if crew_id else None
        if not route_info:
            continue
        route_name = route_info["route_name"]
        actual = safe_float(t.get("HoursAct"))
        estimated = target_hours(t)
        sched_date = safe_date(t.get("ScheduledStartDate"))
        if not sched_date:
            continue

        # Find which week this ticket belongs to
        for monday, friday, label in weeks:
            if monday <= sched_date <= friday:
                matrix[route_name][label]["actual"] += actual
                matrix[route_name][label]["estimated"] += estimated
                week_totals[label]["actual"] += actual
                week_totals[label]["estimated"] += estimated
                grand_actual += actual
                grand_estimated += estimated
                break

    week_labels = [w[2] for w in weeks]

    # Build rows
    rows = []
    for route_name, week_data in sorted(matrix.items()):
        # Route total across all weeks
        total_actual = sum(d["actual"] for d in week_data.values())
        total_estimated = sum(d["estimated"] for d in week_data.values())
        total_eff = round((total_estimated / total_actual) * 100, 1) if total_actual > 0 else None

        weekly_values = []
        for label in week_labels:
            wd = week_data.get(label)
            if wd and wd["actual"] > 0:
                weekly_values.append(round((wd["estimated"] / wd["actual"]) * 100, 1))
            else:
                weekly_values.append(None)

        rows.append({
            "name": route_name,
            "total": total_eff,
            "weekly_values": weekly_values,
        })

    # Sort by total efficiency (highest first)
    rows.sort(key=lambda r: r["name"])

    # Total row
    total_weekly = []
    for label in week_labels:
        wt = week_totals.get(label)
        if wt and wt["actual"] > 0:
            total_weekly.append(round((wt["estimated"] / wt["actual"]) * 100, 1))
        else:
            total_weekly.append(None)

    overall = round((grand_estimated / grand_actual) * 100, 1) if grand_actual > 0 else None

    return {
        "weeks": week_labels,
        "rows": rows,
        "total_row": {
            "total": overall,
            "weekly_values": total_weekly,
        },
        "overall_efficiency": overall,
    }


def build_generic_scorecard(all_tickets, time_buckets, group_fn, required_groups=None):
    """Build a matrix from tickets using any grouping function and time buckets.

    Args:
        all_tickets: list of WorkTicket dicts
        time_buckets: list of (start_date, end_date, label) tuples
        group_fn: function(ticket) -> group_name or None
        required_groups: optional list of group names that must appear even with no data
    """
    matrix = defaultdict(lambda: defaultdict(lambda: {"actual": 0.0, "estimated": 0.0}))
    bucket_totals = defaultdict(lambda: {"actual": 0.0, "estimated": 0.0})
    grand_actual = 0.0
    grand_estimated = 0.0

    for t in all_tickets:
        group = group_fn(t)
        if not group:
            continue
        actual = safe_float(t.get("HoursAct"))
        estimated = target_hours(t)
        sched_date = safe_date(t.get("ScheduledStartDate"))
        if not sched_date:
            continue

        for start, end, label in time_buckets:
            if start <= sched_date <= end:
                matrix[group][label]["actual"] += actual
                matrix[group][label]["estimated"] += estimated
                bucket_totals[label]["actual"] += actual
                bucket_totals[label]["estimated"] += estimated
                grand_actual += actual
                grand_estimated += estimated
                break

    bucket_labels = [b[2] for b in time_buckets]

    rows = []
    for name, bucket_data in sorted(matrix.items()):
        total_actual = sum(d["actual"] for d in bucket_data.values())
        total_estimated = sum(d["estimated"] for d in bucket_data.values())
        total_eff = round((total_estimated / total_actual) * 100, 1) if total_actual > 0 else None

        values = []
        for label in bucket_labels:
            bd = bucket_data.get(label)
            if bd and bd["actual"] > 0:
                values.append(round((bd["estimated"] / bd["actual"]) * 100, 1))
            else:
                values.append(None)

        rows.append({"name": name, "total": total_eff, "weekly_values": values})

    # Ensure required groups are present even with no data
    if required_groups:
        existing = {r["name"] for r in rows}
        for g in required_groups:
            if g not in existing:
                rows.append({"name": g, "total": None, "weekly_values": [None] * len(bucket_labels)})

    rows.sort(key=lambda r: r["name"])

    total_values = []
    for label in bucket_labels:
        bt = bucket_totals.get(label)
        if bt and bt["actual"] > 0:
            total_values.append(round((bt["estimated"] / bt["actual"]) * 100, 1))
        else:
            total_values.append(None)

    overall = round((grand_estimated / grand_actual) * 100, 1) if grand_actual > 0 else None

    return {
        "weeks": bucket_labels,
        "rows": rows,
        "total_row": {"total": overall, "weekly_values": total_values},
        "overall_efficiency": overall,
    }


def get_daily_buckets(start_date, end_date):
    """Return list of (date, date, label) for each day in range (Mon-Sun)."""
    buckets = []
    d = start_date
    while d <= end_date:
        label = f"{d.month}/{d.day}"
        buckets.append((d, d, label))
        d += timedelta(days=1)
    return buckets


def get_monthly_buckets(start_date, end_date):
    """Return list of (first_day, last_day, label) for each month in range."""
    buckets = []
    d = start_date.replace(day=1)
    while d <= end_date:
        first = d
        if d.month == 12:
            last = d.replace(year=d.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            last = d.replace(month=d.month + 1, day=1) - timedelta(days=1)
        label = d.strftime("%b %Y")
        buckets.append((first, last, label))
        d = last + timedelta(days=1)
    return buckets


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Ops Dashboard Export to JSON")
    parser.add_argument("--dry-run", action="store_true", help="Print summary without writing file")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Custom output path")
    args = parser.parse_args()

    output_path = os.path.expanduser(args.output)

    print(f"Ops Dashboard Export - {TODAY.isoformat()}")
    print(f"Output: {output_path}")
    print()

    # Aspire auth
    try:
        config, token = get_aspire_connection()
        print("Aspire API: authenticated")
    except Exception as e:
        print(f"ERROR: Aspire auth failed ({e})", file=sys.stderr)
        sys.exit(1)

    # Date ranges — two periods
    # 1) Month-to-date (for bonus tracking)
    mtd_start = TODAY.replace(day=1)
    mtd_end = TODAY

    # 2) Prior completed week (Mon-Fri)
    pw_start, pw_end = get_prev_week_range(TODAY)

    # 3) Week before that (for trend comparison)
    wbt_start, wbt_end = get_week_before_range(TODAY)

    # Date ranges
    mtd_start = TODAY.replace(day=1)
    mtd_end = TODAY
    pw_start, pw_end = get_prev_week_range(TODAY)
    wbt_start, wbt_end = get_week_before_range(TODAY)

    # Scorecard matrix: 4 weeks of history
    SCORECARD_WEEKS = 4
    scorecard_week_ranges = get_week_ranges(SCORECARD_WEEKS, TODAY)
    sc_start = scorecard_week_ranges[0][0]  # earliest Monday
    sc_end = scorecard_week_ranges[-1][1]    # latest Friday

    print(f"  Month-to-date: {mtd_start} to {mtd_end}")
    print(f"  Prior week: {pw_start} to {pw_end}")
    print(f"  Scorecard range: {sc_start} to {sc_end} ({SCORECARD_WEEKS} weeks)")

    # Fetch tickets — use the broadest range (scorecard covers most history)
    fetch_start = min(mtd_start, sc_start)
    fetch_end = max(mtd_end, sc_end)
    print(f"  Fetching WorkTickets {fetch_start} to {fetch_end}...")
    all_tickets = fetch_work_tickets(config, token, fetch_start, fetch_end)
    print(f"    {len(all_tickets)} tickets")

    # Subset tickets for each period
    def filter_tickets(tickets, start, end):
        return [t for t in tickets
                if safe_date(t.get("ScheduledStartDate"))
                and start <= safe_date(t.get("ScheduledStartDate")) <= end]

    mtd_tickets = filter_tickets(all_tickets, mtd_start, mtd_end)
    pw_tickets = filter_tickets(all_tickets, pw_start, pw_end)
    wbt_tickets = filter_tickets(all_tickets, wbt_start, wbt_end)

    print(f"    MTD: {len(mtd_tickets)}, Prior week: {len(pw_tickets)}, Trend week: {len(wbt_tickets)}")

    # Fetch route definitions
    print("  Fetching routes...")
    crew_to_route = fetch_routes(config, token)
    # Apply extra crew-to-route mappings (secondary crew leaders)
    for extra_cid, primary_cid in EXTRA_CREW_ROUTE.items():
        if primary_cid in crew_to_route and extra_cid not in crew_to_route:
            crew_to_route[extra_cid] = crew_to_route[primary_cid]
    print(f"    {len(crew_to_route)} crew-route mappings")

    # Fetch opportunity → property mapping for job view
    print("  Fetching opportunity properties...")
    opp_to_property = fetch_opportunity_properties(config, token)
    print(f"    {len(opp_to_property)} opportunities mapped")

    # Fetch service names for job board
    print("  Fetching service names...")
    svc_names = fetch_service_names(config, token)
    print(f"    {len(svc_names)} services mapped")

    # Filter out T&M tickets (efficiency comparison not meaningful for T&M)
    print("  Fetching T&M opportunities...")
    tm_opp_ids = fetch_tm_opportunity_ids(config, token)
    print(f"    {len(tm_opp_ids)} T&M opportunities found")
    pre_filter = len(all_tickets)
    all_tickets = [t for t in all_tickets if t.get("OpportunityID") not in tm_opp_ids]
    tm_removed = pre_filter - len(all_tickets)
    print(f"    Removed {tm_removed} T&M tickets ({len(all_tickets)} remaining)")

    # Remove tickets from excluded crew leaders (owner/admin, not field crews)
    pre_excl = len(all_tickets)
    all_tickets = [t for t in all_tickets if t.get("CrewLeaderContactID") not in EXCLUDED_CREW_IDS]
    print(f"    Removed {pre_excl - len(all_tickets)} excluded crew leader tickets ({len(all_tickets)} remaining)")

    # --- Build leaderboards ---
    print("  Building MTD leaderboard...")
    mtd_leaderboard = build_leaderboard(mtd_tickets, wbt_tickets)
    print(f"    {len(mtd_leaderboard)} crew leaders")

    print("  Building prior week leaderboard...")
    pw_leaderboard = build_leaderboard(pw_tickets, wbt_tickets)
    print(f"    {len(pw_leaderboard)} crew leaders")

    # --- Build Ops Scorecard matrices ---
    print("  Building ops scorecard matrices...")
    scorecard_tickets = filter_tickets(all_tickets, sc_start, sc_end)

    # Group-by functions
    # Filter out subcontractor routes
    sub_crew_ids = set()
    for crew_id, ri in crew_to_route.items():
        if "sub" in ri["route_name"].lower():
            sub_crew_ids.add(crew_id)

    def is_sub(t):
        return t.get("CrewLeaderContactID") in sub_crew_ids

    def group_by_route(t):
        if is_sub(t):
            return None
        crew_id = t.get("CrewLeaderContactID")
        ri = crew_to_route.get(crew_id) if crew_id else None
        if not ri:
            return None
        name = ri["route_name"]
        if any(x in name.lower() for x in EXCLUDED_ROUTE_NAMES):
            return None
        return name

    def group_by_crew(t):
        if is_sub(t):
            return None
        return (t.get("CrewLeaderName") or "").strip() or None

    def group_by_division(t):
        if is_sub(t):
            return None
        crew_id = t.get("CrewLeaderContactID")
        ri = crew_to_route.get(crew_id) if crew_id else None
        return (ri.get("division") or "Unknown") if ri else None

    def group_by_property(t):
        if is_sub(t):
            return None
        opp_id = t.get("OpportunityID")
        return opp_to_property.get(opp_id) if opp_id else None

    # Time buckets
    # Weekly: just the prior completed week (1 column)
    weekly_buckets = [scorecard_week_ranges[-1]]
    # Daily: day-by-day for the prior completed week (Mon-Sun)
    daily_buckets = get_daily_buckets(pw_start, pw_end)
    # Monthly: entire current month (MTD)
    monthly_buckets = [(mtd_start, mtd_end, mtd_start.strftime("%b %Y"))]

    # Build all combinations (4 views x 3 time granularities)
    REQUIRED_DIVISIONS = ["Maintenance", "Landscape"]
    EXCLUDED_ROUTE_NAMES = {"test"}
    REQUIRED_ROUTES = [
        ri["route_name"] for cid, ri in crew_to_route.items()
        if cid not in sub_crew_ids and not any(x in ri["route_name"].lower() for x in EXCLUDED_ROUTE_NAMES)
    ]
    scorecard_data = {}
    required_map = {"division": REQUIRED_DIVISIONS, "route": REQUIRED_ROUTES}
    for view_name, group_fn in [("route", group_by_route), ("crew", group_by_crew), ("division", group_by_division), ("property", group_by_property)]:
        req = required_map.get(view_name)
        for time_name, buckets in [("weekly", weekly_buckets), ("daily", daily_buckets), ("monthly", monthly_buckets)]:
            key = f"{view_name}_{time_name}"
            scorecard_data[key] = build_generic_scorecard(scorecard_tickets, buckets, group_fn, required_groups=req)
            print(f"    {key}: {len(scorecard_data[key]['rows'])} rows x {len(scorecard_data[key]['weeks'])} cols")


    # Compute summary stats
    def compute_stats(tickets, leaderboard):
        bonus_eligible = sum(1 for e in leaderboard if e["bonus_eligible"])
        total_actual = sum(safe_float(t.get("HoursAct")) for t in tickets)
        total_estimated = sum(target_hours(t) for t in tickets)
        avg = (total_estimated / total_actual * 100) if total_actual > 0 else 0
        return bonus_eligible, round(avg, 1)

    mtd_bonus, mtd_avg = compute_stats(mtd_tickets, mtd_leaderboard)
    pw_bonus, pw_avg = compute_stats(pw_tickets, pw_leaderboard)

    month_label = mtd_start.strftime("%B %Y")
    week_label = get_week_label(pw_start, pw_end)

    # --- Build Job Board (Aspire-style schedule view) ---
    print("  Building job board...")
    # Current week: Monday of this week through Sunday
    cw_monday = TODAY - timedelta(days=TODAY.weekday())
    cw_sunday = cw_monday + timedelta(days=6)
    cw_days = []
    d = cw_monday
    while d <= cw_sunday:
        cw_days.append(d)
        d += timedelta(days=1)
    day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Filter tickets for current week (include all statuses for scheduling view)
    cw_tickets = filter_tickets(all_tickets, cw_monday, cw_sunday)

    # Build route → day → tickets structure
    job_board_routes = {}
    for cid, ri in crew_to_route.items():
        if cid in sub_crew_ids:
            continue
        rname = ri["route_name"]
        if any(x in rname.lower() for x in EXCLUDED_ROUTE_NAMES):
            continue
        # Find crew leader name from any ticket, or use route name
        crew_name = None
        for t in all_tickets:
            if t.get("CrewLeaderContactID") == cid:
                crew_name = (t.get("CrewLeaderName") or "").strip()
                break
        job_board_routes[rname] = {
            "name": rname,
            "crew_leader": crew_name or "",
            "crew_leader_id": cid,
            "days": {},
        }

    for t in cw_tickets:
        crew_id = t.get("CrewLeaderContactID")
        if crew_id in sub_crew_ids or crew_id in EXCLUDED_CREW_IDS:
            continue
        ri = crew_to_route.get(crew_id)
        if not ri:
            continue
        rname = ri["route_name"]
        if any(x in rname.lower() for x in EXCLUDED_ROUTE_NAMES):
            continue
        if t.get("OpportunityID") in tm_opp_ids:
            continue

        sched_date = safe_date(t.get("ScheduledStartDate"))
        if not sched_date or sched_date < cw_monday or sched_date > cw_sunday:
            continue

        day_key = f"{day_labels[sched_date.weekday()]} {sched_date.month}/{sched_date.day}"
        opp_id = t.get("OpportunityID")
        svc_id = t.get("OpportunityServiceID")
        prop = opp_to_property.get(opp_id, "Unknown")
        svc = svc_names.get(svc_id, "Service")
        occur = t.get("Occur") or ""
        occurrences = t.get("Occurrences") or ""
        occur_label = f"{occur}/{occurrences}" if occur and occurrences else ""
        tgt = target_hours(t)
        act = safe_float(t.get("HoursAct"))
        variance = round((tgt / act) * 100, 1) if act > 0 else None

        if rname not in job_board_routes:
            continue
        route_data = job_board_routes[rname]
        if day_key not in route_data["days"]:
            route_data["days"][day_key] = {"tickets": [], "total_hours": 0.0}

        route_data["days"][day_key]["tickets"].append({
            "property": prop,
            "service": svc,
            "occur": occur_label,
            "target_hours": round(tgt, 2),
            "actual_hours": round(act, 2),
            "variance_pct": variance,
        })
        route_data["days"][day_key]["total_hours"] = round(
            route_data["days"][day_key]["total_hours"] + tgt, 2
        )

    # Build ordered day columns
    jb_day_keys = [f"{day_labels[d.weekday()]} {d.month}/{d.day}" for d in cw_days]
    jb_routes = sorted(job_board_routes.values(), key=lambda r: r["name"])
    # Count jobs per route
    for r in jb_routes:
        r["job_count"] = sum(len(day["tickets"]) for day in r["days"].values())

    job_board = {
        "week_label": f"{cw_monday.month}/{cw_monday.day} - {cw_sunday.month}/{cw_sunday.day}",
        "days": jb_day_keys,
        "routes": jb_routes,
    }
    print(f"    {len(jb_routes)} routes, {sum(r['job_count'] for r in jb_routes)} tickets for week of {job_board['week_label']}")

    # --- Build ticket-level detail for drill-down ---
    print("  Building ticket details...")
    ticket_details = []
    for t in all_tickets:
        crew_id = t.get("CrewLeaderContactID")
        if crew_id in sub_crew_ids or crew_id in EXCLUDED_CREW_IDS:
            continue
        ri = crew_to_route.get(crew_id)
        opp_id = t.get("OpportunityID")
        svc_id = t.get("OpportunityServiceID")
        sched = safe_date(t.get("ScheduledStartDate"))
        if not sched:
            continue
        tgt = target_hours(t)
        act = safe_float(t.get("HoursAct"))
        eff = round((tgt / act) * 100, 1) if act > 0 else None

        ticket_details.append({
            "date": sched.isoformat(),
            "day_label": f"{sched.month}/{sched.day}",
            "crew_leader": (t.get("CrewLeaderName") or "").strip(),
            "route": ri["route_name"] if ri else None,
            "division": (ri.get("division") or "Unknown") if ri else None,
            "property": opp_to_property.get(opp_id, "Unknown"),
            "service": svc_names.get(svc_id, "Service"),
            "target_hours": round(tgt, 2),
            "actual_hours": round(act, 2),
            "efficiency": eff,
        })
    print(f"    {len(ticket_details)} tickets with detail")

    output = {
        "generated": datetime.now().isoformat(timespec="seconds"),
        "bonus_threshold": BONUS_THRESHOLD,
        "scorecard": scorecard_data,
        "job_board": job_board,
        "tickets": ticket_details,
        "mtd": {
            "period": {
                "label": month_label,
                "start": mtd_start.isoformat(),
                "end": mtd_end.isoformat(),
            },
            "leaderboard": mtd_leaderboard,
            "bonus_eligible_count": mtd_bonus,
            "company_avg_variance": mtd_avg,
            "total_crew_leaders": len(mtd_leaderboard),
            "tickets_analyzed": len(mtd_tickets),
        },
        "prior_week": {
            "period": {
                "label": f"Week of {week_label}",
                "start": pw_start.isoformat(),
                "end": pw_end.isoformat(),
            },
            "leaderboard": pw_leaderboard,
            "bonus_eligible_count": pw_bonus,
            "company_avg_variance": pw_avg,
            "total_crew_leaders": len(pw_leaderboard),
            "tickets_analyzed": len(pw_tickets),
        },
    }

    # Summary
    print()
    print(f"Summary:")
    print(f"  MTD ({month_label}): {len(mtd_tickets)} tickets, {len(mtd_leaderboard)} crews, {mtd_bonus} bonus-eligible, {mtd_avg}% avg")
    print(f"  Prior Week ({week_label}): {len(pw_tickets)} tickets, {len(pw_leaderboard)} crews, {pw_bonus} bonus-eligible, {pw_avg}% avg")

    if args.dry_run:
        print("\n(dry-run: file not written)")
        print("\nMTD Leaderboard:")
        for e in mtd_leaderboard[:5]:
            bonus = "BONUS" if e["bonus_eligible"] else ""
            print(f"  #{e['rank']} {e['crew_leader']}: {e['labor_variance_pct']}% ({e['trend']}) {bonus}")
        print("\nPrior Week Leaderboard:")
        for e in pw_leaderboard[:5]:
            bonus = "BONUS" if e["bonus_eligible"] else ""
            print(f"  #{e['rank']} {e['crew_leader']}: {e['labor_variance_pct']}% ({e['trend']}) {bonus}")
        print(f"\nOps Scorecard Matrix ({len(ops_scorecard['weeks'])} weeks):")
        print(f"  Weeks: {', '.join(ops_scorecard['weeks'])}")
        for r in ops_scorecard["rows"]:
            vals = [f"{v}%" if v else "—" for v in r["weekly_values"]]
            print(f"  {r['name']}: total={r['total']}% | {' | '.join(vals)}")
        tr = ops_scorecard["total_row"]
        vals = [f"{v}%" if v else "—" for v in tr["weekly_values"]]
        print(f"  TOTAL: {tr['total']}% | {' | '.join(vals)}")
        return

    # Write JSON
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nExported to {output_path}")


if __name__ == "__main__":
    main()
