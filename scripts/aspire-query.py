#!/usr/bin/env python3
"""
Aspire API Query Tool
Generic OData query tool for any Aspire API endpoint.

Uses ClientId/Secret authentication via the Reporting API client (all endpoints, GET-only).
Zero external dependencies — uses only Python stdlib.

Usage:
    python3 aspire-query.py <endpoint> [options]

Examples:
    # List opportunities in bidding stage
    python3 aspire-query.py Opportunities --filter "OpportunityStatusName eq 'Won'" --top 20

    # Get unpaid invoices ordered by due date
    python3 aspire-query.py Invoices --filter "Status eq 'Unpaid'" --orderby "DueDate asc"

    # Work tickets with expanded items
    python3 aspire-query.py WorkTickets --expand "WorkTicketItems" --top 50

    # Select specific fields
    python3 aspire-query.py Equipments --select "Id,Name,Class,Status"

    # Export to CSV
    python3 aspire-query.py Invoices --format csv --output invoices.csv

    # Fetch all pages
    python3 aspire-query.py Opportunities --filter "OpportunityStatusName eq 'Lost'" --all

    # List all available endpoints
    python3 aspire-query.py --list-endpoints

    # Use Lead Monitor client instead (for contacts)
    python3 aspire-query.py Contacts --client lead --top 10

Config: ~/.config/aspire/config.json
"""

import argparse
import csv
import io
import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
from pathlib import Path

# Import auth from sibling script
SCRIPT_DIR = Path(__file__).parent

# All known endpoints from the Aspire API swagger spec
ENDPOINTS = {
    # CRM
    "Activities": {"methods": ["GET"], "category": "CRM"},
    "ActivityCategories": {"methods": ["GET"], "category": "CRM"},
    "ActivityCommentHistories": {"methods": ["GET"], "category": "CRM"},
    "ActivityContacts": {"methods": ["GET"], "category": "CRM"},
    "Companies": {"methods": ["GET", "POST", "PUT"], "category": "CRM"},
    "ContactCustomFieldDefinitions": {"methods": ["GET"], "category": "CRM"},
    "ContactCustomFields": {"methods": ["GET", "POST", "PUT"], "category": "CRM"},
    "ContactTypes": {"methods": ["GET"], "category": "CRM"},
    "Contacts": {"methods": ["GET", "POST", "PUT"], "category": "CRM"},
    "ProspectRatings": {"methods": ["GET"], "category": "CRM"},
    "Tags": {"methods": ["GET"], "category": "CRM"},

    # Properties
    "Addresses": {"methods": ["GET"], "category": "Properties"},
    "Properties": {"methods": ["GET", "POST", "PUT"], "category": "Properties"},
    "PropertyAvailabilities": {"methods": ["GET", "POST"], "category": "Properties"},
    "PropertyContacts": {"methods": ["GET", "POST", "PUT"], "category": "Properties"},
    "PropertyCustomFieldDefinitions": {"methods": ["GET"], "category": "Properties"},
    "PropertyCustomFields": {"methods": ["GET", "POST", "PUT"], "category": "Properties"},
    "PropertyGroups": {"methods": ["GET"], "category": "Properties"},
    "PropertyStatuses": {"methods": ["GET"], "category": "Properties"},
    "PropertyTypes": {"methods": ["GET"], "category": "Properties"},

    # Sales & Estimating
    "CatalogItemCategories": {"methods": ["GET"], "category": "Estimating"},
    "CatalogItems": {"methods": ["GET", "POST", "PUT"], "category": "Estimating"},
    "Opportunities": {"methods": ["GET", "POST"], "category": "Sales"},
    "OpportunityLostReasons": {"methods": ["GET", "POST", "PUT"], "category": "Sales"},
    "OpportunityServiceGroups": {"methods": ["GET"], "category": "Sales"},
    "OpportunityServiceItems": {"methods": ["GET"], "category": "Sales"},
    "OpportunityServiceKitItems": {"methods": ["GET"], "category": "Sales"},
    "OpportunityServices": {"methods": ["GET"], "category": "Sales"},
    "OpportunityStatuses": {"methods": ["GET"], "category": "Sales"},
    "OpportunityTags": {"methods": ["GET", "POST"], "category": "Sales"},
    "SalesTypes": {"methods": ["GET"], "category": "Sales"},
    "ServiceTypeIntegrationCodes": {"methods": ["GET"], "category": "Estimating"},
    "ServiceTypes": {"methods": ["GET"], "category": "Estimating"},
    "Services": {"methods": ["GET"], "category": "Estimating"},
    "TakeoffGroups": {"methods": ["GET"], "category": "Estimating"},
    "TakeoffItems": {"methods": ["GET"], "category": "Estimating"},
    "UnitTypes": {"methods": ["GET", "POST", "PUT"], "category": "Estimating"},

    # Operations
    "ClockTimes": {"methods": ["GET", "POST"], "category": "Operations"},
    "Jobs": {"methods": ["GET"], "category": "Operations"},
    "JobStatuses": {"methods": ["GET"], "category": "Operations"},
    "Routes": {"methods": ["GET"], "category": "Operations"},
    "Tasks": {"methods": ["POST"], "category": "Operations"},
    "WorkTicketCanceledReasons": {"methods": ["GET"], "category": "Operations"},
    "WorkTicketItems": {"methods": ["GET"], "category": "Operations"},
    "WorkTicketRevenues": {"methods": ["GET"], "category": "Operations"},
    "WorkTicketStatus": {"methods": ["POST"], "category": "Operations"},
    "WorkTicketTimes": {"methods": ["GET", "POST"], "category": "Operations"},
    "WorkTicketVisitNotes": {"methods": ["GET"], "category": "Operations"},
    "WorkTicketVisits": {"methods": ["GET"], "category": "Operations"},
    "WorkTickets": {"methods": ["GET", "POST"], "category": "Operations"},

    # Financial
    "BankDeposits": {"methods": ["GET"], "category": "Financial"},
    "InvoiceBatches": {"methods": ["GET"], "category": "Financial"},
    "InvoiceRevenues": {"methods": ["GET"], "category": "Financial"},
    "InvoiceTaxes": {"methods": ["GET"], "category": "Financial"},
    "Invoices": {"methods": ["GET"], "category": "Financial"},
    "PartialPayments": {"methods": ["POST"], "category": "Financial"},
    "PaymentCategories": {"methods": ["GET"], "category": "Financial"},
    "PaymentTerms": {"methods": ["GET"], "category": "Financial"},
    "Payments": {"methods": ["GET"], "category": "Financial"},
    "ReceiptStatuses": {"methods": ["GET"], "category": "Financial"},
    "Receipts": {"methods": ["GET", "POST"], "category": "Financial"},
    "RevenueVariances": {"methods": ["GET"], "category": "Financial"},
    "ServiceTaxOverrides": {"methods": ["POST", "PUT"], "category": "Financial"},
    "TaxEntities": {"methods": ["GET", "POST", "PUT"], "category": "Financial"},
    "TaxJurisdictions": {"methods": ["GET", "POST", "PUT"], "category": "Financial"},
    "Vendors": {"methods": ["GET", "POST", "PUT"], "category": "Financial"},

    # Equipment
    "EquipmentClasses": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentDisposalReasons": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentManufacturers": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentModelServiceSchedules": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentModels": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentReadingLogs": {"methods": ["GET", "POST", "PUT"], "category": "Equipment"},
    "EquipmentRequestedServices": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentServiceLogs": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentServiceTags": {"methods": ["GET"], "category": "Equipment"},
    "EquipmentSizes": {"methods": ["GET"], "category": "Equipment"},
    "Equipments": {"methods": ["GET"], "category": "Equipment"},
    "InventoryLocations": {"methods": ["GET"], "category": "Equipment"},

    # HR / Workforce
    "CertificationTypes": {"methods": ["GET"], "category": "HR"},
    "Certifications": {"methods": ["GET"], "category": "HR"},
    "EmployeeIncidentTypes": {"methods": ["GET"], "category": "HR"},
    "EmployeeIncidents": {"methods": ["GET"], "category": "HR"},
    "PayCodes": {"methods": ["GET", "POST", "PUT"], "category": "HR"},
    "PayRateOverridePayCodes": {"methods": ["GET", "POST", "PUT"], "category": "HR"},
    "PayRates": {"methods": ["GET", "POST", "PUT"], "category": "HR"},
    "PaySchedules": {"methods": ["GET", "POST", "PUT"], "category": "HR"},
    "Roles": {"methods": ["GET"], "category": "HR"},
    "Users": {"methods": ["GET", "POST", "PUT"], "category": "HR"},
    "WorkersComps": {"methods": ["GET", "POST", "PUT"], "category": "HR"},

    # Organization
    "Branches": {"methods": ["GET"], "category": "Organization"},
    "DivisionIntegrationCodes": {"methods": ["GET"], "category": "Organization"},
    "Divisions": {"methods": ["GET"], "category": "Organization"},
    "Localities": {"methods": ["GET", "POST", "PUT"], "category": "Organization"},
    "Regions": {"methods": ["GET"], "category": "Organization"},

    # Other
    "AttachmentTypes": {"methods": ["GET"], "category": "Other"},
    "Attachments": {"methods": ["GET", "POST"], "category": "Other"},
    "Issues": {"methods": ["POST"], "category": "Other"},
    "ItemAllocations": {"methods": ["GET", "POST", "PUT"], "category": "Other"},
    "ObjectCodes": {"methods": ["GET"], "category": "Other"},
    "Version": {"methods": ["GET"], "category": "System"},
}


def load_config(client="reporting"):
    """Load API config."""
    # Import auth module
    auth = _get_auth_module()
    return auth.load_config(client)


def _get_auth_module():
    """Import the aspire-auth module."""
    auth_path = SCRIPT_DIR / "aspire-auth.py"
    if not auth_path.exists():
        print(f"ERROR: Auth script not found at {auth_path}", file=sys.stderr)
        sys.exit(1)
    import importlib.util
    spec = importlib.util.spec_from_file_location("aspire_auth", auth_path)
    auth_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auth_module)
    return auth_module


def get_token(client="reporting"):
    """Get a valid auth token."""
    auth = _get_auth_module()
    return auth.get_token(client)


def encode_url(base, endpoint, params):
    """Build a properly encoded URL for Aspire API OData requests.

    Key learnings:
    - Spaces must be %20 (not literal)
    - Colons in datetime values must NOT be encoded
    - Single quotes in filter values must be preserved
    - $ prefixes for OData params must be preserved
    """
    url = f"{base.rstrip('/')}/{endpoint}"

    if not params:
        return url

    # Build query string manually for OData compatibility
    parts = []
    for key, value in params.items():
        # Don't encode the $ prefix or = sign
        encoded_value = value.replace(" ", "%20")
        parts.append(f"{key}={encoded_value}")

    return f"{url}?{'&'.join(parts)}"


def query_endpoint(endpoint, params, config, token):
    """Execute a GET request against an Aspire API endpoint."""
    url = encode_url(config["api_base_url"], endpoint, params)

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else []
    except urllib.error.HTTPError as e:
        err_body = e.read().decode() if e.fp else ""
        if e.code == 401:
            print("ERROR: Unauthorized. Token may be expired.", file=sys.stderr)
        elif e.code == 403:
            print(f"ERROR: Forbidden. The API client may not have access to /{endpoint}.", file=sys.stderr)
        elif e.code == 404:
            print(f"ERROR: Endpoint /{endpoint} not found.", file=sys.stderr)
        else:
            print(f"ERROR: HTTP {e.code}: {err_body[:500]}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


def format_json(data, pretty=True):
    """Format data as JSON string."""
    if pretty:
        return json.dumps(data, indent=2, default=str)
    return json.dumps(data, default=str)


def format_csv_str(data):
    """Format data as CSV string."""
    if not data:
        return ""
    records = data if isinstance(data, list) else [data]
    if not records:
        return ""

    # Flatten nested objects
    flat_records = []
    for record in records:
        flat = {}
        for key, value in record.items():
            if isinstance(value, (dict, list)):
                flat[key] = json.dumps(value, default=str)
            else:
                flat[key] = value
        flat_records.append(flat)

    # Collect all keys
    all_keys = []
    seen = set()
    for record in flat_records:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(flat_records)
    return output.getvalue()


def format_table(data, max_col_width=40):
    """Format data as a simple text table."""
    if not data:
        return "No results."
    records = data if isinstance(data, list) else [data]
    if not records:
        return "No results."

    all_keys = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    widths = {}
    for key in all_keys:
        values = [str(r.get(key, ""))[:max_col_width] for r in records]
        widths[key] = min(max(len(key), max((len(v) for v in values), default=0)), max_col_width)

    header = " | ".join(k.ljust(widths[k]) for k in all_keys)
    separator = "-+-".join("-" * widths[k] for k in all_keys)
    rows = []
    for record in records:
        row = " | ".join(str(record.get(k, ""))[:widths[k]].ljust(widths[k]) for k in all_keys)
        rows.append(row)

    return f"{header}\n{separator}\n" + "\n".join(rows)


def list_endpoints():
    """Print all available endpoints organized by category."""
    categories = {}
    for name, info in sorted(ENDPOINTS.items()):
        cat = info["category"]
        if cat not in categories:
            categories[cat] = []
        methods = ", ".join(info["methods"])
        categories[cat].append(f"  {name:40s} [{methods}]")

    for cat in sorted(categories.keys()):
        print(f"\n{cat}:")
        for line in categories[cat]:
            print(line)

    print(f"\nTotal: {len(ENDPOINTS)} endpoints")


def main():
    parser = argparse.ArgumentParser(
        description="Aspire API Query Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s Opportunities --filter "OpportunityStatusName eq 'Won'" --top 20
  %(prog)s Invoices --orderby "DueDate asc" --top 50
  %(prog)s WorkTickets --expand "WorkTicketItems" --select "Id,Status"
  %(prog)s Companies --format csv --output companies.csv
  %(prog)s --list-endpoints
        """
    )

    parser.add_argument("endpoint", nargs="?", help="API endpoint name (e.g., Invoices, WorkTickets)")
    parser.add_argument("--filter", dest="odata_filter", help="OData $filter expression")
    parser.add_argument("--select", help="OData $select fields")
    parser.add_argument("--expand", help="OData $expand navigation properties")
    parser.add_argument("--orderby", help="OData $orderby expression")
    parser.add_argument("--top", type=int, help="Max records to return")
    parser.add_argument("--skip", type=int, help="Records to skip (pagination)")
    parser.add_argument("--format", choices=["json", "csv", "table"], default="json", help="Output format")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--count", action="store_true", help="Just count records")
    parser.add_argument("--raw", action="store_true", help="Raw JSON (no pretty-printing)")
    parser.add_argument("--list-endpoints", action="store_true", help="List all endpoints")
    parser.add_argument("--all", action="store_true", help="Auto-paginate all results")
    parser.add_argument("--page-size", type=int, default=100, help="Page size for --all (default: 100)")
    parser.add_argument("--client", choices=["reporting", "lead"], default="reporting",
                        help="API client to use (default: reporting)")

    args = parser.parse_args()

    if args.list_endpoints:
        list_endpoints()
        return

    if not args.endpoint:
        parser.print_help()
        sys.exit(1)

    # Validate endpoint (case-insensitive match)
    if args.endpoint not in ENDPOINTS:
        matched = [e for e in ENDPOINTS if e.lower() == args.endpoint.lower()]
        if matched:
            args.endpoint = matched[0]
        else:
            print(f"ERROR: Unknown endpoint '{args.endpoint}'", file=sys.stderr)
            suggestions = [e for e in ENDPOINTS if args.endpoint.lower() in e.lower()]
            if suggestions:
                print(f"Did you mean: {', '.join(suggestions[:5])}?", file=sys.stderr)
            print("Run with --list-endpoints to see all.", file=sys.stderr)
            sys.exit(1)

    # Get token
    token = get_token(args.client)

    # Load config for base URL
    config = load_config(args.client)

    # Build OData params
    params = {}
    if args.odata_filter:
        params["$filter"] = args.odata_filter
    if args.select:
        params["$select"] = args.select
    if args.expand:
        params["$expand"] = args.expand
    if args.orderby:
        params["$orderby"] = args.orderby
    if args.top:
        params["$top"] = str(args.top)
    if args.skip:
        params["$skip"] = str(args.skip)

    # Fetch data
    if args.all:
        all_records = []
        skip = 0
        params["$top"] = str(args.page_size)
        while True:
            params["$skip"] = str(skip)
            data = query_endpoint(args.endpoint, params, config, token)
            records = data if isinstance(data, list) else [data] if data else []
            if not records:
                break
            all_records.extend(records)
            if len(records) < args.page_size:
                break
            skip += args.page_size
            print(f"  Fetched {len(all_records)} records...", file=sys.stderr)
        data = all_records
    else:
        data = query_endpoint(args.endpoint, params, config, token)

    # Count mode
    if args.count:
        count = len(data) if isinstance(data, list) else 1
        print(count)
        return

    # Format output
    if args.format == "csv":
        output = format_csv_str(data)
    elif args.format == "table":
        output = format_table(data)
    else:
        output = format_json(data, pretty=not args.raw)

    # Write output
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        count = len(data) if isinstance(data, list) else 1
        print(f"Wrote {count} record(s) to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
