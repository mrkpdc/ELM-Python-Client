##
## Delete all work items from an EWM/CCM project area via OSLC
## Compatible with Jazz server 6.0.2+ / 7.x (CCM / Engineering Workflow Management)
##
## ⚠️  DESTRUCTIVE OPERATION -- this script permanently deletes work items.
##     By default it runs in DRY-RUN mode (no actual deletions).
##     Set DRY_RUN = False only when you are sure you want to proceed.
##
## Notes on EWM delete behaviour:
##   - EWM exposes HTTP DELETE on the individual work-item resource URI
##     (e.g. /ccm/resource/itemName/com.ibm.team.workitem.WorkItem/<id>).
##   - The OSLC query returns the "about" URI of each work item; that same
##     URI is used for the DELETE request.
##   - The authenticated user must have "Save Work Items" + administrative
##     permissions (or "Delete Work Items" if that role exists) in the
##     project area.
##   - Archived / read-only items may return 403 -- the script logs these
##     and continues rather than aborting.
##   - There is no bulk-delete API; deletions are issued one at a time.
##

import logging
import time

import elmclient.server as elmserver
import elmclient.utils as utils

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
loglevel = "INFO,INFO"   # file level, console level
levels = [utils.loglevels.get(l, -1) for l in loglevel.split(",", 1)]
if len(levels) < 2:
    levels.append(None)
if -1 in levels:
    raise Exception(
        f"Logging level '{loglevel}' not valid - should be comma-separated one or "
        "two values from DEBUG, INFO, WARNING, ERROR, CRITICAL, OFF"
    )
utils.setup_logging(filelevel=levels[0], consolelevel=levels[1])

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration -- update these for your environment
# ---------------------------------------------------------------------------
jazzhost    = "https://jazz710.local:9443"   # Jazz server base URL (no trailing slash)
username    = "jazzadmin"
password    = "jazzadmin"

jtscontext  = "jts"   # context root for JTS
ccmcontext  = "ccm"   # context root for CCM/EWM

# Exact name of the project area whose work items will be deleted
projectname = "Test Project 1 (CM)"

# ---------------------------------------------------------------------------
# Safety flags
# ---------------------------------------------------------------------------

# DRY_RUN = True  --> only lists what WOULD be deleted, no actual HTTP DELETE
# DRY_RUN = False --> performs the real deletions  ⚠️
DRY_RUN = False

# Pause between DELETE requests (seconds).  Helps avoid overwhelming the server.
# Set to 0 for maximum speed (not recommended on large datasets).
DELETE_DELAY_SECONDS = 0.5

# ---------------------------------------------------------------------------
# Connect to the Jazz Team Server
# ---------------------------------------------------------------------------
elmserver.setupproxy(jazzhost, proxyport=8888)

theserver = elmserver.JazzTeamServer(
    jazzhost,
    username,
    password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="ccm",
    cachingcontrol=2,   # always bypass cache so we see the live item list
)

# ---------------------------------------------------------------------------
# Locate the CCM application and project area
# ---------------------------------------------------------------------------
ccmapp = theserver.find_app(f"ccm:{ccmcontext}", ok_to_create=True)

print(f"Connecting to project '{projectname}' ...")
p = ccmapp.find_project(projectname)

if p is None:
    raise Exception(
        f"Project '{projectname}' not found. "
        "Check the project name and your access permissions."
    )

print(f"Project found: {p.name}")

# ---------------------------------------------------------------------------
# Retrieve the authenticated HTTP session from elmclient
# ---------------------------------------------------------------------------
session = (
    getattr(theserver, "_session", None)
    or getattr(theserver, "session",  None)
    or getattr(ccmapp,   "_session", None)
    or getattr(ccmapp,   "session",  None)
)

if session is None:
    raise Exception(
        "Cannot obtain the HTTP session from elmclient. "
        "Inspect dir(theserver) to find the correct attribute name."
    )

print(f"HTTP session obtained: {type(session).__name__}")

# ---------------------------------------------------------------------------
# Query all work items
# ---------------------------------------------------------------------------
qcbase = p.get_query_capability_uri("oslc_cm1:ChangeRequest")

if qcbase is None:
    raise Exception(
        "Could not find an OSLC query capability for 'oslc_cm1:ChangeRequest'. "
        "Verify the project has the Change Management component enabled."
    )

print("Querying all work items (this may take a moment) ...")

# Use ['*'] -- elmclient resolves prefixed names internally only for certain
# query modes; passing '*' is always safe and returns all available properties.
# The URI (key of the results dict) is what we actually need for DELETE anyway.
results = p.execute_oslc_query(
    qcbase,
    whereterms=None,
    select=["*"],
    show_progress=True,
)

if not results:
    print("No work items found in the project. Nothing to delete.")
    exit(0)

total = len(results)
print(f"\nFound {total} work item(s) in '{projectname}'.")

# elmclient in CM mode returns property keys in prefixed form: dcterms:title etc.

# ---------------------------------------------------------------------------
# Extract JSESSIONID for CSRF prevention header.
# EWM 7.x requires   X-Jazz-CSRF-Prevent: <JSESSIONID value>
# on every state-mutating request (DELETE / PUT / POST).
# ---------------------------------------------------------------------------
jsessionid = session.cookies.get("JSESSIONID", "")
if not jsessionid:
    for cookie in session.cookies:
        if cookie.name == "JSESSIONID":
            jsessionid = cookie.value
            break

if jsessionid:
    print(f"JSESSIONID found -- CSRF token ready.")
else:
    print("WARNING: JSESSIONID not found in session cookies.")
    print("         DELETE requests will likely fail with HTTP 403.")
    print("         Check that elmclient has completed authentication before this point.")

# ---------------------------------------------------------------------------
# Deletion loop
# ---------------------------------------------------------------------------
if DRY_RUN:
    print("\n*** DRY-RUN MODE -- no items will actually be deleted ***")
    print("Set DRY_RUN = False in the script to perform real deletions.\n")

deleted   = 0
skipped   = 0
failed    = 0

for idx, (uri, props) in enumerate(results.items(), start=1):
    # elmclient returns keys in prefixed form in CM mode (dcterms:title, dcterms:identifier)
    wi_id    = props.get("dcterms:identifier", props.get("oslc:shortId", "?"))
    wi_title = props.get("dcterms:title", "(no title)")
    label    = f"[{idx}/{total}] #{wi_id} -- {wi_title}"

    if DRY_RUN:
        print(f"  DRY-RUN  {label}")
        print(f"           URI: {uri}")
        skipped += 1
        continue

    # Real DELETE -- X-Jazz-CSRF-Prevent is mandatory on EWM 7.x
    try:
        delete_headers = {
            "Accept":            "application/rdf+xml",
            "OSLC-Core-Version": "2.0",
        }
        if jsessionid:
            delete_headers["X-Jazz-CSRF-Prevent"] = jsessionid

        response = session.delete(
            uri,
            headers=delete_headers,
            verify=False,
        )

        if response.status_code in (200, 204):
            print(f"  DELETED  {label}")
            deleted += 1
        elif response.status_code == 403:
            # Read-only / archived items, or missing permissions
            print(f"  SKIPPED  {label}  (HTTP 403 -- check permissions or item state)")
            logger.warning("403 on DELETE %s: %s", uri, response.text[:500])
            skipped += 1
        elif response.status_code == 404:
            # Already gone -- treat as success
            print(f"  MISSING  {label}  (HTTP 404 -- already deleted?)")
            skipped += 1
        else:
            print(
                f"  FAILED   {label}  "
                f"(HTTP {response.status_code}: {response.text[:200]})"
            )
            logger.error(
                "DELETE failed for %s -- HTTP %s: %s",
                uri, response.status_code, response.text[:500]
            )
            failed += 1

    except Exception as exc:
        print(f"  ERROR    {label}  ({exc})")
        logger.exception("Exception while deleting %s", uri)
        failed += 1

    if DELETE_DELAY_SECONDS > 0:
        time.sleep(DELETE_DELAY_SECONDS)

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
if DRY_RUN:
    print(f"DRY-RUN complete. {total} work item(s) would be deleted.")
    print("Set DRY_RUN = False to perform the actual deletions.")
else:
    print(f"Deletion complete.")
    print(f"  Deleted : {deleted}")
    print(f"  Skipped : {skipped}  (403 / 404 / dry-run)")
    print(f"  Failed  : {failed}")
    print(f"  Total   : {total}")
print("=" * 60)

print("\nFinished.")