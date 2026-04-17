##
## Export all work items from an EWM/CCM project area to a CSV file
## Compatible with Jazz server 6.0.2 (CCM / Engineering Workflow Management)
##

import csv
import logging

import lxml.etree as ET

import elmclient.server as elmserver
import elmclient.utils as utils
import elmclient.rdfxml as rdfxml

# ---------------------------------------------------------------------------
# Logging setup
# Use "INFO,INFO" to see info-level messages on the console as well.
# Available levels: DEBUG, TRACE, INFO, WARNING, ERROR, CRITICAL, OFF
# ---------------------------------------------------------------------------
loglevel = "INFO,OFF"
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
# Connection settings  -- update these for your environment
# ---------------------------------------------------------------------------
jazzhost    = "https://192.168.1.62:8443"   # your Jazz server base URL (no trailing slash)
username    = "jazzadmin"
password    = "jazzadmin"

# Context roots -- change if your JTS/CCM are not on the default paths
jtscontext  = "jts"   # e.g. "jts" for /jts, "jts2" for /jts2
ccmcontext  = "ccm"   # e.g. "ccm" for /ccm, "ccm2" for /ccm2

# The exact name of the CCM project area to export
projectname = "Test Project 1 (CM)"

# Output files
outfile     = ".\elmclient\examples\\testscripts\outputfiles\ccm_workitems_export.csv"
xmloutfile  = ".\elmclient\examples\\testscripts\outputfiles\ccm_workitems_export.xml"

# Properties to export (OSLC select).  Use ['*'] to attempt to fetch everything,
# or list specific prefixed properties for a lighter query.
# NOTE: EWM 6.0.2 may not return every property with '*' -- add explicit names
# for anything that is missing.
# select_properties = [
#     "dcterms:identifier",
#     "dcterms:title",
#     "dcterms:description",
#     "oslc_cm:status",
#     "dcterms:creator",
#     "dcterms:created",
#     "dcterms:modified",
#     "dcterms:type",
# ]

select_properties = ['*']

# Caching control
# 0 = use existing cache (fastest for repeat runs)
# 1 = clear cache once, then re-enable caching
# 2 = clear cache and disable caching entirely (always hits the server)
caching = 2

# ---------------------------------------------------------------------------
# Connect to the Jazz Team Server
# ---------------------------------------------------------------------------
# Enable the debug proxy if one is running on localhost:8888 (ignored otherwise)
elmserver.setupproxy(jazzhost, proxyport=8888)

theserver = elmserver.JazzTeamServer(
    jazzhost,
    username,
    password,
    verifysslcerts=False,           # set True if you have valid SSL certs
    jtsappstring=f"jts:{jtscontext}",
    appstring="ccm",                # tell the library the primary app is CCM
    cachingcontrol=caching,
)

# ---------------------------------------------------------------------------
# Locate the CCM application and project area
# ---------------------------------------------------------------------------
ccmapp = theserver.find_app(f"ccm:{ccmcontext}", ok_to_create=True)

print(f"Connecting to project '{projectname}' ...")
p = ccmapp.find_project(projectname)

if p is None:
    raise Exception(
        f"Project '{projectname}' not found.  "
        "Check the project name and your access permissions."
    )

print(f"Project found: {p.name}")

# ---------------------------------------------------------------------------
# Discover the OSLC query capability for ChangeRequests (= work items in CCM)
# ---------------------------------------------------------------------------
qcbase = p.get_query_capability_uri("oslc_cm1:ChangeRequest")

if qcbase is None:
    raise Exception(
        "Could not find an OSLC query capability for 'oslc_cm1:ChangeRequest'. "
        "Verify the project has the Change Management component enabled."
    )

print("Running OSLC query for all work items ...")

# ---------------------------------------------------------------------------
# Execute the query -- no whereterms means return every work item
# ---------------------------------------------------------------------------
results = p.execute_oslc_query(
    qcbase,
    whereterms=None,           # no filter = all work items
    select=select_properties,
    show_progress=True,        # print a progress indicator during paged retrieval
)

if not results:
    print("No work items returned.")
else:
    print(f"Retrieved {len(results)} work item(s).")

    # -----------------------------------------------------------------------
    # Write results to CSV
    # -----------------------------------------------------------------------
    # Collect all column names from all rows so the CSV header is complete
    all_keys = set()
    for uri, props in results.items():
        all_keys.update(props.keys())

    # Always put the URI first, then sort the rest for consistency
    fieldnames = ["uri"] + sorted(all_keys)

    with open(outfile, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for uri, props in results.items():
            row = {"uri": uri}
            for k, v in props.items():
                # Values may be lists (multi-valued); join them with ' | '
                if isinstance(v, list):
                    row[k] = " | ".join(str(item) for item in v)
                else:
                    row[k] = v
            writer.writerow(row)

    print(f"Results written to '{outfile}'.")

    # -----------------------------------------------------------------------
    # Write results to XML
    # -----------------------------------------------------------------------
    root = ET.Element("workitems")
    root.set("project", projectname)
    root.set("count", str(len(results)))

    for uri, props in results.items():
        wi_el = ET.SubElement(root, "workitem")
        wi_el.set("uri", uri)
        for k, v in sorted(props.items()):
            prop_el = ET.SubElement(wi_el, "property")
            prop_el.set("name", k)
            if isinstance(v, list):
                for item in v:
                    val_el = ET.SubElement(prop_el, "value")
                    val_el.text = str(item)
            else:
                prop_el.text = str(v) if v is not None else ""

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(xmloutfile, encoding="utf-8", xml_declaration=True)
    print(f"Results written to '{xmloutfile}'.")

print("Finished.")
