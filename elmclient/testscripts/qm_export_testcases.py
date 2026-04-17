##
## Export test artifacts from an ETM/QM project area to CSV and XML
## Compatible with Jazz server 6.0.2+ / 7.x (ETM / Engineering Test Manager)
##

import csv
import logging
import os

import lxml.etree as ET

import elmclient.server as elmserver
import elmclient.utils as utils
import elmclient.rdfxml as rdfxml

# ---------------------------------------------------------------------------
# Logging setup
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
# Connection settings -- update these for your environment
# ---------------------------------------------------------------------------
jazzhost   = "https://192.168.1.62:8443"   # Jazz server base URL (no trailing slash)
username   = "jazzadmin"
password   = "jazzadmin"

jtscontext = "jts"   # context root for JTS, e.g. "jts" or "jts2"
qmcontext  = "qm"    # context root for ETM, e.g. "qm" or "qm2"

# The exact name of the ETM project area to export
projectname = "Test Project (QM)"

# Component and configuration (leave both None for non-configuration-enabled projects)
# If the project IS configuration-enabled, set these:
componentname = None   # e.g. "My Component"  -- set to None to use the default component
configname    = None   # e.g. "My Stream"      -- set to None for non-config-enabled projects

# ---------------------------------------------------------------------------
# Artifact types to export
# Each entry is:  (label, oslc_query_type, output_filename_base)
# Comment out any types you do not need.
# ---------------------------------------------------------------------------
ARTIFACT_TYPES = [
    ("Test Cases",              "oslc_qm:TestCaseQuery",              "etm_testcases"),
    ("Test Plans",              "oslc_qm:TestPlanQuery",              "etm_testplans"),
    ("Test Scripts",            "oslc_qm:TestScriptQuery",            "etm_testscripts"),
    ("Test Execution Records",  "oslc_qm:TestExecutionRecordQuery",   "etm_executionrecords"),
    ("Test Results",            "oslc_qm:TestResultQuery",            "etm_testresults"),
]

# Output folder
OUTPUT_DIR = r".\elmclient\examples\testscripts\outputfiles"

# Properties to export -- '*' requests all available properties
select_properties = ["*"]

# Caching control
# 0 = use existing cache (fastest on repeat runs)
# 1 = clear cache once, then continue with caching
# 2 = clear cache and disable caching entirely
caching = 2

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resolve_value(queryon, v):
    """If value looks like a URI, resolve it to a human-readable name."""
    s = str(v) if v is not None else ""
    if s.startswith("http://") or s.startswith("https://"):
        resolved = queryon.resolve_uri_to_name(s)
        return resolved if resolved is not None else s
    return s


def resolve_row(queryon, uri, props):
    """Return a flat dict with human-readable keys and values for one artifact."""
    row = {"uri": uri}
    keys = set()
    for k, v in props.items():
        col = queryon.resolve_uri_to_name(k) if (
            k.startswith("http://") or k.startswith("https://")
        ) else k
        if isinstance(v, list):
            row[col] = " | ".join(resolve_value(queryon, item) for item in v)
        else:
            row[col] = resolve_value(queryon, v)
        keys.add(col)
    return row, keys


def write_csv(rows, all_keys, filepath):
    fieldnames = ["uri"] + sorted(all_keys)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_xml(rows, projectname, artifact_label, filepath):
    root = ET.Element("artifacts")
    root.set("project", projectname)
    root.set("type", artifact_label)
    root.set("count", str(len(rows)))

    for row in rows:
        art_el = ET.SubElement(root, "artifact")
        art_el.set("uri", row["uri"])
        for k, v in sorted(row.items()):
            if k == "uri":
                continue
            prop_el = ET.SubElement(art_el, "property")
            prop_el.set("name", k)
            prop_el.text = v if v is not None else ""

    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(filepath, encoding="utf-8", xml_declaration=True)


def export_artifact_type(queryon, label, query_type, filebase):
    print(f"\n--- {label} ---")

    qcbase = queryon.get_query_capability_uri(query_type)
    if qcbase is None:
        print(f"  Query capability '{query_type}' not found -- skipping.")
        return

    results = queryon.execute_oslc_query(
        qcbase,
        whereterms=None,            # no filter = all artifacts
        select=select_properties,
        show_progress=True,
    )

    if not results:
        print("  No artifacts returned.")
        return

    print(f"  Retrieved {len(results)} artifact(s). Resolving names ...")

    rows = []
    all_keys = set()
    for uri, props in results.items():
        row, keys = resolve_row(queryon, uri, props)
        rows.append(row)
        all_keys.update(keys)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_path = os.path.join(OUTPUT_DIR, f"{filebase}.csv")
    write_csv(rows, all_keys, csv_path)
    print(f"  CSV  -> {csv_path}")

    xml_path = os.path.join(OUTPUT_DIR, f"{filebase}.xml")
    write_xml(rows, projectname, label, xml_path)
    print(f"  XML  -> {xml_path}")


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
    appstring="qm",
    cachingcontrol=caching,
)

# ---------------------------------------------------------------------------
# Locate the ETM application and project area
# ---------------------------------------------------------------------------
qmapp = theserver.find_app(f"qm:{qmcontext}", ok_to_create=True)

print(f"Connecting to project '{projectname}' ...")
p = qmapp.find_project(projectname)
if p is None:
    raise Exception(
        f"Project '{projectname}' not found. "
        "Check the project name and your access permissions."
    )
print(f"Project found: {p.name}")

# ---------------------------------------------------------------------------
# Select the query target: component+config (if config-enabled) or project
# ---------------------------------------------------------------------------
if componentname:
    # Configuration-enabled project: query at component level
    c = p.find_local_component(componentname)
    if c is None:
        raise Exception(f"Component '{componentname}' not found in project '{projectname}'.")
    if configname:
        local_config_u = c.get_local_config(configname)
        if local_config_u is None:
            raise Exception(f"Configuration '{configname}' not found in component '{componentname}'.")
        c.set_local_config(local_config_u)
        print(f"Using component '{componentname}', configuration '{configname}'.")
    else:
        print(f"Using component '{componentname}' (no specific configuration selected).")
    queryon = c
else:
    # Non-configuration-enabled project: query at project level
    print("Using project-level query (no component/configuration).")
    queryon = p

# ---------------------------------------------------------------------------
# Load type system so URI values resolve to human-readable names
# ---------------------------------------------------------------------------
print("Loading type system (may take a moment) ...")
queryon.load_types()

# ---------------------------------------------------------------------------
# Export each artifact type
# ---------------------------------------------------------------------------
for label, query_type, filebase in ARTIFACT_TYPES:
    export_artifact_type(queryon, label, query_type, filebase)

print("\nFinished.")
