##
## migration_links.py
##
## Phase 3 of the ELM migration pipeline.
## Reads mapping_table.json and updates artifacts on the TARGET (7.1.0)
## by adding the correct OSLC links (cross-app and internal ETM).
##
## Links recreated:
##   EWM WorkItem -> ETM:
##     oslc_cm1:relatedTestCase
##     oslc_cm1:relatedTestPlan
##     oslc_cm1:affectsTestResult
##   ETM TestCase -> EWM:
##     oslc_qm:relatedChangeRequest
##   ETM TestPlan -> EWM:
##     oslc_qm:relatedChangeRequest
##   ETM TestResult -> EWM:
##     oslc_qm:affectedByChangeRequest
##   ETM internal:
##     TestCase  -> oslc_qm:usesTestScript
##     TestPlan  -> oslc_qm:usesTestCase
##
## Strategy: for each artifact, fetch the current RDF from target,
## then PUT the updated RDF with remapped link URIs.
##
## Re-runnable: links already present on target are not duplicated.
##

import json
import logging
import os

import lxml.etree as ET

import elmclient.server as elmserver
import elmclient.utils as utils

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
loglevel = "INFO,INFO"
levels = [utils.loglevels.get(l, -1) for l in loglevel.split(",", 1)]
if len(levels) < 2:
    levels.append(None)
utils.setup_logging(filelevel=levels[0], consolelevel=levels[1])

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection settings -- TARGET instance (7.1.0)
# ---------------------------------------------------------------------------
jazzhost    = "https://jazz710.local:9443"
username    = "jazzadmin"
password    = "jazzadmin"
jtscontext  = "jts"
ccmcontext  = "ccm"
qmcontext   = "qm"

ewm_projectname = "Test Project 3 (CM)"
etm_projectname = "Test Project 3 (QM)"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR     = "./migration_data"
MAPPING_FILE = os.path.join(DATA_DIR, "mapping_table.json")

# Links to recreate per artifact type.
# Format: { source_prop_key: (target_prop_namespace_uri, target_prop_localname) }
# We use the same key for source and target since the property names don't change.
EWM_LINK_FIELDS = {
    "oslc_cm1:relatedTestCase":  "http://open-services.net/ns/cm#relatedTestCase",
    "oslc_cm1:relatedTestPlan":  "http://open-services.net/ns/cm#relatedTestPlan",
    "oslc_cm1:affectsTestResult":"http://open-services.net/ns/cm#affectsTestResult",
}

ETM_LINK_FIELDS = {
    # TestCase
    "oslc_qm:relatedChangeRequest": "http://open-services.net/ns/qm#relatedChangeRequest",
    "oslc_qm:usesTestScript":       "http://open-services.net/ns/qm#usesTestScript",
    # TestPlan
    "oslc_qm:usesTestCase":         "http://open-services.net/ns/qm#usesTestCase",
    # TestResult
    "oslc_qm:affectedByChangeRequest": "http://open-services.net/ns/qm#affectedByChangeRequest",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fetch_artifact(session, uri: str) -> bytes | None:
    """Fetch an artifact as RDF/XML bytes."""
    resp = session.get(
        uri,
        headers={
            "Accept":            "application/rdf+xml",
            "OSLC-Core-Version": "2.0",
        },
        verify=False,
    )
    if resp.status_code == 200:
        return resp.content
    else:
        print(f"  FETCH failed ({resp.status_code}): {uri}")
        return None


def put_artifact(session, uri: str, rdf_bytes: bytes, etag: str = None) -> bool:
    """PUT updated RDF/XML back to the server."""
    headers = {
        "Content-Type":      "application/rdf+xml",
        "Accept":            "application/rdf+xml",
        "OSLC-Core-Version": "2.0",
    }
    if etag:
        headers["If-Match"] = etag

    resp = session.put(
        uri,
        data=rdf_bytes,
        headers=headers,
        verify=False,
    )
    if resp.status_code in (200, 204):
        return True
    else:
        print(f"  PUT failed ({resp.status_code}): {uri}")
        print(f"  {resp.text[:600]}")
        return False


def get_etag(session, uri: str) -> str | None:
    """HEAD request to get ETag for optimistic locking."""
    resp = session.head(uri, verify=False)
    return resp.headers.get("ETag")


def add_links_to_rdf(rdf_bytes: bytes, links: dict, mapping: dict) -> bytes | None:
    """
    Given RDF/XML bytes of an artifact, add rdf:resource link elements
    for each entry in links dict: { prop_uri: [src_uri, ...] }.
    Remaps URIs via mapping table.
    Returns updated RDF/XML bytes, or None if no changes needed.
    """
    RDF_NS   = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    try:
        parser = ET.XMLParser(recover=True)
        root = ET.fromstring(rdf_bytes, parser=parser)
    except ET.XMLSyntaxError as e:
        print(f"  XML parse error: {e}")
        return None

    # Find the main resource element (first child of rdf:RDF that is not a blank node)
    main_el = None
    for child in root:
        about = child.get(f"{{{RDF_NS}}}about")
        if about:
            main_el = child
            break

    if main_el is None:
        # Fallback: use first child
        children = list(root)
        if children:
            main_el = children[0]
        else:
            print("  No elements found in RDF document")
            return None

    changed = False
    for prop_uri, src_uris in links.items():
        # Split prop_uri into namespace + localname for lxml
        if "#" in prop_uri:
            ns, local = prop_uri.rsplit("#", 1)
            ns = ns + "#"
        elif "/" in prop_uri:
            ns, local = prop_uri.rsplit("/", 1)
            ns = ns + "/"
        else:
            print(f"  Cannot parse prop URI: {prop_uri}")
            continue

        tag = f"{{{ns}}}{local}"

        # Check which target URIs are already present
        existing = set()
        for el in main_el.findall(tag):
            existing.add(el.get(f"{{{RDF_NS}}}resource", ""))

        for src_uri in src_uris:
            target_uri = mapping.get(src_uri, src_uri)
            if target_uri in existing:
                continue  # already present, skip
            el = ET.SubElement(main_el, tag)
            el.set(f"{{{RDF_NS}}}resource", target_uri)
            changed = True
            print(f"    + {local}: {target_uri}")

    if not changed:
        return None

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


# ---------------------------------------------------------------------------
# Connect to TARGET
# ---------------------------------------------------------------------------
print("Connecting to TARGET (7.1.0) ...")

elmserver.setupproxy(jazzhost, proxyport=8888)

ewm_server = elmserver.JazzTeamServer(
    jazzhost, username, password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="ccm",
    cachingcontrol=2,
)
ewm_session = getattr(ewm_server, '_session', None) \
           or getattr(ewm_server, 'session',  None)

# Force login by locating the project area (triggers elmclient authentication)
ccmapp = ewm_server.find_app(f"ccm:{ccmcontext}", ok_to_create=True)
ewm_p  = ccmapp.find_project(ewm_projectname)
if ewm_p is None:
    raise Exception(f"EWM project '{ewm_projectname}' not found on target.")
print(f"EWM project: {ewm_p.name}")

etm_server = elmserver.JazzTeamServer(
    jazzhost, username, password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="qm",
    cachingcontrol=2,
)
etm_session = getattr(etm_server, '_session', None) \
           or getattr(etm_server, 'session',  None)

# Force login for ETM session
qmapp = etm_server.find_app(f"qm:{qmcontext}", ok_to_create=True)
etm_p = qmapp.find_project(etm_projectname)
if etm_p is None:
    raise Exception(f"ETM project '{etm_projectname}' not found on target.")
print(f"ETM project: {etm_p.name}")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
mapping       = load_json(MAPPING_FILE)
ewm_workitems = load_json(os.path.join(DATA_DIR, "ewm_workitems.json"))
etm_testcases = load_json(os.path.join(DATA_DIR, "etm_testcases.json"))
etm_testplans = load_json(os.path.join(DATA_DIR, "etm_testplans.json"))
etm_testresults = load_json(os.path.join(DATA_DIR, "etm_testresults.json"))

print(f"Mapping table: {len(mapping)} entries.")

# ===========================================================================
# STEP 1 -- Add cross-app links to EWM WorkItems
# ===========================================================================
print("\n=== Updating EWM WorkItem links ===")

for src_wi_uri, props in ewm_workitems.items():
    target_wi_uri = mapping.get(src_wi_uri)
    if not target_wi_uri:
        print(f"  SKIP (not in mapping): {src_wi_uri}")
        continue

    # Collect links to add
    links_to_add = {}
    for prop_key, prop_uri in EWM_LINK_FIELDS.items():
        val = props.get(prop_key)
        if not val:
            continue
        src_uris = val if isinstance(val, list) else [val]
        src_uris = [u for u in src_uris if u]
        if src_uris:
            links_to_add[prop_uri] = src_uris

    if not links_to_add:
        continue

    title = props.get("dcterms:title", src_wi_uri)
    print(f"\n  WorkItem: {title}")

    rdf_bytes = fetch_artifact(ewm_session, target_wi_uri)
    if not rdf_bytes:
        continue

    etag      = get_etag(ewm_session, target_wi_uri)
    updated   = add_links_to_rdf(rdf_bytes, links_to_add, mapping)

    if updated:
        ok = put_artifact(ewm_session, target_wi_uri, updated, etag)
        print(f"    {'OK' if ok else 'FAILED'}")
    else:
        print("    (no new links to add)")

# ===========================================================================
# STEP 2 -- Add links to ETM TestCases
# ===========================================================================
print("\n=== Updating ETM TestCase links ===")

TC_LINK_FIELDS = {
    "oslc_qm:relatedChangeRequest": "http://open-services.net/ns/qm#relatedChangeRequest",
    "oslc_qm:usesTestScript":       "http://open-services.net/ns/qm#usesTestScript",
}

for src_tc_uri, props in etm_testcases.items():
    target_tc_uri = mapping.get(src_tc_uri)
    if not target_tc_uri:
        print(f"  SKIP (not in mapping): {src_tc_uri}")
        continue

    links_to_add = {}
    for prop_key, prop_uri in TC_LINK_FIELDS.items():
        val = props.get(prop_key)
        if not val:
            continue
        src_uris = val if isinstance(val, list) else [val]
        src_uris = [u for u in src_uris if u]
        if src_uris:
            links_to_add[prop_uri] = src_uris

    if not links_to_add:
        continue

    title = props.get("dcterms:title", src_tc_uri)
    print(f"\n  TestCase: {title}")

    rdf_bytes = fetch_artifact(etm_session, target_tc_uri)
    if not rdf_bytes:
        continue

    etag    = get_etag(etm_session, target_tc_uri)
    updated = add_links_to_rdf(rdf_bytes, links_to_add, mapping)

    if updated:
        ok = put_artifact(etm_session, target_tc_uri, updated, etag)
        print(f"    {'OK' if ok else 'FAILED'}")
    else:
        print("    (no new links to add)")

# ===========================================================================
# STEP 3 -- Add links to ETM TestPlans
# ===========================================================================
print("\n=== Updating ETM TestPlan links ===")

TP_LINK_FIELDS = {
    "oslc_qm:relatedChangeRequest": "http://open-services.net/ns/qm#relatedChangeRequest",
    "oslc_qm:usesTestCase":         "http://open-services.net/ns/qm#usesTestCase",
}

for src_tp_uri, props in etm_testplans.items():
    target_tp_uri = mapping.get(src_tp_uri)
    if not target_tp_uri:
        print(f"  SKIP (not in mapping): {src_tp_uri}")
        continue

    links_to_add = {}
    for prop_key, prop_uri in TP_LINK_FIELDS.items():
        val = props.get(prop_key)
        if not val:
            continue
        src_uris = val if isinstance(val, list) else [val]
        src_uris = [u for u in src_uris if u]
        if src_uris:
            links_to_add[prop_uri] = src_uris

    if not links_to_add:
        continue

    title = props.get("dcterms:title", src_tp_uri)
    print(f"\n  TestPlan: {title}")

    rdf_bytes = fetch_artifact(etm_session, target_tp_uri)
    if not rdf_bytes:
        continue

    etag    = get_etag(etm_session, target_tp_uri)
    updated = add_links_to_rdf(rdf_bytes, links_to_add, mapping)

    if updated:
        ok = put_artifact(etm_session, target_tp_uri, updated, etag)
        print(f"    {'OK' if ok else 'FAILED'}")
    else:
        print("    (no new links to add)")

# ===========================================================================
# STEP 4 -- Add links to ETM TestResults
# ===========================================================================
print("\n=== Updating ETM TestResult links ===")

TR_LINK_FIELDS = {
    "oslc_qm:affectedByChangeRequest": "http://open-services.net/ns/qm#affectedByChangeRequest",
}

for src_tr_uri, props in etm_testresults.items():
    target_tr_uri = mapping.get(src_tr_uri)
    if not target_tr_uri:
        print(f"  SKIP (not in mapping): {src_tr_uri}")
        continue

    links_to_add = {}
    for prop_key, prop_uri in TR_LINK_FIELDS.items():
        val = props.get(prop_key)
        if not val:
            continue
        src_uris = val if isinstance(val, list) else [val]
        src_uris = [u for u in src_uris if u]
        if src_uris:
            links_to_add[prop_uri] = src_uris

    if not links_to_add:
        continue

    title = props.get("dcterms:title", src_tr_uri)
    print(f"\n  TestResult: {title}")

    rdf_bytes = fetch_artifact(etm_session, target_tr_uri)
    if not rdf_bytes:
        continue

    etag    = get_etag(etm_session, target_tr_uri)
    updated = add_links_to_rdf(rdf_bytes, links_to_add, mapping)

    if updated:
        ok = put_artifact(etm_session, target_tr_uri, updated, etag)
        print(f"    {'OK' if ok else 'FAILED'}")
    else:
        print("    (no new links to add)")

# ===========================================================================
# Summary
# ===========================================================================
print("\n=== Linking complete ===")
print("Verify results in the 7.1.0 UI.")