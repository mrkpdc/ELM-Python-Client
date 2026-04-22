##
## migration_import.py
##
## Phase 2 of the ELM migration pipeline.
## Reads artifacts exported by migration_export.py from migration_data/
## and creates them on the TARGET instance (7.1.0) in the correct order.
##
## Creation order (dependency-driven):
##   1. TestScript   (no dependencies)
##   2. TestCase     (depends on TestScript)
##   3. TestPlan     (depends on TestCase)
##   4. ExecutionRecord (depends on TestCase + TestPlan + TestScript)
##   5. TestResult   (depends on ExecutionRecord)
##   6. WorkItems EWM (created last -- links to ETM artifacts added in migration_links.py)
##
## Output:
##   migration_data/mapping_table.json   { source_uri: target_uri }
##
## Re-runnable: artifacts already in mapping_table.json are skipped.
##

import json
import logging
import os
import re

import lxml.etree as ET

import elmclient.server as elmserver
import elmclient.utils as utils

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
loglevel = "INFO,OFF"
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

ewm_projectname = "Test Project 1 (CM)"
etm_projectname = "Test Project (QM)"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR         = "./migration_data"
MAPPING_FILE     = os.path.join(DATA_DIR, "mapping_table.json")
EWM_ATT_DIR      = os.path.join(DATA_DIR, "attachments", "ewm")
ETM_ATT_DIR      = os.path.join(DATA_DIR, "attachments", "etm")
EWM_ATT_INDEX    = os.path.join(DATA_DIR, "attachments", "ewm_attachment_index.json")
ETM_ATT_INDEX    = os.path.join(DATA_DIR, "attachments", "etm_attachment_index.json")

# ---------------------------------------------------------------------------
# OSLC namespaces
# ---------------------------------------------------------------------------
NS = {
    "oslc":    "http://open-services.net/ns/core#",
    "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
}
RDF_RESOURCE = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"

# ---------------------------------------------------------------------------
# Fields to skip when creating artifacts on the target
# (server-managed, read-only, or instance-specific)
# ---------------------------------------------------------------------------
SKIP_FIELDS = {
    "oslc:instanceShape",
    "oslc:serviceProvider",
    "rtc_cm:repository",
    "rtc_cm:progressTracking",
    "rtc_cm:timeSheet",
    "http://open-services.net/ns/pl#schedule",
    "oslc:discussedBy",
    "acc:accessContext",
    "acp:accessControl",
    "process:projectArea",
    "rdf:type",
    "dcterms:identifier",
    "oslc:shortId",
    "oslc:shortTitle",
    "dcterms:created",
    "dcterms:modified",
    "dcterms:creator",
    "dcterms:contributor",
    "rtc_cm:modifiedBy",
    "rtc_cm:resolvedBy",
    "rtc_cm:subscribers",
    "rtc_cm:state",
    # ETM server-managed
    "rqm_qm:orderIndex",
    "rqm_qm:weight",
    "rqm_qm:copiedFrom",
    "rqm_qm:copiedRoot",
    "rqm_qm:currentTestResult",
    "rqm_qm:lastFailedTestResult",
    "rqm_qm:producesTestResult",
    "dcterms:relation",
}

# Fields that are OSLC links to ETM/EWM artifacts -- remapped via mapping table
# (handled separately in migration_links.py, skipped during initial creation)
LINK_FIELDS = {
    # EWM -> ETM
    "oslc_cm1:relatedTestCase",
    "oslc_cm1:relatedTestPlan",
    "oslc_cm1:affectsTestResult",
    # ETM -> EWM
    "oslc_qm:relatedChangeRequest",
    # ETM internal links (non-mandatory -- added by migration_links.py)
    "oslc_qm:usesTestScript",
    "oslc_qm:usesTestCase",
    "rqm_qm:containsTestScriptStep",
    "rqm_qm:containsStepResult",
    # Attachments -- handled separately
    "rtc_cm:com.ibm.team.workitem.linktype.attachment.attachment",
    "rqm_qm:attachment",
}

# Links that are MANDATORY for creation and must be included in the payload,
# remapped via mapping table. If the mapping is missing, creation is skipped.
MANDATORY_LINKS = {
    # ExecutionRecord requires: runsTestCase, reportsOnTestPlan, executesTestScript
    "oslc_qm:runsTestCase",
    "oslc_qm:reportsOnTestPlan",
    "oslc_qm:executesTestScript",
    # TestResult requires: producedByTestExecutionRecord, reportsOnTestCase, reportsOnTestPlan
    "oslc_qm:producedByTestExecutionRecord",
    "oslc_qm:reportsOnTestCase",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def get_factory_uri(services_xml, resource_type_uri: str) -> str | None:
    """Find the creation factory URI for a given OSLC resource type."""
    for factory in services_xml.findall(".//oslc:CreationFactory", NS):
        rtypes = [
            rt.get(RDF_RESOURCE, "")
            for rt in factory.findall("oslc:resourceType", NS)
        ]
        if resource_type_uri in rtypes:
            el = factory.find("oslc:creation", NS)
            if el is not None:
                return el.get(RDF_RESOURCE)
    return None


def get_ewm_factory_uri_by_type(services_xml, workitem_type: str) -> str | None:
    """Find the EWM creation factory URI by work item type segment (e.g. 'task', 'defect')."""
    for factory in services_xml.findall(".//oslc:CreationFactory", NS):
        el = factory.find("oslc:creation", NS)
        if el is None:
            continue
        uri = el.get(RDF_RESOURCE, "")
        if uri.rstrip("/").endswith(f"/{workitem_type}"):
            return uri
    return None


def post_artifact(session, factory_uri: str, payload: str) -> str | None:
    """
    POST an RDF/XML payload to a creation factory.
    Returns the new resource URI (Location header) or None on failure.
    """
    response = session.post(
        factory_uri,
        data=payload.encode("utf-8"),
        headers={
            "Content-Type":      "application/rdf+xml",
            "Accept":            "application/rdf+xml",
            "OSLC-Core-Version": "2.0",
        },
        verify=False,
    )
    if response.status_code == 201:
        return response.headers.get("Location")
    else:
        print(f"  POST failed ({response.status_code}) to {factory_uri}")
        print(f"  Response: {response.text[:2000]}")
        return None


def upload_ewm_attachment(session, jazzhost: str, wi_uri: str, filepath: str,
                          filename: str, content_type: str,
                          project_area_id: str, category_id: str) -> str | None:
    """
    Upload a binary attachment to EWM and link it to a work item.

    Step 1: POST the file to the IAttachmentRestService endpoint
            (discovered by inspecting browser network traffic).
    Step 2: Extract the new attachment URI from the JSON response.
    Step 3: PUT the work item RDF adding the rtc_cm:attachment link.

    Returns the new attachment resource URI or None on failure.
    """
    ct = content_type or "application/octet-stream"

    # Step 1 -- upload the file
    # Endpoint discovered via browser DevTools:
    # POST /ccm/service/com.ibm.team.workitem.service.internal.rest.IAttachmentRestService
    # ?projectId=<projectAreaId>&multiple=true&category=<categoryId>
    upload_url = (
        f"{jazzhost}/ccm/service/com.ibm.team.workitem.service.internal.rest.IAttachmentRestService"
        f"?projectId={project_area_id}&multiple=true"
    )

    try:
        # Warm up the session on the CCM service endpoint before uploading
        session.get(
            upload_url.split("?")[0],
            headers={"Accept": "text/html"},
            verify=False,
        )

        with open(filepath, "rb") as f:
            response = session.post(
                upload_url,
                files={"uploadedFile": (filename, f, ct)},
                headers={"Accept": "application/json"},
                verify=False,
            )

        if response.status_code not in (200, 201):
            print(f"  Attachment upload failed ({response.status_code}): {upload_url}")
            print(f"  {response.text[:500]}")
            return None

        # Step 2 -- parse the JSON response to get the new attachment item ID
        # Response format: [{"id": "_XYZ...", "name": "filename.txt", ...}]
        import json as _json
        resp_data = _json.loads(response.text)
        # Response format: {"files": [{"uuid": "...", "url": "https://...", ...}]}
        files = resp_data.get("files") or (resp_data if isinstance(resp_data, list) else [resp_data])
        if not files:
            print(f"  Could not extract attachment from response: {response.text[:300]}")
            return None
        first = files[0]
        new_att_uri = first.get("url") or (
            f"{jazzhost}/ccm/resource/itemOid/com.ibm.team.workitem.Attachment/{first.get('uuid') or first.get('id')}"
        )
        if not new_att_uri:
            print(f"  Could not extract attachment URI from response: {response.text[:300]}")
            return None
        print(f"    Uploaded: {filename} -> {new_att_uri}")

        # Step 3 -- fetch the work item RDF and add the attachment link
        wi_resp = session.get(
            wi_uri,
            headers={"Accept": "application/rdf+xml", "OSLC-Core-Version": "2.0"},
            verify=False,
        )
        if wi_resp.status_code != 200:
            print(f"  Could not fetch work item for attachment linking ({wi_resp.status_code})")
            return new_att_uri  # attachment uploaded but not linked

        import lxml.etree as _ET
        RDF_NS  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        ATT_TAG = "{http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/}com.ibm.team.workitem.linktype.attachment.attachment"

        parser = _ET.XMLParser(recover=True)
        root   = _ET.fromstring(wi_resp.content, parser=parser)

        # Find main resource element
        main_el = next(
            (c for c in root if c.get(f"{{{RDF_NS}}}about")),
            list(root)[0] if list(root) else None
        )
        if main_el is None:
            print("  Could not find main element in work item RDF")
            return new_att_uri

        # Check if link already present
        existing = {el.get(f"{{{RDF_NS}}}resource", "") for el in main_el.findall(ATT_TAG)}
        if new_att_uri not in existing:
            el = _ET.SubElement(main_el, ATT_TAG)
            el.set(f"{{{RDF_NS}}}resource", new_att_uri)

        updated_rdf = _ET.tostring(root, encoding="utf-8", xml_declaration=True)

        etag = wi_resp.headers.get("ETag")
        put_headers = {
            "Content-Type":      "application/rdf+xml",
            "Accept":            "application/rdf+xml",
            "OSLC-Core-Version": "2.0",
        }
        if etag:
            put_headers["If-Match"] = etag

        put_resp = session.put(wi_uri, data=updated_rdf, headers=put_headers, verify=False)
        if put_resp.status_code not in (200, 204):
            print(f"  Attachment linked but work item PUT failed ({put_resp.status_code})")
        else:
            print(f"    Linked to work item: {wi_uri}")

        return new_att_uri

    except Exception as e:
        print(f"  Attachment upload error: {e}")
        return None


def upload_etm_attachment(session, jazzhost: str, project_area_id: str,
                          filepath: str, filename: str, content_type: str,
                          artifact_uri: str = None, project_name: str = "") -> str | None:
    """
    Upload a binary attachment to ETM.
    Endpoint discovered via browser DevTools:
    POST /qm/service/com.ibm.rqm.planning.service.internal.rest.IAttachmentRestService/
         ?projectId={projectAreaId}
    Returns the new attachment URI or None on failure.
    """
    upload_url = (
        f"{jazzhost}/qm/service/com.ibm.rqm.planning.service.internal.rest.IAttachmentRestService/"
        f"?projectId={project_area_id}"
    )
    ct = content_type or "application/octet-stream"

    try:
        # Warm up the session on the QM service endpoint before uploading
        session.get(
            upload_url.split("?")[0],
            headers={"Accept": "text/html"},
            verify=False,
        )

        with open(filepath, "rb") as f:
            response = session.post(
                upload_url,
                files={"uploadedFile": (filename, f, ct)},
                headers={"Accept": "application/json"},
                verify=False,
            )
        if response.status_code not in (200, 201):
            print(f"  ETM attachment upload failed ({response.status_code}): {upload_url}")
            print(f"  Response: {response.text[:800]}")
            return None

        # ETM response format: <html><body>File Upload Response:{uuid},{size},{id}</body></html>
        # The id (3rd field) is the sequential attachment number used in the urn
        import re as _re_etm
        match = _re_etm.search(r"File Upload Response:([^,<]+),([^,<]+),([^,<]+)", response.text)
        if not match:
            print(f"  Could not parse ETM upload response: {response.text[:300]}")
            return None
        att_id = match.group(3).strip()   # sequential id, e.g. "35"
        # URI format used by ETM 7.1.0 (from RDF inspection):
        # /qm/service/com.ibm.rqm.integration.service.IIntegrationService/resources/{projectName}/attachment/urn:com.ibm.rqm:attachment:{id}
        import urllib.parse as _up
        project_encoded = _up.quote(project_name, safe="")
        new_att_uri = (
            f"{jazzhost}/qm/service/com.ibm.rqm.integration.service.IIntegrationService"
            f"/resources/{project_encoded}/attachment/urn:com.ibm.rqm:attachment:{att_id}"
        )

        print(f"    Uploaded ETM: {filename} -> {new_att_uri}")

        # Link the attachment to the artifact via PUT
        if artifact_uri:
            art_resp = session.get(
                artifact_uri,
                headers={"Accept": "application/rdf+xml", "OSLC-Core-Version": "2.0"},
                verify=False,
            )
            if art_resp.status_code == 200:
                import lxml.etree as _ET2
                RDF_NS2  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                ATT_TAG2 = "{http://jazz.net/ns/qm/rqm#}attachment"
                parser2  = _ET2.XMLParser(recover=True)
                root2    = _ET2.fromstring(art_resp.content, parser2)
                main2    = next(
                    (c for c in root2 if c.get(f"{{{RDF_NS2}}}about")),
                    list(root2)[0] if list(root2) else None
                )
                if main2 is not None:
                    existing2 = {el.get(f"{{{RDF_NS2}}}resource", "") for el in main2.findall(ATT_TAG2)}
                    if new_att_uri not in existing2:
                        el2 = _ET2.SubElement(main2, ATT_TAG2)
                        el2.set(f"{{{RDF_NS2}}}resource", new_att_uri)
                    updated2 = _ET2.tostring(root2, encoding="utf-8", xml_declaration=True)
                    etag2    = art_resp.headers.get("ETag")
                    put_h2   = {"Content-Type": "application/rdf+xml", "Accept": "application/rdf+xml", "OSLC-Core-Version": "2.0"}
                    if etag2:
                        put_h2["If-Match"] = etag2
                    put2 = session.put(artifact_uri, data=updated2, headers=put_h2, verify=False)
                    if put2.status_code in (200, 204):
                        print(f"    Linked to artifact: {artifact_uri}")
                    else:
                        print(f"    Artifact PUT failed ({put2.status_code}): {put2.text[:300]}")
            else:
                print(f"    Could not fetch artifact for linking ({art_resp.status_code})")

        return new_att_uri

    except Exception as e:
        print(f"  ETM attachment upload error: {e}")
        return None


def build_rdf_payload(rdf_type_uri: str, props: dict,
                      skip_fields: set, link_fields: set,
                      mapping: dict, mandatory_links: set = None) -> str:
    """
    Build a minimal RDF/XML payload for artifact creation.
    - Skips server-managed fields (skip_fields)
    - Skips cross-app link fields (link_fields) -- added in migration_links.py
    - Only includes known safe scalar fields
    Uses the specific RDF type tag as root element (e.g. <oslc_qm:TestCase>).
    The generic <rdf:Description rdf:type="..."> form is rejected by ETM/EWM with HTTP 400.
    """

    # Map full type URI -> prefixed XML tag
    TYPE_TAG_MAP = {
        "http://open-services.net/ns/qm#TestScript":          "oslc_qm:TestScript",
        "http://open-services.net/ns/qm#TestCase":            "oslc_qm:TestCase",
        "http://open-services.net/ns/qm#TestPlan":            "oslc_qm:TestPlan",
        "http://open-services.net/ns/qm#TestExecutionRecord": "oslc_qm:TestExecutionRecord",
        "http://open-services.net/ns/qm#TestResult":          "oslc_qm:TestResult",
        "http://open-services.net/ns/cm#ChangeRequest":       "oslc_cm:ChangeRequest",
    }
    if mandatory_links is None:
        mandatory_links = set()
    root_tag = TYPE_TAG_MAP.get(rdf_type_uri, "rdf:Description")

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rdf:RDF',
        '  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"',
        '  xmlns:dcterms="http://purl.org/dc/terms/"',
        '  xmlns:oslc_cm="http://open-services.net/ns/cm#"',
        '  xmlns:oslc_qm="http://open-services.net/ns/qm#"',
        '  xmlns:rtc_cm="http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/"',
        '  xmlns:rqm_qm="http://jazz.net/xmlns/prod/jazz/rqm/qm/1.0/"',
        '  xmlns:oslc_cm1="http://open-services.net/ns/cm-x#"',
        '>',
        f'  <{root_tag}>',
    ]

    # Safe plain-text scalar fields only.
    # URI-valued fields (rqm_qm:scriptType etc.) are excluded here --
    # they require rdf:resource syntax and are handled below.
    # Fields with full-URI keys (http://...) are also excluded to avoid
    # invalid XML tag names.
    SCALAR_FIELDS = {
        "dcterms:title",
        "dcterms:description",
        "dcterms:subject",

        "rqm_qm:verdict",
        "rqm_qm:isCurrent",
        "rqm_qm:isLocked",
        "rqm_qm:numberOfIterations",
    }

    # URI-valued fields that must be rendered as rdf:resource attributes
    URI_FIELDS = {
        "rqm_qm:scriptType",
    }

    for key, val in props.items():
        if key in skip_fields or key in link_fields:
            continue
        if not val:
            continue
        # Skip keys that are full URIs (not valid XML tag names)
        if key.startswith("http://") or key.startswith("https://"):
            continue

        if key in SCALAR_FIELDS:
            text = str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            lines.append(f'    <{key}>{text}</{key}>')
        elif key in URI_FIELDS:
            uri_val = str(val).replace("&", "&amp;")
            lines.append(f'    <{key} rdf:resource="{uri_val}"/>')
        elif key in mandatory_links:
            # Remap via mapping table
            src_val = str(val)
            target_val = mapping.get(src_val, src_val)
            target_val = target_val.replace("&", "&amp;")
            lines.append(f'    <{key} rdf:resource="{target_val}"/>')

    lines.append(f'  </{root_tag}>')
    lines.append('</rdf:RDF>')
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Connect to TARGET
# ---------------------------------------------------------------------------
print("Connecting to TARGET (7.1.0) ...")

elmserver.setupproxy(jazzhost, proxyport=8888)

# EWM connection
ewm_server = elmserver.JazzTeamServer(
    jazzhost, username, password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="ccm",
    cachingcontrol=2,
)
ewm_session = getattr(ewm_server, '_session', None) \
           or getattr(ewm_server, 'session',  None)

ccmapp = ewm_server.find_app(f"ccm:{ccmcontext}", ok_to_create=True)
ewm_p  = ccmapp.find_project(ewm_projectname)
if ewm_p is None:
    raise Exception(f"EWM project '{ewm_projectname}' not found on target.")
print(f"EWM project: {ewm_p.name}")

ewm_services_xml = ewm_p.get_services_xml()

# ETM connection
etm_server = elmserver.JazzTeamServer(
    jazzhost, username, password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="qm",
    cachingcontrol=2,
)
etm_session = getattr(etm_server, '_session', None) \
           or getattr(etm_server, 'session',  None)

qmapp = etm_server.find_app(f"qm:{qmcontext}", ok_to_create=True)
etm_p = qmapp.find_project(etm_projectname)
if etm_p is None:
    raise Exception(f"ETM project '{etm_projectname}' not found on target.")
print(f"ETM project: {etm_p.name}")

etm_services_xml  = etm_p.get_services_xml()
# Extract service document URL from rdf:about of the ServiceProvider element
etm_svc_root     = etm_services_xml.getroot() if hasattr(etm_services_xml, "getroot") else etm_services_xml
etm_services_url = ""
for child in etm_svc_root:
    about = child.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "")
    if about.endswith("services.xml"):
        etm_services_url = about
        break
print(f"ETM services URL: {etm_services_url}")

# ---------------------------------------------------------------------------
# Load mapping table (resume support)
# ---------------------------------------------------------------------------
mapping = load_json(MAPPING_FILE)
print(f"\nMapping table loaded: {len(mapping)} existing entries.")

# ---------------------------------------------------------------------------
# Load exported data
# ---------------------------------------------------------------------------
ewm_workitems      = load_json(os.path.join(DATA_DIR, "ewm_workitems.json"))
etm_testscripts    = load_json(os.path.join(DATA_DIR, "etm_testscripts.json"))
etm_testcases      = load_json(os.path.join(DATA_DIR, "etm_testcases.json"))
etm_testplans      = load_json(os.path.join(DATA_DIR, "etm_testplans.json"))
etm_execrecords    = load_json(os.path.join(DATA_DIR, "etm_executionrecords.json"))
etm_testresults    = load_json(os.path.join(DATA_DIR, "etm_testresults.json"))
ewm_att_index      = load_json(EWM_ATT_INDEX)
etm_att_index      = load_json(ETM_ATT_INDEX)

# ===========================================================================
# STEP 1 -- Create ETM TestScripts
# ===========================================================================
print("\n=== Creating ETM TestScripts ===")

factory_uri = get_factory_uri(etm_services_xml, "http://open-services.net/ns/qm#TestScript")
if not factory_uri:
    print("  WARNING: TestScript factory not found -- skipping.")
else:
    for src_uri, props in etm_testscripts.items():
        if src_uri in mapping:
            print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
            continue

        payload = build_rdf_payload(
            "http://open-services.net/ns/qm#TestScript",
            props, SKIP_FIELDS, LINK_FIELDS, mapping
        )
        new_uri = post_artifact(etm_session, factory_uri, payload)
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  CREATED: {props.get('dcterms:title', src_uri)}")
            print(f"    {src_uri}")
            print(f"    -> {new_uri}")
        else:
            print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 2 -- Create ETM TestCases
# ===========================================================================
print("\n=== Creating ETM TestCases ===")

factory_uri = get_factory_uri(etm_services_xml, "http://open-services.net/ns/qm#TestCase")
if not factory_uri:
    print("  WARNING: TestCase factory not found -- skipping.")
else:
    for src_uri, props in etm_testcases.items():
        if src_uri in mapping:
            print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
            continue

        payload = build_rdf_payload(
            "http://open-services.net/ns/qm#TestCase",
            props, SKIP_FIELDS, LINK_FIELDS, mapping
        )
        new_uri = post_artifact(etm_session, factory_uri, payload)
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  CREATED: {props.get('dcterms:title', src_uri)}")
            print(f"    -> {new_uri}")
        else:
            print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 3 -- Create ETM TestPlans
# ===========================================================================
print("\n=== Creating ETM TestPlans ===")

factory_uri = get_factory_uri(etm_services_xml, "http://open-services.net/ns/qm#TestPlan")
if not factory_uri:
    print("  WARNING: TestPlan factory not found -- skipping.")
else:
    for src_uri, props in etm_testplans.items():
        if src_uri in mapping:
            print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
            continue

        payload = build_rdf_payload(
            "http://open-services.net/ns/qm#TestPlan",
            props, SKIP_FIELDS, LINK_FIELDS, mapping
        )
        new_uri = post_artifact(etm_session, factory_uri, payload)
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  CREATED: {props.get('dcterms:title', src_uri)}")
            print(f"    -> {new_uri}")
        else:
            print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 4 -- Create ETM ExecutionRecords
# ===========================================================================
print("\n=== Creating ETM ExecutionRecords ===")

factory_uri = get_factory_uri(
    etm_services_xml, "http://open-services.net/ns/qm#TestExecutionRecord"
)
if not factory_uri:
    print("  WARNING: TestExecutionRecord factory not found -- skipping.")
else:
    for src_uri, props in etm_execrecords.items():
        if src_uri in mapping:
            print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
            continue

        payload = build_rdf_payload(
            "http://open-services.net/ns/qm#TestExecutionRecord",
            props, SKIP_FIELDS, LINK_FIELDS, mapping, MANDATORY_LINKS
        )
        new_uri = post_artifact(etm_session, factory_uri, payload)
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  CREATED: {props.get('dcterms:title', src_uri)}")
            print(f"    -> {new_uri}")
        else:
            print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 5 -- Create ETM TestResults
# ===========================================================================
print("\n=== Creating ETM TestResults ===")

factory_uri = get_factory_uri(
    etm_services_xml, "http://open-services.net/ns/qm#TestResult"
)
if not factory_uri:
    print("  WARNING: TestResult factory not found -- skipping.")
else:
    for src_uri, props in etm_testresults.items():
        if src_uri in mapping:
            print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
            continue

        payload = build_rdf_payload(
            "http://open-services.net/ns/qm#TestResult",
            props, SKIP_FIELDS, LINK_FIELDS, mapping, MANDATORY_LINKS
        )
        new_uri = post_artifact(etm_session, factory_uri, payload)
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  CREATED: {props.get('dcterms:title', src_uri)}")
            print(f"    -> {new_uri}")
        else:
            print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 6 -- Create EWM WorkItems (without ETM links -- added by migration_links.py)
# ===========================================================================
print("\n=== Creating EWM WorkItems ===")

for src_uri, props in ewm_workitems.items():
    if src_uri in mapping:
        print(f"  SKIP (already mapped): {props.get('dcterms:title', src_uri)}")
        continue

    # Determine work item type from rtc_cm:type URI (last segment)
    type_uri = props.get("rtc_cm:type", "")
    wi_type  = type_uri.rstrip("/").split("/")[-1] if type_uri else "task"

    factory_uri = get_ewm_factory_uri_by_type(ewm_services_xml, wi_type)
    if not factory_uri:
        # Fallback to generic ChangeRequest factory
        factory_uri = get_ewm_factory_uri_by_type(ewm_services_xml, "workitems")
    if not factory_uri:
        print(f"  FAILED (no factory for type '{wi_type}'): {src_uri}")
        continue

    payload = build_rdf_payload(
        "http://open-services.net/ns/cm#ChangeRequest",
        props, SKIP_FIELDS, LINK_FIELDS, mapping
    )
    new_uri = post_artifact(ewm_session, factory_uri, payload)
    if new_uri:
        mapping[src_uri] = new_uri
        save_json(MAPPING_FILE, mapping)
        print(f"  CREATED [{wi_type}]: {props.get('dcterms:title', src_uri)}")
        print(f"    -> {new_uri}")
    else:
        print(f"  FAILED: {props.get('dcterms:title', src_uri)}")

# ===========================================================================
# STEP 7 -- Upload ETM attachments
# ===========================================================================
print("\n=== Uploading ETM attachments ===")

for src_att_uri, saved_filename in etm_att_index.items():
    if src_att_uri in mapping:
        print(f"  SKIP (already uploaded): {saved_filename}")
        continue

    filepath = os.path.join(ETM_ATT_DIR, saved_filename)
    if not os.path.exists(filepath):
        print(f"  MISSING file: {filepath}")
        continue

    # Read meta if available
    meta_path = filepath.rsplit(".", 1)[0] + ".meta.json" if "." in saved_filename else filepath + ".meta.json"
    meta = load_json(meta_path) if os.path.exists(meta_path) else {}
    filename     = meta.get("original_filename") or saved_filename
    content_type = meta.get("content_type", "application/octet-stream")

    # Extract ETM project area ID from services URL
    # URL format: .../qm/oslc_qm/contexts/{projectAreaId}/services.xml
    import re as _re2
    etm_pa_match = _re2.search(r"/contexts/([^/]+)/", etm_services_url)
    etm_pa_id    = etm_pa_match.group(1) if etm_pa_match else ""

    # Find which ETM artifact owns this attachment (search all artifact types)
    target_artifact_uri = None
    etm_all_artifacts = [
        etm_testcases,
        etm_testscripts,
        etm_testplans,
        etm_execrecords,
        etm_testresults,
    ]
    import urllib.parse as _ulp
    # Normalize URI for comparison (decode percent-encoding, normalize + vs %20)
    def _norm(u):
        return _ulp.unquote(u).replace("+", " ")
    src_att_normalized = _norm(src_att_uri)

    for artifact_dict in etm_all_artifacts:
        for art_src_uri, art_props in artifact_dict.items():
            att_val = art_props.get("rqm_qm:attachment", "")
            att_list = att_val if isinstance(att_val, list) else [att_val] if att_val else []
            if any(_norm(a) == src_att_normalized for a in att_list if a):
                target_artifact_uri = mapping.get(art_src_uri)
                break
        if target_artifact_uri:
            break

    if target_artifact_uri:
        print(f"    Owner artifact: {target_artifact_uri}")
    else:
        print(f"    WARNING: no owner artifact found for {src_att_uri}")
        # Debug: check what rqm_qm:attachment values exist in testplans
        for tp_uri, tp_props in etm_testplans.items():
            tp_att = tp_props.get("rqm_qm:attachment", "")
            if tp_att:
                print(f"    TestPlan att value: '{tp_att}' (type: {type(tp_att).__name__})")

    new_att_uri = upload_etm_attachment(
        etm_session, jazzhost, etm_pa_id,
        filepath, filename, content_type,
        artifact_uri=target_artifact_uri,
        project_name=etm_projectname,
    )
    if new_att_uri:
        mapping[src_att_uri] = new_att_uri
        save_json(MAPPING_FILE, mapping)
        print(f"  UPLOADED: {filename} -> {new_att_uri}")
    else:
        print(f"  FAILED: {filename}")

# ===========================================================================
# STEP 8 -- Upload EWM attachments
# ===========================================================================
print("\n=== Uploading EWM attachments ===")

# We need to find which work item owns each attachment
# ewm_workitems stores attachment URIs per work item
att_key = "rtc_cm:com.ibm.team.workitem.linktype.attachment.attachment"

for src_wi_uri, wi_props in ewm_workitems.items():
    target_wi_uri = mapping.get(src_wi_uri)
    if not target_wi_uri:
        print(f"  SKIP (work item not yet migrated): {src_wi_uri}")
        continue

    att_val = wi_props.get(att_key)
    if not att_val:
        continue

    att_uris = att_val if isinstance(att_val, list) else [att_val]
    for src_att_uri in att_uris:
        if not src_att_uri or src_att_uri in mapping:
            continue

        saved_filename = ewm_att_index.get(src_att_uri)
        if not saved_filename:
            print(f"  SKIP (not in index): {src_att_uri}")
            continue

        filepath = os.path.join(EWM_ATT_DIR, saved_filename)
        if not os.path.exists(filepath):
            print(f"  MISSING file: {filepath}")
            continue

        meta_path    = os.path.join(EWM_ATT_DIR, saved_filename.rsplit(".", 1)[0] + ".meta.json")
        meta         = load_json(meta_path) if os.path.exists(meta_path) else {}
        filename     = meta.get("original_filename") or saved_filename
        content_type = meta.get("content_type", "application/octet-stream")

        # Extract project area ID and category ID from work item properties
        # process:projectArea URI ends with the project area ID
        # rtc_cm:filedAgainst URI ends with the category ID
        proj_uri     = wi_props.get("process:projectArea", "")
        cat_uri      = wi_props.get("rtc_cm:filedAgainst", "")
        proj_area_id = proj_uri.rstrip("/").split("/")[-1] if proj_uri else ""
        category_id  = cat_uri.rstrip("/").split("/")[-1] if cat_uri else proj_area_id

        # Remap project area and category to target IDs via mapping table
        # (they are not in mapping table as artifacts, so we derive from target wi)
        # Instead: extract from target work item URI indirectly -- use the
        # ewm_services_xml project area ID which we already know
        target_proj_area_id = ewm_p.fget_project_area_id() if hasattr(ewm_p, "fget_project_area_id") else proj_area_id

        # Extract project area ID from the EWM service document
        # ServiceProvider rdf:about = .../ccm/oslc/contexts/{projectAreaId}/workitems/services.xml
        import re as _re
        svc_root  = ewm_services_xml.getroot() if hasattr(ewm_services_xml, "getroot") else ewm_services_xml
        # ServiceProvider is a child element with rdf:about
        svc_about = ""
        for child in svc_root:
            about = child.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "")
            if "/contexts/" in about:
                svc_about = about
                break
        pa_match     = _re.search(r"/contexts/([^/]+)/", svc_about)
        target_pa_id = pa_match.group(1) if pa_match else proj_area_id

        new_att_uri = upload_ewm_attachment(
            ewm_session, jazzhost, target_wi_uri,
            filepath, filename, content_type,
            project_area_id=target_pa_id,
            category_id=target_pa_id,  # use project area as category fallback
        )
        if new_att_uri:
            mapping[src_att_uri] = new_att_uri
            save_json(MAPPING_FILE, mapping)
            print(f"  UPLOADED: {filename} -> {new_att_uri}")
        else:
            print(f"  FAILED: {filename}")

# ===========================================================================
# Summary
# ===========================================================================
print(f"\n=== Import complete ===")
print(f"Mapping table: {MAPPING_FILE}")
print(f"Total mapped entries: {len(mapping)}")
print("\nNext step: run migration_links.py")