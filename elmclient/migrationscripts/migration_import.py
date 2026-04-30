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
import urllib.parse

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

# Source instance host -- used by sanitize_raw_rdf to rewrite URIs
SOURCE_HOST = "https://jazz602.local:8443"

# ---------------------------------------------------------------------------
# EWM workflow state mapping (source state literal -> target action flag name).
#
# EWM state migration mapping.
#
# Key   = last path segment of the source rtc_cm:state URI
#           e.g. "2"  from .../states/2
#                "com.ibm.team.workitem.taskWorkflow.state.s1"
#
# Value = last path segment of the TARGET state URI to set
#           e.g. "bugzillaWorkflow.state.s2"  (In Progress on 7.x)
#         OR empty string "" = initial state, skip transition
#
# How to find the correct value:
#   Open a Defect on jazz710 and manually set it to the desired state.
#   Then GET that work item with Accept: application/rdf+xml and read
#   the rtc_cm:state rdf:resource URI -- take the last path segment.
#
# If source and target have IDENTICAL workflow configuration (same process
# template AND same customizations), leave this map empty -- the script
# will replace the host in the source state URI and use it directly.
# ---------------------------------------------------------------------------
EWM_STATE_MAP: dict[str, str] = {
    # Source state '2' = "In Progress" on 6.0.2.
    # Target "In Progress" in bugzillaWorkflow on 7.1.0 = bugzillaWorkflow.state.s2
    # (confirmed via discover_ewm_states.py)
    "2": "bugzillaWorkflow.state.s2",
    # Initial states -- no transition needed
    "com.ibm.team.workitem.taskWorkflow.state.s1": "",
    "bugzillaWorkflow.state.s1": "",
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR         = "./migration_data"
MAPPING_FILE     = os.path.join(DATA_DIR, "mapping_table.json")
EWM_ATT_DIR      = os.path.join(DATA_DIR, "attachments", "ewm")
ETM_ATT_DIR      = os.path.join(DATA_DIR, "attachments", "etm")
EWM_ATT_INDEX    = os.path.join(DATA_DIR, "attachments", "ewm_attachment_index.json")
ETM_ATT_INDEX    = os.path.join(DATA_DIR, "attachments", "etm_attachment_index.json")
RAW_RDF_DIR      = os.path.join(DATA_DIR, "raw_rdf")
TS_RAW_INDEX     = os.path.join(RAW_RDF_DIR, "etm_testscripts_raw_index.json")
TC_RAW_INDEX     = os.path.join(RAW_RDF_DIR, "etm_testcases_raw_index.json")

# ---------------------------------------------------------------------------
# OSLC namespaces
# ---------------------------------------------------------------------------
NS = {
    "oslc":    "http://open-services.net/ns/core#",
    "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
}
RDF_NS       = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
OSLC_NS      = "http://open-services.net/ns/core#"
RDF_RESOURCE = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"

# ---------------------------------------------------------------------------
# Fields to skip when creating artifacts on the target
# (server-managed, read-only, or instance-specific)
# ---------------------------------------------------------------------------
SKIP_FIELDS = {
    "oslc:instanceShape",
    "oslc:serviceProvider",
    "oslc:discussedBy",
    "rtc_cm:repository",
    "rtc_cm:progressTracking",
    "rtc_cm:timeSheet",
    "rtc_cm:state",       # excluded from POST payload; applied via transition_ewm_state after creation
    "rtc_cm:modifiedBy",
    "rtc_cm:resolvedBy",
    "rtc_cm:subscribers",
    "http://open-services.net/ns/pl#schedule",
    "acc:accessContext",
    # acp:accessControl -- actual ns is http://jazz.net/ns/acp# (NOT open-services acc#)
    # handled via EXTRA_SKIP_TAGS below with Clark notation
    "process:projectArea",
    "rdf:type",
    "dcterms:identifier",
    "dcterms:created",
    "dcterms:modified",
    "dcterms:creator",
    "dcterms:contributor",
    "dcterms:relation",
    "oslc:shortId",
    "oslc:shortTitle",
    # ETM server-managed / computed
    "rqm_qm:copiedFrom",
    "rqm_qm:copiedRoot",
    "rqm_qm:currentTestResult",
    "rqm_qm:lastFailedTestResult",
    "rqm_qm:producesTestResult",
    # ETM versioning
    "oslc_config:versionId",
    "oslc_config:component",
    "oslc_config:configurations",
    "calm:trackedResourceSet",
    # ETM step container props (flat string value, not valid in constructed payload)
    "rqm_qm:containsStepElement",
    "rqm_qm:containsScriptStep",
    "rqm_qm:steps",
}

# Additional Clark-notation tags to strip from raw RDF payloads.
# These use namespace URIs that differ from the prefixes in SKIP_FIELDS
# and would be missed by the prefix-based lookup.
EXTRA_SKIP_TAGS = {
    # acp:accessControl -> http://jazz.net/ns/acp# (not the acc# namespace)
    "{http://jazz.net/ns/acp#}accessControl",
    # process:projectArea -> http://jazz.net/ns/process# (not jazz.net/xmlns/prod/jazz/process)
    "{http://jazz.net/ns/process#}projectArea",
    # rqm_process:hasWorkflowState and hasPriority carry project-specific URI segments
    # but the literal state/priority IDs are STABLE across ETM versions.
    # We keep these fields and let _rewrite() fix the host + project context.
    # --> NOT in EXTRA_SKIP_TAGS
    # rqm_qm:category, template, executionInstructions contain source project name
    "{http://jazz.net/ns/qm/rqm#}category",
    "{http://jazz.net/ns/qm/rqm#}template",
    "{http://open-services.net/ns/qm#}executionInstructions",
    # copiedFrom / copiedRoot
    "{http://jazz.net/ns/qm/rqm#}copiedFrom",
    "{http://jazz.net/ns/qm/rqm#}copiedRoot",
    # acc namespace variant
    "{http://open-services.net/ns/core/acc#}accessContext",
    # ---------------------------------------------------------------------------
    # LINK_FIELDS in Clark notation.
    # Cross-artifact links must be stripped from raw RDF blobs at creation time:
    # the server resolves rdf:resource URIs immediately and returns AQXCM5012E
    # if the referenced artifact doesn't exist yet on the target.
    # migration_links.py re-adds all these links after all artifacts are created.
    # ---------------------------------------------------------------------------
    # ETM -> EWM
    "{http://open-services.net/ns/qm#}relatedChangeRequest",
    # ETM internal (re-added by migration_links.py)
    "{http://open-services.net/ns/qm#}usesTestScript",
    "{http://open-services.net/ns/qm#}usesTestCase",
    "{http://jazz.net/ns/qm/rqm#}containsStepResult",
    # EWM -> ETM cross-app links
    "{http://open-services.net/ns/cm-x#}relatedTestCase",
    "{http://open-services.net/ns/cm-x#}relatedTestPlan",
    "{http://open-services.net/ns/cm-x#}affectsTestResult",
    # Attachments (handled separately in steps 7-8)
    "{http://jazz.net/ns/qm/rqm#}attachment",
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
    """
    Find the creation factory URI for a given OSLC resource type.
    Uses iter() to traverse the entire document including nested ServiceProviders
    (ETM 7.x places execution factories in a child ServiceProvider).
    """
    root = services_xml.getroot() if hasattr(services_xml, "getroot") else services_xml
    for factory in root.iter(f"{{{NS['oslc']}}}CreationFactory"):
        rtypes = [
            rt.get(RDF_RESOURCE, "")
            for rt in factory.findall(f"{{{NS['oslc']}}}resourceType")
        ]
        if resource_type_uri in rtypes:
            el = factory.find(f"{{{NS['oslc']}}}creation")
            if el is not None:
                return el.get(RDF_RESOURCE)
    return None


def get_ewm_factory_uri_by_type(services_xml, workitem_type: str) -> str | None:
    """Find the EWM creation factory URI by work item type segment."""
    root = services_xml.getroot() if hasattr(services_xml, "getroot") else services_xml
    for factory in root.iter(f"{{{NS['oslc']}}}CreationFactory"):
        el = factory.find(f"{{{NS['oslc']}}}creation")
        if el is None:
            continue
        uri = el.get(RDF_RESOURCE, "")
        if uri.rstrip("/").endswith(f"/{workitem_type}"):
            return uri
    return None


def post_artifact(session, factory_uri: str, payload) -> str | None:
    """
    POST an RDF/XML payload (str or bytes) to a creation factory.
    Returns the new resource URI (Location header) or None on failure.
    """
    data = payload.encode("utf-8") if isinstance(payload, str) else payload
    response = session.post(
        factory_uri,
        data=data,
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


def sanitize_raw_rdf(rdf_bytes: bytes, rdf_type_uri: str,
                     skip_fields: set, extra_skip_tags: set,
                     source_host: str, target_host: str,
                     source_project_context: str, target_project_context: str) -> bytes:
    """
    Prepare a raw RDF/XML blob from the source for POST to the target.

    Fixes identified from payload inspection:
    1. Root tag <rdf:Description> -> typed element (e.g. <oslc_qm:TestScript>)
    2. Remove server-managed fields via both prefixed SKIP_FIELDS and
       Clark-notation EXTRA_SKIP_TAGS (catches namespace mismatches like
       acp: -> http://jazz.net/ns/acp# instead of http://jazz.net/xmlns/..)
    3. Rewrite source_host -> target_host AND source project context ID ->
       target project context ID in all URI attributes (fixes containsTestScriptStep)
    4. Strip namespace declarations rejected by ETM 7.x
    """
    PREFIX_MAP = {
        "oslc":        "http://open-services.net/ns/core#",
        "rdf":         "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "dcterms":     "http://purl.org/dc/terms/",
        "rtc_cm":      "http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/",
        "rqm_qm":      "http://jazz.net/xmlns/prod/jazz/rqm/qm/1.0/",
        "oslc_qm":     "http://open-services.net/ns/qm#",
        "oslc_cm":     "http://open-services.net/ns/cm#",
        "oslc_cm1":    "http://open-services.net/ns/cm-x#",
        "process":     "http://jazz.net/xmlns/prod/jazz/process/1.0/",
        "acc":         "http://open-services.net/ns/core/acc#",
        "acp":         "http://jazz.net/xmlns/prod/jazz/jfs/1.0/",
        "calm":        "http://jazz.net/xmlns/prod/jazz/calm/1.0/",
        "oslc_config": "http://open-services.net/ns/config#",
    }
    # Map rdf_type_uri -> typed Clark tag for the main element
    TYPE_TAG = {
        "http://open-services.net/ns/qm#TestScript":
            "{http://open-services.net/ns/qm#}TestScript",
        "http://open-services.net/ns/qm#TestCase":
            "{http://open-services.net/ns/qm#}TestCase",
        "http://open-services.net/ns/qm#TestPlan":
            "{http://open-services.net/ns/qm#}TestPlan",
        "http://open-services.net/ns/qm#TestExecutionRecord":
            "{http://open-services.net/ns/qm#}TestExecutionRecord",
        "http://open-services.net/ns/qm#TestResult":
            "{http://open-services.net/ns/qm#}TestResult",
    }
    RDF_NS_URI   = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    RDF_ABOUT    = f"{{{RDF_NS_URI}}}about"
    RDF_RES_ATTR = f"{{{RDF_NS_URI}}}resource"
    RQM_NS       = "http://jazz.net/ns/qm/rqm#"   # actual ns used in ETM RDF

    # Build tag removal set from prefixed skip_fields + extra_skip_tags
    tags_to_remove: set = set(extra_skip_tags)
    for field in skip_fields:
        if ":" in field and not field.startswith("http"):
            prefix, local = field.split(":", 1)
            ns = PREFIX_MAP.get(prefix)
            if ns:
                tags_to_remove.add(f"{{{ns}}}{local}")
            # Also add the actual ETM rqm_qm namespace variant
            if prefix == "rqm_qm":
                tags_to_remove.add(f"{{{RQM_NS}}}{local}")

    parser = ET.XMLParser(recover=True, remove_comments=True)
    root   = ET.fromstring(rdf_bytes, parser=parser)

    # Find main resource element (first child with rdf:about, or first child)
    main_el = None
    for child in root:
        if child.get(RDF_ABOUT):
            main_el = child
            break
    if main_el is None and list(root):
        main_el = list(root)[0]
    if main_el is None:
        return rdf_bytes

    # 1. Remove server-managed top-level fields
    for child in list(main_el):
        if child.tag in tags_to_remove:
            main_el.remove(child)

    # 2. Replace rdf:Description tag with the typed element tag
    typed_tag = TYPE_TAG.get(rdf_type_uri)
    if typed_tag and main_el.tag == f"{{{RDF_NS_URI}}}Description":
        main_el.tag = typed_tag

    # 3. Remove rdf:about (creation semantics)
    main_el.attrib.pop(RDF_ABOUT, None)

    # 4. Rewrite host and project context ID throughout the document
    def _rewrite(node):
        for attr in (RDF_ABOUT, RDF_RES_ATTR):
            val = node.get(attr)
            if val:
                if source_host in val:
                    val = val.replace(source_host, target_host)
                if source_project_context and source_project_context in val:
                    val = val.replace(source_project_context, target_project_context)
                node.set(attr, val)
        if node.text and source_host in node.text:
            node.text = node.text.replace(source_host, target_host)
        for child in node:
            _rewrite(child)

    _rewrite(root)

    # 5. Strip namespace declarations rejected by ETM 7.x
    NS_TO_STRIP = {
        "http://open-services.net/ns/config#",
        "http://jazz.net/xmlns/prod/jazz/calm/1.0/",
        "http://open-services.net/xmlns/qm/1.0/",
    }
    serialised = ET.tostring(root, encoding="unicode")
    for ns_uri in NS_TO_STRIP:
        serialised = re.sub(
            r'\s+xmlns(?::\w+)?="' + re.escape(ns_uri) + r'"',
            "",
            serialised,
        )
    return serialised.encode("utf-8")




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

        ATT_TAG = "{http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/}com.ibm.team.workitem.linktype.attachment.attachment"

        parser = ET.XMLParser(recover=True)
        root   = ET.fromstring(wi_resp.content, parser=parser)

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
            el = ET.SubElement(main_el, ATT_TAG)
            el.set(f"{{{RDF_NS}}}resource", new_att_uri)

        updated_rdf = ET.tostring(root, encoding="utf-8", xml_declaration=True)

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
                ATT_TAG2 = "{http://jazz.net/ns/qm/rqm#}attachment"
                parser2  = ET.XMLParser(recover=True)
                root2    = ET.fromstring(art_resp.content, parser2)
                main2    = next(
                    (c for c in root2 if c.get(f"{{{RDF_NS}}}about")),
                    list(root2)[0] if list(root2) else None
                )
                if main2 is not None:
                    existing2 = {el.get(f"{{{RDF_NS}}}resource", "") for el in main2.findall(ATT_TAG2)}
                    if new_att_uri not in existing2:
                        el2 = ET.SubElement(main2, ATT_TAG2)
                        el2.set(f"{{{RDF_NS}}}resource", new_att_uri)
                    updated2 = ET.tostring(root2, encoding="utf-8", xml_declaration=True)
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
        # ETM TestResult fields
        "oslc_qm:status",
        "rqm_qm:isCurrent",
        "rqm_qm:isLocked",
        "rqm_qm:numberOfIterations",
        "rqm_qm:orderIndex",
        "rqm_qm:weight",
        "rqm_qm:pointsPassed",
        "rqm_qm:pointsFailed",
        "rqm_qm:pointsBlocked",
        "rqm_qm:pointsAttempted",
        "rqm_qm:pointsDeferred",
        "rqm_qm:pointsInconclusive",
        "rqm_qm:pointsPermFailed",
        "rqm_qm:startTime",
        "rqm_qm:endTime",
    }

    # URI-valued fields that must be rendered as rdf:resource attributes
    URI_FIELDS = {
        "rqm_qm:scriptType",
        "rqm_qm:verdict",
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

# Extract project context IDs for URI rewriting in sanitize_raw_rdf
# Target context comes from the services URL
_tgt_ctx_match = re.search(r"/contexts/([^/]+)/", etm_services_url)
TARGET_ETM_CTX = _tgt_ctx_match.group(1) if _tgt_ctx_match else ""

# Source context: try raw RDF blobs first, then fall back to exported JSON data
SOURCE_ETM_CTX = ""

def _extract_ctx_from_uri(uri: str) -> str:
    m = re.search(r"/contexts/([^/]+)/", uri)
    return m.group(1) if m else ""

# Method 1: scan a raw RDF blob
_ts_idx_path = TS_RAW_INDEX
if not SOURCE_ETM_CTX and os.path.exists(_ts_idx_path):
    _ts_idx = load_json(_ts_idx_path)
    if _ts_idx:
        _first_raw = os.path.join(RAW_RDF_DIR, "testscripts", next(iter(_ts_idx.values())))
        if os.path.exists(_first_raw):
            try:
                _raw_tree = ET.parse(_first_raw)
                _rdf_res  = f"{{{RDF_NS}}}resource"
                _rdf_ab   = f"{{{RDF_NS}}}about"
                for _el in _raw_tree.iter():
                    for _attr in (_rdf_res, _rdf_ab):
                        _ctx = _extract_ctx_from_uri(_el.get(_attr, ""))
                        if _ctx:
                            SOURCE_ETM_CTX = _ctx
                            break
                    if SOURCE_ETM_CTX:
                        break
            except Exception:
                pass

# Method 2: scan URI keys in exported JSON (artifact URIs contain the context)
if not SOURCE_ETM_CTX:
    _etm_data = load_json(os.path.join(DATA_DIR, "etm_testscripts.json"))
    if not _etm_data:
        _etm_data = load_json(os.path.join(DATA_DIR, "etm_testcases.json"))
    for _uri in _etm_data:
        _ctx = _extract_ctx_from_uri(_uri)
        if _ctx:
            SOURCE_ETM_CTX = _ctx
            break

print(f"Source ETM context: {SOURCE_ETM_CTX or '(unknown - check raw_rdf/ exists)'}")
print(f"Target ETM context: {TARGET_ETM_CTX or '(unknown)'}")

# ---------------------------------------------------------------------------
# Load mapping table (resume support)
# ---------------------------------------------------------------------------
mapping = load_json(MAPPING_FILE)
print(f"\nMapping table loaded: {len(mapping)} existing entries.")

# ---------------------------------------------------------------------------
# Load exported data
# ---------------------------------------------------------------------------
ewm_workitems   = load_json(os.path.join(DATA_DIR, "ewm_workitems.json"))
etm_testscripts = load_json(os.path.join(DATA_DIR, "etm_testscripts.json"))
etm_testcases   = load_json(os.path.join(DATA_DIR, "etm_testcases.json"))
etm_testplans   = load_json(os.path.join(DATA_DIR, "etm_testplans.json"))
etm_execrecords = load_json(os.path.join(DATA_DIR, "etm_executionrecords.json"))
etm_testresults = load_json(os.path.join(DATA_DIR, "etm_testresults.json"))
ewm_att_index   = load_json(EWM_ATT_INDEX)
etm_att_index   = load_json(ETM_ATT_INDEX)
ts_raw_index    = load_json(TS_RAW_INDEX)
tc_raw_index    = load_json(TC_RAW_INDEX)


def transition_etm_state(session, artifact_uri: str,
                          target_workflow_state_uri: str) -> bool:
    """
    Attempt to set the workflow state of an ETM artifact after creation.

    ETM ignores hasWorkflowState in the creation POST payload (the factory
    always assigns the initial workflow state).  To migrate state, we:
      1. GET the artifact RDF
      2. Find the current hasWorkflowState element
      3. Replace its rdf:resource with the target state URI
      4. PUT the updated RDF back

    Returns True if the state was updated, False otherwise.
    The function is best-effort: failures are logged but do not abort the run.
    """
    HWS_TAG  = "{http://jazz.net/xmlns/prod/jazz/rqm/process/1.0/}hasWorkflowState"
    RDF_RES  = f"{{{RDF_NS}}}resource"
    RDF_AB   = f"{{{RDF_NS}}}about"

    try:
        resp = session.get(
            artifact_uri,
            headers={"Accept": "application/rdf+xml", "OSLC-Core-Version": "2.0"},
            verify=False,
        )
        if resp.status_code != 200:
            print(f"    [state] GET failed ({resp.status_code}): {artifact_uri}")
            return False

        parser  = ET.XMLParser(recover=True)
        root    = ET.fromstring(resp.content, parser=parser)
        main_el = next(
            (c for c in root if c.get(RDF_AB)),
            list(root)[0] if list(root) else None,
        )
        if main_el is None:
            return False

        hws_el = main_el.find(HWS_TAG)
        if hws_el is None:
            # Add the element if missing
            hws_el = ET.SubElement(main_el, HWS_TAG)

        current = hws_el.get(RDF_RES, "")
        if current == target_workflow_state_uri:
            return True  # already correct

        hws_el.set(RDF_RES, target_workflow_state_uri)
        updated = ET.tostring(root, encoding="utf-8", xml_declaration=True)

        etag = resp.headers.get("ETag")
        put_h = {
            "Content-Type":      "application/rdf+xml",
            "Accept":            "application/rdf+xml",
            "OSLC-Core-Version": "2.0",
        }
        if etag:
            put_h["If-Match"] = etag

        put_resp = session.put(artifact_uri, data=updated, headers=put_h, verify=False)
        if put_resp.status_code in (200, 204):
            print(f"    [state] Set: {target_workflow_state_uri.split('/')[-1]}")
            return True
        else:
            print(f"    [state] PUT failed ({put_resp.status_code}): {put_resp.text[:300]}")
            return False

    except Exception as e:
        print(f"    [state] Error: {e}")
        return False


def create_etm_artifact_raw(src_uri: str, props: dict, rdf_type_uri: str,
                             factory_uri: str, raw_index: dict,
                             raw_subdir: str) -> str | None:
    """
    POST an ETM artifact.  Prefers the raw RDF blob (preserves step elements).
    Falls back to a constructed minimal payload.
    After creation, applies state via a follow-up PUT (ETM factory always
    resets to initial state on creation regardless of payload content).
    Writes the sanitized payload to debug_last_<subdir>_payload.rdf.
    """
    title        = props.get("dcterms:title", src_uri)
    raw_filename = raw_index.get(src_uri)
    raw_path     = os.path.join(RAW_RDF_DIR, raw_subdir, raw_filename) if raw_filename else None

    # Extract source hasWorkflowState URI before sanitizing so we can reapply it
    source_state_uri = None
    HWS_TAG      = "{http://jazz.net/xmlns/prod/jazz/rqm/process/1.0/}hasWorkflowState"
    RDF_RES_ATTR = f"{{{RDF_NS}}}resource"

    if raw_path and os.path.exists(raw_path):
        with open(raw_path, "rb") as _f:
            raw_bytes = _f.read()

        try:
            _parser = ET.XMLParser(recover=True)
            _root   = ET.fromstring(raw_bytes, _parser)
            _main   = next((c for c in _root if c.get(f"{{{RDF_NS}}}about")),
                           list(_root)[0] if list(_root) else None)
            if _main is not None:
                _hws = _main.find(HWS_TAG)
                if _hws is not None:
                    source_state_uri = _hws.get(RDF_RES_ATTR, "")
        except Exception:
            pass

        payload = sanitize_raw_rdf(
            raw_bytes, rdf_type_uri,
            SKIP_FIELDS, EXTRA_SKIP_TAGS,
            SOURCE_HOST, jazzhost,
            SOURCE_ETM_CTX, TARGET_ETM_CTX,
        )
        method = "raw"
    else:
        if raw_filename:
            print(f"  WARN: raw file missing ({raw_path}) -- constructed payload")
        else:
            print(f"  WARN: no raw index entry for {title} -- constructed payload")
        payload = build_rdf_payload(
            rdf_type_uri, props, SKIP_FIELDS, LINK_FIELDS, mapping
        ).encode("utf-8")
        method = "constructed"

    debug_path = os.path.join(DATA_DIR, f"debug_last_{raw_subdir}_payload.rdf")
    with open(debug_path, "wb") as _f:
        _f.write(payload)

    new_uri = post_artifact(etm_session, factory_uri, payload)
    if new_uri:
        print(f"  CREATED [{method}]: {title}")
        print(f"    {src_uri} -> {new_uri}")

        # Follow-up PUT to set workflow state: ETM factory always assigns the
        # initial state regardless of what hasWorkflowState was in the payload.
        if source_state_uri and SOURCE_ETM_CTX and TARGET_ETM_CTX:
            target_state_uri = (
                source_state_uri
                .replace(SOURCE_HOST, jazzhost)
                .replace(SOURCE_ETM_CTX, TARGET_ETM_CTX)
            )
            transition_etm_state(etm_session, new_uri, target_state_uri)
    else:
        print(f"  FAILED [{method}]: {title} -- payload: {debug_path}")
    return new_uri


# ===========================================================================
# STEP 1 -- Create ETM TestScripts  (raw RDF path preserves step elements)
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
        new_uri = create_etm_artifact_raw(
            src_uri, props,
            "http://open-services.net/ns/qm#TestScript",
            factory_uri, ts_raw_index, "testscripts",
        )
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)

# ===========================================================================
# STEP 2 -- Create ETM TestCases  (raw RDF path)
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
        new_uri = create_etm_artifact_raw(
            src_uri, props,
            "http://open-services.net/ns/qm#TestCase",
            factory_uri, tc_raw_index, "testcases",
        )
        if new_uri:
            mapping[src_uri] = new_uri
            save_json(MAPPING_FILE, mapping)

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

        missing = [lf for lf in MANDATORY_LINKS
                   if props.get(lf) and not mapping.get(str(props[lf]))]
        if missing:
            print(f"  SKIP (missing mandatory links {missing}): {props.get('dcterms:title', src_uri)}")
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

        missing = [lf for lf in MANDATORY_LINKS
                   if props.get(lf) and not mapping.get(str(props[lf]))]
        if missing:
            print(f"  SKIP (missing mandatory links {missing}): {props.get('dcterms:title', src_uri)}")
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

def transition_ewm_state(session, wi_uri: str, source_state_uri: str,
                          source_host: str, target_host: str) -> bool:
    """
    EWM state migration placeholder.

    EWM state transitions via OSLC are not supported in 7.x -- the server
    accepts PUT requests but silently ignores state field changes.

    To migrate states correctly, ensure source and target EWM projects use
    identical workflow configuration (same process template with same
    workflow customizations), then populate EWM_STATE_MAP with the correct
    mappings discovered via discover_ewm_states.py.

    For now this function is a no-op that logs the source state for reference.
    """
    src_literal = source_state_uri.rstrip("/").split("/")[-1]
    mapped = EWM_STATE_MAP.get(src_literal, None)
    if mapped == "":
        return True  # initial state, already correct
    if src_literal:
        print(f"    [state] Skipped (source: '{src_literal}') -- "
              f"align workflow config between instances to enable state migration")
    return False



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

        # Apply source workflow state via follow-up PUT
        src_state = props.get("rtc_cm:state", "")
        if src_state:
            transition_ewm_state(ewm_session, new_uri, src_state,
                                  SOURCE_HOST, jazzhost)
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