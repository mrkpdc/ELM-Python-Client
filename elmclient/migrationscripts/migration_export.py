##
## migration_export.py
##
## Phase 1 of the ELM migration pipeline.
## Reads all artifacts from the SOURCE instance (6.0.2) and saves them to disk
## under migration_data/ for use by migration_import.py.
##
## Output structure:
##   migration_data/
##     ewm_workitems.json
##     etm_testscripts.json
##     etm_testcases.json
##     etm_testplans.json
##     etm_executionrecords.json
##     etm_testresults.json
##     attachments/
##       ewm/<attachment_id>.<ext>        -- binary files
##       ewm/<attachment_id>.meta.json    -- metadata (filename, content-type, workitem_uri)
##       etm/<attachment_id>.<ext>
##       etm/<attachment_id>.meta.json
##
## Re-runnable: if migration_data/ already exists, existing files are overwritten.
##

import csv
import json
import logging
import os
import re
import urllib.parse

import lxml.etree as ET
import requests

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
# Connection settings -- SOURCE instance (6.0.2)
# ---------------------------------------------------------------------------
jazzhost    = "https://jazz602.local:8443"
username    = "jazzadmin"
password    = "jazzadmin"
jtscontext  = "jts"
ccmcontext  = "ccm"
qmcontext   = "qm"

ewm_projectname = "Test Project 3 (CM)"
etm_projectname = "Test Project 3 (QM)"

# Output directory
OUTPUT_DIR = "./migration_data"

# ETM artifact types to export
ETM_ARTIFACT_TYPES = [
    ("TestScripts",           "oslc_qm:TestScriptQuery",           "etm_testscripts"),
    ("TestCases",             "oslc_qm:TestCaseQuery",             "etm_testcases"),
    ("TestPlans",             "oslc_qm:TestPlanQuery",             "etm_testplans"),
    ("TestExecutionRecords",  "oslc_qm:TestExecutionRecordQuery",  "etm_executionrecords"),
    ("TestResults",           "oslc_qm:TestResultQuery",           "etm_testresults"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    """Remove characters that are invalid in filenames."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def props_to_serializable(props: dict) -> dict:
    """
    Convert a props dict from elmclient (may contain non-serializable values)
    to a plain dict safe for json.dumps.
    Multi-valued properties are stored as lists.
    """
    out = {}
    for k, v in props.items():
        if isinstance(v, list):
            out[k] = [str(i) for i in v]
        elif v is None:
            out[k] = None
        else:
            out[k] = str(v)
    return out


def download_attachment(session, url: str, dest_dir: str, attachment_id: str,
                        original_filename: str, content_type: str, source_uri: str):
    """
    Download a binary attachment and save it with metadata.
    Returns the saved filename (without path) or None on failure.
    """
    os.makedirs(dest_dir, exist_ok=True)

    # Determine extension from original filename or content-type
    ext = ""
    if original_filename and "." in original_filename:
        ext = "." + original_filename.rsplit(".", 1)[-1]
    elif content_type:
        ct_map = {
            "application/pdf":  ".pdf",
            "image/png":        ".png",
            "image/jpeg":       ".jpg",
            "image/gif":        ".gif",
            "text/plain":       ".txt",
            "text/html":        ".html",
            "application/zip":  ".zip",
        }
        ext = ct_map.get(content_type.split(";")[0].strip(), ".bin")

    safe_id   = sanitize_filename(attachment_id)
    bin_name  = f"{safe_id}{ext}"
    meta_name = f"{safe_id}.meta.json"
    bin_path  = os.path.join(dest_dir, bin_name)
    meta_path = os.path.join(dest_dir, meta_name)

    try:
        resp = session.get(url, verify=False, stream=True)
        if resp.status_code == 200:
            with open(bin_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            meta = {
                "attachment_id":      attachment_id,
                "original_filename":  original_filename,
                "content_type":       content_type,
                "source_uri":         source_uri,
                "download_url":       url,
                "saved_filename":     bin_name,
            }
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            return bin_name
        else:
            logger.warning(f"  Attachment download failed ({resp.status_code}): {url}")
            return None
    except Exception as e:
        logger.warning(f"  Attachment download error: {e} -- {url}")
        return None


def extract_attachment_url_and_meta(session, attachment_uri: str):
    """
    Given an attachment resource URI from EWM, fetch its metadata
    to get the actual download URL, filename and content-type.
    Returns (download_url, filename, content_type) or (None, None, None).
    """
    try:
        resp = session.get(
            attachment_uri,
            headers={"Accept": "application/rdf+xml", "OSLC-Core-Version": "2.0"},
            verify=False,
        )
        if resp.status_code != 200:
            return None, None, None

        NS = {
            "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "dcterms": "http://purl.org/dc/terms/",
            "oslc":    "http://open-services.net/ns/core#",
            "rtc_cm":  "http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/",
        }
        root = ET.fromstring(resp.content)

        # EWM attachment: dcterms:title = filename, rtc_cm:contentUrl = download URL
        filename     = root.findtext(".//dcterms:title",      namespaces=NS)
        download_url = None
        content_type = None

        # Try rtc_cm:contentUrl
        cu_el = root.find(".//{http://jazz.net/xmlns/prod/jazz/rtc/cm/1.0/}contentUrl")
        if cu_el is not None:
            download_url = cu_el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource") or cu_el.text

        # Try dcterms:format for content-type
        fmt_el = root.find(".//{http://purl.org/dc/terms/}format")
        if fmt_el is not None:
            content_type = fmt_el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource") or fmt_el.text

        # Fallback: use the attachment URI itself as download URL
        if not download_url:
            download_url = attachment_uri

        return download_url, filename, content_type

    except Exception as e:
        logger.warning(f"  Could not fetch attachment metadata: {e}")
        return None, None, None


# ---------------------------------------------------------------------------
# Connect to SOURCE
# ---------------------------------------------------------------------------
elmserver.setupproxy(jazzhost, proxyport=8888)

theserver = elmserver.JazzTeamServer(
    jazzhost,
    username,
    password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="ccm",
    cachingcontrol=2,
)

# Grab the authenticated session for direct HTTP calls (attachments)
session = getattr(theserver, '_session', None) \
       or getattr(theserver, 'session',  None)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# PHASE 1A -- Export EWM work items
# ===========================================================================
print("\n=== EWM: exporting work items ===")

ccmapp = theserver.find_app(f"ccm:{ccmcontext}", ok_to_create=True)
ewm_p  = ccmapp.find_project(ewm_projectname)
if ewm_p is None:
    raise Exception(f"EWM project '{ewm_projectname}' not found.")
print(f"Project found: {ewm_p.name}")

qcbase = ewm_p.get_query_capability_uri("oslc_cm1:ChangeRequest")
if qcbase is None:
    raise Exception("Could not find OSLC query capability for ChangeRequests.")

results = ewm_p.execute_oslc_query(
    qcbase,
    whereterms=None,
    select=["*"],
    show_progress=True,
)
print(f"Retrieved {len(results)} work item(s).")

# Collect attachment URIs while building the workitems dict
ewm_attachments = {}   # { attachment_uri: workitem_uri }

ewm_workitems = {}
for uri, props in results.items():
    serializable = props_to_serializable(props)

    # Identify attachment links
    attachment_key = "rtc_cm:com.ibm.team.workitem.linktype.attachment.attachment"
    att_val = serializable.get(attachment_key)
    if att_val:
        att_uris = att_val if isinstance(att_val, list) else [att_val]
        for att_uri in att_uris:
            if att_uri:
                ewm_attachments[att_uri] = uri

    ewm_workitems[uri] = serializable

# Save workitems JSON
wi_path = os.path.join(OUTPUT_DIR, "ewm_workitems.json")
with open(wi_path, "w", encoding="utf-8") as f:
    json.dump(ewm_workitems, f, indent=2, ensure_ascii=False)
print(f"Saved: {wi_path}")

# ===========================================================================
# PHASE 1B -- Download EWM attachments
# ===========================================================================
print(f"\n=== EWM: downloading {len(ewm_attachments)} attachment(s) ===")

ewm_att_dir = os.path.join(OUTPUT_DIR, "attachments", "ewm")
os.makedirs(ewm_att_dir, exist_ok=True)

ewm_attachment_index = {}   # { attachment_uri: saved_filename }

for att_uri, wi_uri in ewm_attachments.items():
    print(f"  Fetching metadata: {att_uri}")
    dl_url, filename, content_type = extract_attachment_url_and_meta(session, att_uri)

    if dl_url:
        att_id   = att_uri.split("/")[-1]
        saved    = download_attachment(
            session, dl_url, ewm_att_dir, att_id,
            filename or "attachment", content_type or "", att_uri
        )
        if saved:
            ewm_attachment_index[att_uri] = saved
            print(f"    Saved: {saved}")
    else:
        print(f"    Could not resolve download URL for: {att_uri}")

att_index_path = os.path.join(OUTPUT_DIR, "attachments", "ewm_attachment_index.json")
with open(att_index_path, "w", encoding="utf-8") as f:
    json.dump(ewm_attachment_index, f, indent=2)
print(f"Saved attachment index: {att_index_path}")

# ===========================================================================
# PHASE 2 -- Export ETM artifacts
# ===========================================================================
print("\n=== ETM: connecting ===")

# Re-create server object for QM app
etm_server = elmserver.JazzTeamServer(
    jazzhost,
    username,
    password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="qm",
    cachingcontrol=2,
)

qmapp = etm_server.find_app(f"qm:{qmcontext}", ok_to_create=True)
etm_p = qmapp.find_project(etm_projectname)
if etm_p is None:
    raise Exception(f"ETM project '{etm_projectname}' not found.")
print(f"Project found: {etm_p.name}")

etm_session = getattr(etm_server, '_session', None) \
           or getattr(etm_server, 'session',  None)

etm_attachments = {}   # { attachment_uri: artifact_uri }

for label, query_type, filebase in ETM_ARTIFACT_TYPES:
    print(f"\n--- ETM: {label} ---")

    qcbase = etm_p.get_query_capability_uri(query_type)
    if qcbase is None:
        print(f"  Query capability '{query_type}' not found -- skipping.")
        continue

    results = etm_p.execute_oslc_query(
        qcbase,
        whereterms=None,
        select=["*"],
        show_progress=True,
    )

    if not results:
        print("  No artifacts returned.")
        artifacts = {}
    else:
        print(f"  Retrieved {len(results)} artifact(s).")
        artifacts = {}
        for uri, props in results.items():
            serializable = props_to_serializable(props)

            # Collect ETM attachment URIs (rqm_qm:attachment)
            att_val = serializable.get("rqm_qm:attachment")
            if att_val:
                att_uris = att_val if isinstance(att_val, list) else [att_val]
                for att_uri in att_uris:
                    # ETM attachment URIs look like:
                    # https://.../qm/service/.../attachment/urn:com.ibm.rqm:attachment:N
                    if att_uri and "attachment" in att_uri:
                        etm_attachments[att_uri] = uri

            artifacts[uri] = serializable

    out_path = os.path.join(OUTPUT_DIR, f"{filebase}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artifacts, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {out_path}")

# ===========================================================================
# PHASE 2B -- Download ETM attachments
# ===========================================================================
print(f"\n=== ETM: downloading {len(etm_attachments)} attachment(s) ===")

etm_att_dir = os.path.join(OUTPUT_DIR, "attachments", "etm")
os.makedirs(etm_att_dir, exist_ok=True)

etm_attachment_index = {}

for att_uri, artifact_uri in etm_attachments.items():
    print(f"  Downloading: {att_uri}")
    att_id = sanitize_filename(att_uri.split("/")[-1])

    # Fetch Content-Disposition header to get original filename
    original_filename = ""
    content_type      = ""
    try:
        head_resp = etm_session.get(att_uri, verify=False, stream=True)
        cd = head_resp.headers.get("Content-Disposition", "")
        ct = head_resp.headers.get("Content-Type", "")
        content_type = ct.split(";")[0].strip() if ct else ""
        cd_match = re.search(r'filename=["\']?([^"\';\r\n]+)["\']?', cd)
        if cd_match:
            original_filename = cd_match.group(1).strip()
            print(f"    Filename: {original_filename}")
        head_resp.close()
    except Exception as e:
        print(f"    Could not fetch headers: {e}")

    saved = download_attachment(
        etm_session, att_uri, etm_att_dir, att_id,
        original_filename=original_filename,
        content_type=content_type,
        source_uri=att_uri,
    )
    if saved:
        etm_attachment_index[att_uri] = saved
        print(f"    Saved: {saved}")

att_index_path = os.path.join(OUTPUT_DIR, "attachments", "etm_attachment_index.json")
with open(att_index_path, "w", encoding="utf-8") as f:
    json.dump(etm_attachment_index, f, indent=2)
print(f"Saved attachment index: {att_index_path}")

# ===========================================================================
# Summary
# ===========================================================================
print("\n=== Export complete ===")
print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
print(f"  ewm_workitems.json")
for _, _, filebase in ETM_ARTIFACT_TYPES:
    print(f"  {filebase}.json")
print(f"  attachments/ewm/  ({len(ewm_attachment_index)} file(s))")
print(f"  attachments/etm/  ({len(etm_attachment_index)} file(s))")
print("\nNext step: run migration_import.py")