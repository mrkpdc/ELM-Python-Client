##
## Create a single TestCase on ETM/QM 7.1.0 via OSLC
## Modelled on the same structure as ccm_create_workitem.py
##
## Discovers the creation factory URI from the project service document,
## then POSTs a minimal RDF/XML payload to create one TestCase.
##

import logging
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

# ---------------------------------------------------------------------------
# Connection settings -- istanza TARGET (7.1.0)
# ---------------------------------------------------------------------------
jazzhost    = "https://jazz710.local:9443"
username    = "jazzadmin"
password    = "jazzadmin"
jtscontext  = "jts"
qmcontext   = "qm"
projectname = "Test Project createWorkItem (QM)"

# Tipo di artefatto ETM da creare
# Opzioni standard: TestCase, TestPlan, TestScript, TestExecutionRecord
artifact_type = "TestCase"

# ---------------------------------------------------------------------------
# Payload RDF/XML minimale per un TestCase
# ---------------------------------------------------------------------------
PAYLOAD = """\
<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:dcterms="http://purl.org/dc/terms/"
  xmlns:oslc_qm="http://open-services.net/ns/qm#">

  <oslc_qm:TestCase>
    <dcterms:title>Test Case - created via OSLC API</dcterms:title>
    <dcterms:description>Created by the migration test script.</dcterms:description>
  </oslc_qm:TestCase>

</rdf:RDF>
"""

# ---------------------------------------------------------------------------
# Connessione al Jazz Team Server
# ---------------------------------------------------------------------------
elmserver.setupproxy(jazzhost, proxyport=8888)

theserver = elmserver.JazzTeamServer(
    jazzhost,
    username,
    password,
    verifysslcerts=False,
    jtsappstring=f"jts:{jtscontext}",
    appstring="qm",
    cachingcontrol=2,
)

qmapp = theserver.find_app(f"qm:{qmcontext}", ok_to_create=True)

print(f"Connecting to project '{projectname}' ...")
p = qmapp.find_project(projectname)
if p is None:
    raise Exception(f"Project '{projectname}' not found.")
print(f"Project found: {p.name}")

# ---------------------------------------------------------------------------
# Recupera la factory URI dal service document
# ---------------------------------------------------------------------------
NS = {
    "oslc":    "http://open-services.net/ns/core#",
    "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
}

# ETM resource type URI per TestCase
RESOURCE_TYPE = "http://open-services.net/ns/qm#TestCase"

services_xml = p.get_services_xml()
factory_uri  = None

for factory in services_xml.findall(".//oslc:CreationFactory", NS):
    # Cerca la factory che ha oslc:resourceType corrispondente a TestCase
    rtypes = factory.findall("oslc:resourceType", NS)
    rtype_uris = [rt.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "") for rt in rtypes]
    if RESOURCE_TYPE in rtype_uris:
        creation_el = factory.find("oslc:creation", NS)
        if creation_el is not None:
            factory_uri = creation_el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "")
            break

if factory_uri is None:
    # Dump factories disponibili per debug
    print("\nCreation factories disponibili nel service document:")
    for factory in services_xml.findall(".//oslc:CreationFactory", NS):
        title = factory.findtext("dcterms:title", default="(no title)", namespaces=NS)
        el    = factory.find("oslc:creation", NS)
        uri   = el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "") if el is not None else ""
        rtypes = [rt.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "")
                  for rt in factory.findall("oslc:resourceType", NS)]
        print(f"  {title}")
        print(f"    URI:   {uri}")
        print(f"    Types: {rtypes}")
    raise Exception(
        f"Nessuna creation factory trovata per resource type '{RESOURCE_TYPE}'."
    )

print(f"Factory URI: {factory_uri}")

# ---------------------------------------------------------------------------
# POST alla creation factory
# ---------------------------------------------------------------------------
print(f"\nPOSTing TestCase ...")

session = getattr(theserver, '_session', None) \
       or getattr(theserver, 'session',  None) \
       or getattr(qmapp,     '_session', None) \
       or getattr(qmapp,     'session',  None)

if session is None:
    raise Exception(
        "Impossibile ottenere la sessione HTTP da elmclient. "
        "Aggiungi print(dir(theserver)) per ispezionare gli attributi disponibili."
    )

print(f"Sessione HTTP ottenuta: {type(session).__name__}")

response = session.post(
    factory_uri,
    data=PAYLOAD.encode("utf-8"),
    headers={
        "Content-Type":      "application/rdf+xml",
        "Accept":            "application/rdf+xml",
        "OSLC-Core-Version": "2.0",
    },
    verify=False,
)

# ---------------------------------------------------------------------------
# Risultato
# ---------------------------------------------------------------------------
print(f"\nHTTP status: {response.status_code}")

if response.status_code == 201:
    new_uri = response.headers.get("Location", "(no Location header)")
    print(f"SUCCESS -- nuovo TestCase URI:\n  {new_uri}")
else:
    print(f"FAILED -- response body:\n{response.text[:2000]}")
    raise Exception(f"Creazione TestCase fallita con HTTP {response.status_code}.")

print("\nFinished.")