##
## Create a single work item on EWM/CCM 7.1.0 via OSLC
## Uses the creation factory URI discovered from the service document.
##
## Factory URIs per tipo (da service document):
##   task:    .../ccm/oslc/contexts/{projectAreaId}/workitems/task
##   defect:  .../ccm/oslc/contexts/{projectAreaId}/workitems/defect
##   issue:   .../ccm/oslc/contexts/{projectAreaId}/workitems/issue
##   (etc.)
##
## Il tipo è encoded nell'URL stesso -- rtc_cm:type non serve nel payload.
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
ccmcontext  = "ccm"
projectname = "Test Project createWorkItem (CM)"

# Tipo di work item da creare -- deve corrispondere a una factory nel service document
# Opzioni disponibili: task, defect, issue, projectchangerequest, milestone, ...
workitem_type = "task"

# ---------------------------------------------------------------------------
# Payload RDF/XML minimale
# Nessun rtc_cm:type necessario -- la factory URL lo specifica già.
# ---------------------------------------------------------------------------
PAYLOAD = """\
<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:dcterms="http://purl.org/dc/terms/"
  xmlns:oslc_cm="http://open-services.net/ns/cm#">

  <oslc_cm:ChangeRequest>
    <dcterms:title>Test Work Item - created via OSLC API</dcterms:title>
    <dcterms:description>Created by the migration test script.</dcterms:description>
  </oslc_cm:ChangeRequest>

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
    appstring="ccm",
    cachingcontrol=2,
)

ccmapp = theserver.find_app(f"ccm:{ccmcontext}", ok_to_create=True)

print(f"Connecting to project '{projectname}' ...")
p = ccmapp.find_project(projectname)
if p is None:
    raise Exception(f"Project '{projectname}' not found.")
print(f"Project found: {p.name}")

# ---------------------------------------------------------------------------
# Recupera la factory URI per il tipo richiesto dal service document
# ---------------------------------------------------------------------------
NS = {
    "oslc":    "http://open-services.net/ns/core#",
    "rdf":     "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
}

services_xml = p.get_services_xml()
factory_uri  = None

for factory in services_xml.findall(".//oslc:CreationFactory", NS):
    creation_el = factory.find("oslc:creation", NS)
    if creation_el is None:
        continue
    uri = creation_el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "")
    # Match sull'ultimo segmento dell'URI (es. ".../workitems/task")
    if uri.rstrip("/").endswith(f"/{workitem_type}"):
        factory_uri = uri
        break

if factory_uri is None:
    available = []
    for factory in services_xml.findall(".//oslc:CreationFactory", NS):
        el = factory.find("oslc:creation", NS)
        if el is not None:
            u = el.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource", "")
            seg = u.split("/")[-1]
            if seg not in ("workitems", ""):
                available.append(seg)
    raise Exception(
        f"Nessuna creation factory trovata per il tipo '{workitem_type}'. "
        f"Tipi disponibili: {available}"
    )

print(f"Factory URI: {factory_uri}")

# ---------------------------------------------------------------------------
# POST alla creation factory
# Usiamo la sessione autenticata di elmclient tramite theserver.request()
# ---------------------------------------------------------------------------
print(f"\nPOSTing work item di tipo '{workitem_type}' ...")

# Recupera la sessione HTTP autenticata da elmclient.
# elmclient espone la sessione requests tramite l'app object (_session o similar).
# Proviamo i percorsi noti:
session = getattr(theserver, '_session', None) \
       or getattr(theserver, 'session', None) \
       or getattr(ccmapp,    '_session', None) \
       or getattr(ccmapp,    'session',  None)

if session is None:
    # Fallback: elmclient usa internamente httpops -- proviamo a ottenerlo
    try:
        session = theserver.jazz_team_server._get_handler()
    except Exception:
        pass

if session is None:
    raise Exception(
        "Impossibile ottenere la sessione HTTP da elmclient. "
        "Aggiungi un print(dir(theserver)) per ispezionare gli attributi disponibili."
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
    print(f"SUCCESS -- nuovo work item URI:\n  {new_uri}")
else:
    print(f"FAILED -- response body:\n{response.text[:2000]}")
    raise Exception(f"Creazione work item fallita con HTTP {response.status_code}.")

print("\nFinished.")