import re
import requests
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

BASE_FEED_URL = "https://jobs.siemens.com/en_US/externaljobs/SearchJobs/feed/?42386=%5B812078%5D&42386_format=17546&listFilterMode=1"
ID_RE = re.compile(r"/JobDetail/(\d+)", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s\"'<>()]+", re.IGNORECASE)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "content": "http://purl.org/rss/1.0/modules/content/",
}

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"}

def with_params(url: str, **params) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    for k, v in params.items():
        q[k] = [str(v)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def extract_id_from_item(it: ET.Element) -> str:
    # 1) Standard RSS Felder
    link = (it.findtext("link") or "").strip()
    guid = (it.findtext("guid") or "").strip()
    desc = (it.findtext("description") or "").strip()

    # 2) Namespaced Felder
    content_encoded = (it.findtext("content:encoded", namespaces=NS) or "").strip()

    # 3) Atom link in item (falls vorhanden)
    atom_link = ""
    atom_el = it.find("atom:link", namespaces=NS)
    if atom_el is not None:
        atom_link = (atom_el.get("href") or "").strip()

    # Kandidaten in Reihenfolge
    candidates = [link, guid, atom_link, content_encoded, desc]

    # Erst: JobDetail/<id>
    for s in candidates:
        m = ID_RE.search(s or "")
        if m:
            return m.group(1)

    # Dann: falls URL irgendwo eingebettet ist
    for s in candidates:
        for u in URL_RE.findall(s or ""):
            m = ID_RE.search(u)
            if m:
                return m.group(1)

    # Letzter Fallback: letzte Zahl aus Kandidaten
    for s in candidates:
        nums = re.findall(r"\d+", s or "")
        if nums:
            return nums[-1]

    return ""

def fetch(url: str):
    r = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    items = root.findall(".//item")
    ids = []
    for it in items:
        jid = extract_id_from_item(it)
        if jid:
            ids.append(jid)
    return r.url, len(items), ids, r.text[:250]

for off in (0, 20, 40):
    url = with_params(BASE_FEED_URL, folderOffset=off, folderRecordsPerPage=20)
    final_url, n_items, ids, head = fetch(url)
    print("\nOFFSET", off)
    print("items:", n_items, "ids:", len(ids), "sample:", ids[:5])
    if n_items and not ids:
        print("XML head:", head.replace("\n", " "))