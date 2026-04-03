import requests
import json
import os
import time
import io
import concurrent.futures
import threading
import re
import unicodedata
import sys
from datetime import datetime

# Use pypdf for local extraction
try:
    import pypdf
except ImportError:
    print("[!] pypdf is missing. Install using: pip install pypdf")
    sys.exit(1)

# Global progress tracking
count_lock = threading.Lock()
processed_count = 0
total_to_process = 0

# --- Helper Functions ---

def strip_accents(text):
    if not text: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

def safe_request(url, params=None, headers=None, timeout=30, retries=3):
    """
    Wrapper for requests.get with automatic retries and exponential backoff.
    """
    for i in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 429:
                # Rate limited - wait longer
                wait = (i + 1) * 10
                print(f"    [!] Rate limited (429). Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
            else:
                print(f"    [!] Request failed with status {resp.status_code}. Retrying...", flush=True)
                time.sleep(2)
        except Exception as e:
            print(f"    [!] Connection error: {e}. Retrying...", flush=True)
            time.sleep(3)
    return None

def generate_ai_summary(text, subject=""):
    """
    Cleans the official Diavgeia subject for a human-readable title.
    """
    title = str(subject or "").strip()
    
    if not title or title.upper() == "ΧΩΡΙΣ ΘΕΜΑ":
        if not text: return "Χωρίς Τίτλο"
        clean_text = re.sub(r'\s+', ' ', text).strip()
        sentences = [s.strip() for s in clean_text.split('.') if len(s.strip()) > 20]
        title = sentences[0] if sentences else "Απόφαση"

    # Prefix cleaning rules
    prefix_rules = [
        (r"^(?:ΑΠΟΦΑΣΗ\s+)?ΔΕΣΜΕΥΣΗ(?:Σ)?\s+ΠΟΣΟΥ(?:\s+ΓΙΑ(?:\s+(?:ΤΗΝ|ΤΙΣ|ΤΟΥΣ|ΤΑ|ΤΟ|ΤΟΝ))?)?\s+", "Δέσμευση Ποσού: "),
        (r"^(?:ΑΠΟΦΑΣΗ\s+)?ΕΓΚΡΙΣΗ(?:Σ)?\s+ΔΑΠΑΝΗΣ(?:\s+ΓΙΑ(?:\s+(?:ΤΗΝ|ΤΙΣ|ΤΟΥΣ|ΤΑ|ΤΟ|ΤΟΝ))?)?\s+", "Έγκριση Δαπάνης: "),
        (r"^(?:ΑΠΟΦΑΣΗ\s+)?ΧΟΡΗΓΗΣΗ(?:Σ)?\s+ΑΔΕΙΑΣ(?:\s+ΠΑΡΑΤΑΣΗΣ(?:\s+ΜΟΥΣΙΚΗΣ)?)?\s+", "Χορήγηση Άδειας: "),
        (r"^(?:ΑΠΟΦΑΣΗ\s+)?ΑΝΑΚΛΗΣΗ(?:Σ)?\s+", "Ανάκληση: "),
        (r"^ΑΠΟΦΑΣΗ\s+", "Απόφαση: "),
        (r"^ΕΓΚΡΙΣΗ\s+ΠΡΑΚΤΙΚΟ(?:\S+)?\s+", "Έγκριση Πρακτικού: "),
        (r"^ΕΓΚΡΙΣΗ\s+", "Έγκριση: "),
        (r"^ΠΡΟΜΗΘΕΙΑ\s+", "Προμήθεια: "),
        (r"^ΠΑΡΟΧΗ\s+ΥΠΗΡΕΣΙ(?:ΩΝ|ΑΣ)\s+", "Παροχή Υπηρεσίας: "),
        (r"^(?:ΑΠΕΥΘΕΙΑΣ\s+)?ΑΝΑΘΕΣΗ\s+", "Ανάθεση: "),
        (r"^ΣΥΓΚΡΟΤΗΣΗ\s+", "Συγκρότηση: ")
    ]
    
    prefix_added = ""
    for pattern, replacement in prefix_rules:
        if re.search(pattern, title, flags=re.IGNORECASE):
            title = re.sub(pattern, "", title, flags=re.IGNORECASE).strip()
            prefix_added = replacement
            break

    # Remove typical suffixes
    title = re.sub(r'\(.*\)\s*$', '', title).strip()
    title = re.sub(r'\s+-\s+.*$', '', title).strip()
    
    if not title: 
        title = subject 
    
    title = title.lower().capitalize()
    
    if prefix_added:
        if title.startswith(prefix_added.split(':')[0]):
            title = title[len(prefix_added.split(':')[0]):].strip()
        title = prefix_added + title
    
    if len(title) > 120:
        title = title[:117] + "..."
        
    return title

def is_false_positive(dec):
    """
    Checks if a decision is a false positive (mostly Vrilissia street address).
    """
    subject = strip_accents(dec.get('subject') or "").upper()
    org = strip_accents(dec.get('organizationLabel') or "").upper()
    text = strip_accents(dec.get('documentText') or "").upper()
    
    problematic_markers = ["15235", "ΒΡΙΛΗΣΣΙΑ", "99887093", "ΑΓΙΑΣΟΥ 45", "ΑΓΙΑΣΟΥ 47"]
    has_marker = any(marker in text or marker in subject or marker in org for marker in problematic_markers)
    
    if not has_marker: return False
        
    local_keywords = ["ΜΥΤΙΛΗΝ", "ΛΕΣΒΟ", "ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ", "ΠΑΝΕΠΙΣΤΗΜΙΟ ΑΙΓΑΙΟΥ"]
    is_local_org = any(word in org for word in local_keywords)
    if is_local_org: return False
        
    return True

def fetch_pdf_text(dec):
    global processed_count, total_to_process
    ada = dec.get('ada')
    url = f"https://diavgeia.gov.gr/doc/{ada}"
    
    try:
         resp = safe_request(url, timeout=20)
         if resp and resp.status_code == 200:
             pdf_file = io.BytesIO(resp.content)
             reader = pypdf.PdfReader(pdf_file)
             text = []
             for page in reader.pages:
                 t = page.extract_text()
                 if t:
                     # Clean up extreme whitespace to reduce file size
                     cleaned_text = " ".join(t.split())
                     text.append(cleaned_text)
             
             full_text = " ".join(text)
             dec['documentText'] = full_text
             
             summary = generate_ai_summary(full_text, dec.get('subject'))
             if summary: dec['summary'] = summary
         else:
             dec['documentText'] = "" 
    except Exception as e:
         pass # Leave for next retry
         
    with count_lock:
        processed_count += 1
        if processed_count % 20 == 0:
            print(f"    -> Extractions completed: {processed_count} / {total_to_process}", flush=True)

def fetch_metadata_page(term, page, date_filter, headers):
    params = {
        'fq': date_filter,
        'q': f'q:["{term}"]',
        'sort': 'recent',
        'size': 100,
        'page': page
    }
    resp = safe_request("https://diavgeia.gov.gr/luminapi/api/search", params=params, headers=headers)
    if resp:
        try:
            data = resp.json()
            return data.get('decisions', [])
        except: pass
    return []

def main():
    search_terms = ["ΑΓΙΑΣΟΣ", "ΑΓΙΑΣΟΥ", "ΑΓΙΑΣΟ"]
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'web', 'data.json')
    
    unique_decisions = {}
    existing_texts = {}
    existing_summaries = {}
    
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                for dec in existing_data:
                    ada = dec.get('ada')
                    if ada:
                        unique_decisions[ada] = dec
                        if 'documentText' in dec: existing_texts[ada] = dec['documentText']
                        if 'summary' in dec: existing_summaries[ada] = dec['summary']
            print(f"[*] Loaded {len(unique_decisions)} existing decisions from data.json")
        except Exception as e:
            print(f"[!] Could not load existing DB: {e}")
    
    print("[*] Fetching metadata from Diavgeia (Incremental Search)...")
    from_date_str = "2022-01-01T00:00:00"
    to_date_str = datetime.now().strftime("%Y-%m-%dT23:59:59")
    date_filter = f"issueDate:[DT({from_date_str}) TO DT({to_date_str})]"
    
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for term in search_terms:
        print(f"    -> Tracking term: {term}")
        # Fetch first page to see what's new
        new_items_count = 0
        consecutive_old = 0
        
        # We fetch up to 100 pages, but stop if we hit 50 consecutive existing records
        for p in range(100):
            results = fetch_metadata_page(term, p, date_filter, headers)
            if not results: break
            
            page_is_new = False
            for d in results:
                ada = d.get('ada')
                if ada not in unique_decisions:
                    unique_decisions[ada] = d
                    new_items_count += 1
                    page_is_new = True
                    consecutive_old = 0
                else:
                    consecutive_old += 1
            
            if not page_is_new and consecutive_old > 50:
                print(f"       -> Reached stable data at page {p}. Stopping fetch for {term}.")
                break
        
        print(f"       -> Found {new_items_count} new items for '{term}'.")

    # Standardize and clean
    final_list = []
    start_ts = int(datetime(2022, 1, 1).timestamp() * 1000)
    
    for ada, d in unique_decisions.items():
        issue_date_str = d.get('issueDate', '')
        issue_date_ts = 0
        if isinstance(issue_date_str, str) and "/" in issue_date_str:
            try:
                dt = datetime.strptime(issue_date_str.split()[0], "%d/%m/%Y")
                issue_date_ts = int(dt.timestamp() * 1000)
            except: pass
        else:
            issue_date_ts = d.get('issueDate', 0)
            
        dec = {
            "ada": ada,
            "subject": d.get('subject', 'Χωρίς Θέμα'),
            "issueDate": issue_date_ts,
            "organizationId": d.get('organization', {}).get('uid', d.get('organizationId', '')),
            "organizationLabel": d.get('organization', {}).get('label', d.get('organizationLabel', '')),
            "decisionTypeLabel": d.get('decisionType', {}).get('label', d.get('decisionTypeLabel', '')),
            "documentUrl": d.get('documentUrl', f"https://diavgeia.gov.gr/doc/{ada}")
        }
        
        if ada in existing_texts: dec['documentText'] = existing_texts[ada]
        elif 'documentText' in d: dec['documentText'] = d['documentText']
            
        if ada in existing_summaries: dec['summary'] = existing_summaries[ada]
        elif 'summary' in d: dec['summary'] = d['summary']
        
        # Filter by date and false positives
        if dec['issueDate'] >= start_ts and not is_false_positive(dec):
            final_list.append(dec)

    print(f"[*] Total unique valid decisions since 2022: {len(final_list)}")
    
    # Identify missing full-text (limit to 100 per run to ensure action stability)
    missing_texts = [dec for dec in final_list if 'documentText' not in dec]
    global total_to_process, processed_count
    total_to_process = min(len(missing_texts), 300) # Reduced limit for better stability
    processed_count = 0
    
    if total_to_process > 0:
        print(f"[*] Downloading {total_to_process} PDFs in parallel...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            to_fetch = missing_texts[:total_to_process]
            executor.map(fetch_pdf_text, to_fetch)
            
    # AI Summarization pass for those that have text but no summary
    summarized_count = 0
    for dec in final_list:
        if 'documentText' in dec and 'summary' not in dec:
            summary = generate_ai_summary(dec.get('documentText', ''), dec.get('subject', ''))
            if summary:
                dec['summary'] = summary
                summarized_count += 1
        if summarized_count >= 1000: break
    
    # Final cleanup and sort
    sorted_decisions = sorted(final_list, key=lambda x: x.get('issueDate', 0), reverse=True)
    
    # Save 
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_decisions, f, ensure_ascii=False, indent=1)
        
    print(f"[*] Completed! Database updated at {db_path}")

if __name__ == "__main__":
    main()
