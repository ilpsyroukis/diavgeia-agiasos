import requests
import json
import os
import time
import io
import concurrent.futures
import threading

# Use pypdf for local extraction
try:
    import pypdf
except ImportError:
    print("[!] pypdf is missing. Install using: pip install pypdf")
    import sys
    sys.exit(1)

count_lock = threading.Lock()
processed_count = 0
total_to_process = 0

import re
import unicodedata

def strip_accents(text):
    if not text: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
def generate_ai_summary(text, subject=""):
    """
    Cleans the official Diavgeia subject for a human-readable title.
    Instead of aggressively deleting prefixes, we reformat them to be descriptive summaries.
    """
    title = str(subject or "").strip()
    
    if not title or title.upper() == "ΧΩΡΙΣ ΘΕΜΑ":
        if not text: return "Χωρίς Τίτλο"
        clean_text = re.sub(r'\s+', ' ', text).strip()
        sentences = [s.strip() for s in clean_text.split('.') if len(s.strip()) > 20]
        title = sentences[0] if sentences else "Απόφαση"

    # Define how to categorize and summarize the heavy administrative prefixes
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

    # Remove typical suffixes in parentheses or after dash
    title = re.sub(r'\(.*\)\s*$', '', title).strip()
    title = re.sub(r'\s+-\s+.*$', '', title).strip()
    
    if not title: 
        title = subject 
    
    # Capitalize the remaining string (which is usually uppercase Greek without accents)
    # The .lower().capitalize() will make it e.g. "Συντηρηση πλυντηριου"
    title = title.lower().capitalize()
    
    if prefix_added:
        # Prevent double starting like "Απόφαση: Απόφαση..."
        if title.startswith(prefix_added.split(':')[0]):
            title = title[len(prefix_added.split(':')[0]):].strip()
        title = prefix_added + title
    
    if len(title) > 120:
        title = title[:117] + "..."
        
    return title

def is_false_positive(dec):
    """
    Checks if a decision is a false positive based on common search noises.
    Specifically targets the Vrilissia address "Odos Agiasou" which is not related to the village.
    """
    subject = strip_accents(dec.get('subject') or "").upper()
    org = strip_accents(dec.get('organizationLabel') or "").upper()
    text = strip_accents(dec.get('documentText') or "").upper()
    
    # Signatures of the common false positive (Vrilissia company/address)
    # The zip code 15235 and the area VRILISSIA are key markers.
    problematic_markers = ["15235", "ΒΡΙΛΗΣΣΙΑ", "99887093", "ΑΓΙΑΣΟΥ 45", "ΑΓΙΑΣΟΥ 47"]
    
    has_marker = any(marker in text or marker in subject or marker in org for marker in problematic_markers)
    
    if not has_marker:
        return False
        
    # If it has the marker, we verify if it has a VALID local connection.
    # If the organization is from Lesvos/Agiasos/Mytilene, it's NOT a false positive.
    # We use partial stems for robustness (e.g., ΜΥΤΙΛΗΝ covers ΜΥΤΙΛΗΝΗΣ, ΜΥΤΙΛΗΝΗ).
    local_keywords = ["ΜΥΤΙΛΗΝ", "ΛΕΣΒΟ", "ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ", "ΠΑΝΕΠΙΣΤΗΜΙΟ ΑΙΓΑΙΟΥ"]
    
    is_local_org = any(word in org for word in local_keywords)
    if is_local_org:
        return False
        
    # If it's NOT a local org but has Vrilissia markers (ZIP, Area, or the specific address block), 
    # it's a false positive regardless of whether "ΑΓΙΑΣΟ" is in the subject (as it's likely a street address).
    return True

def fetch_pdf_text(dec):
    global processed_count, total_to_process
    ada = dec.get('ada')
    url = f"https://diavgeia.gov.gr/doc/{ada}"
    
    try:
         # Timeout sets for robustness
         resp = requests.get(url, timeout=20)
         if resp.status_code == 200:
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
             
             # Generate primary title from cleaned subject
             summary = generate_ai_summary(full_text, dec.get('subject'))
             if summary:
                 dec['summary'] = summary
         else:
             dec['documentText'] = "" # Mark as empty to avoid retrying on 404s
    except Exception as e:
         # On failure, leave it without 'documentText' so it can be retried next time
         pass
         
    with count_lock:
        processed_count += 1
        if processed_count % 50 == 0:
            print(f"    -> Extractions completed: {processed_count} / {total_to_process}", flush=True)

def main():
    base_url = "https://diavgeia.gov.gr/opendata/search.json"
    search_terms = ["ΑΓΙΑΣΟΣ", "ΑΓΙΑΣΟΥ", "ΑΓΙΑΣΟ"]
    
    unique_decisions = {}
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'web', 'data.json')
    
    # 1. Load existing DB to keep cached texts
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
                        if 'documentText' in dec:
                            existing_texts[ada] = dec['documentText']
                        if 'summary' in dec:
                            existing_summaries[ada] = dec['summary']
            print(f"[*] Loaded {len(unique_decisions)} existing decisions from data.json")
        except Exception as e:
            print(f"[!] Could not load existing DB: {e}")
    
    print("[*] Fetching metadata from Diavgeia LuminAPI (Parallel Search)...")
    from datetime import datetime
    import concurrent.futures

    # Set date range
    from_date_str = "2022-01-01T00:00:00"
    to_date_str = datetime.now().strftime("%Y-%m-%dT23:59:59")
    date_filter = f"issueDate:[DT({from_date_str}) TO DT({to_date_str})]"
    
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    def fetch_metadata_page(term, page):
        params = {
            'fq': date_filter,
            'q': f'q:["{term}"]',
            'sort': 'recent',
            'size': 100,
            'page': page
        }
        try:
            resp = requests.get("https://diavgeia.gov.gr/luminapi/api/search", params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('decisions', [])
        except Exception as e:
            print(f"[!] Error on page {page}: {e}")
        return []

    for term in search_terms:
        # 1. Get first page to find total count
        first_page = fetch_metadata_page(term, 0)
        if not first_page: continue
        
        # Simple hack to get total from first page response (LuminAPI usually has it in info)
        # We'll just fetch many pages until empty or we hit a reasonable limit
        # Or better: just fetch 100 pages (10k items) in parallel
        pages_to_fetch = range(1, 100) # Since we know it's ~9000 items
        
        # Add first page results
        for d in first_page:
            ada = d.get('ada')
            if ada: unique_decisions[ada] = d

        print(f"    -> {term}: Starting parallel fetch of remaining pages...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_page = {executor.submit(fetch_metadata_page, term, p): p for p in pages_to_fetch}
            for future in concurrent.futures.as_completed(future_to_page):
                results = future.result()
                if results:
                    for d in results:
                        ada = d.get('ada')
                        if ada: unique_decisions[ada] = d

    # Standardize and preserve existing documentText
    final_list = []
    for ada, d in unique_decisions.items():
        # Convert date
        issue_date_str = d.get('issueDate', '')
        issue_date_ts = 0
        if issue_date_str:
            try:
                dt = datetime.strptime(issue_date_str.split()[0], "%d/%m/%Y")
                issue_date_ts = int(dt.timestamp() * 1000)
            except: pass
            
        dec = {
            "ada": ada,
            "subject": d.get('subject', 'Χωρίς Θέμα'),
            "issueDate": issue_date_ts if issue_date_ts else d.get('issueDate', 0),
            "organizationId": d.get('organization', {}).get('uid', d.get('organizationId', '')),
            "organizationLabel": d.get('organization', {}).get('label', d.get('organizationLabel', '')),
            "decisionTypeLabel": d.get('decisionType', {}).get('label', d.get('decisionTypeLabel', '')),
            "documentUrl": d.get('documentUrl', f"https://diavgeia.gov.gr/doc/{ada}")
        }
        if ada in existing_texts:
            dec['documentText'] = existing_texts[ada]
        elif 'documentText' in d:
            dec['documentText'] = d['documentText']
            
        if ada in existing_summaries:
            dec['summary'] = existing_summaries[ada]
        elif 'summary' in d:
            dec['summary'] = d['summary']
        
        final_list.append(dec)

    # Final cleanup: Ensure everything is since 2022 AND not a false positive
    start_ts = int(datetime(2022, 1, 1).timestamp() * 1000)
    unique_decisions_list = [
        v for v in final_list 
        if v.get('issueDate', 0) >= start_ts and not is_false_positive(v)
    ]
    
    removed_count = len(final_list) - len(unique_decisions_list)
    if removed_count > 0:
        print(f"[*] Filtered out {removed_count} false positive documents (e.g., Vrilissia).")

    # 2. Identify missing documentTexts
    missing_texts = [dec for dec in unique_decisions_list if 'documentText' not in dec]
    global total_to_process, processed_count
    total_to_process = len(missing_texts)
    processed_count = 0
    
    print(f"[*] Total unique decisions found since 2022: {len(unique_decisions_list)}")
    print(f"[*] {total_to_process} decisions are missing full-text.")
    
    # Sort for final save
    sorted_decisions = sorted(unique_decisions_list, key=lambda x: x.get('issueDate', 0), reverse=True)

    # Save initial metadata (with organization labels) immediately
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_decisions, f, ensure_ascii=False, indent=1)
    print(f"[*] Saved metadata for {len(sorted_decisions)} decisions to {db_path}")

    if total_to_process > 0:
        print("[*] Downloading and parsing PDFs in parallel (limit 1000 per run to prevent timeout)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            to_fetch = missing_texts[:1000] # First 1000 PDFs in this pass
            executor.map(fetch_pdf_text, to_fetch)
            
    # 4. AI Summarization pass - Updating first 1000 items with new cleaner logic
    print("[*] Updating first 1000 AI summaries with new cleaner logic...")
    summarized_count = 0
    for dec in sorted_decisions:
        if ('documentText' in dec or 'subject' in dec) and 'summary' not in dec:
            summary = generate_ai_summary(dec.get('documentText', ''), dec.get('subject', ''))
            if summary:
                dec['summary'] = summary
                summarized_count += 1
        if summarized_count >= 1000: break
    print(f"[*] AI Summarization pass completed: {summarized_count} summaries updated.")

    # Final Sub-Filtering after PDF downloading is done
    original_len = len(sorted_decisions)
    sorted_decisions = [v for v in sorted_decisions if not is_false_positive(v)]
    removed = original_len - len(sorted_decisions)
    if removed > 0:
        print(f"[*] Filtered out {removed} false positive documents after full-text extraction.")

    # Final Save after PDF and Summarization
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_decisions, f, ensure_ascii=False, indent=1)
        
    print(f"[*] Completed! Final update saved to {db_path}")

if __name__ == "__main__":
    main()
