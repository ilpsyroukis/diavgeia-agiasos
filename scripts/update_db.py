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

def generate_ai_summary(text):
    """
    Improved AI summary with noise reduction and redundant prefix removal.
    Focuses on extracting meaningful action descriptions (e.g. 'Επισκευή πόρτας').
    """
    if not text or len(text.strip()) < 50:
        return None
        
    # 1. Aggressive cleaning of symbols and noise from PDF tables
    clean_text = text.replace('|', ' ').replace('-', ' ').replace('_', ' ').replace('=', ' ')
    clean_text = re.sub(r'\.{2,}', ' ', clean_text) # remove multi-dots
    clean_text = re.sub(r'\s+', ' ', clean_text).strip() # normalize whitespace
    
    # 2. Redundant administrative phrases to strip out
    junk_prefixes = [
        "ΔΕΣΜΕΥΣΗ ΠΟΣΟΥ ΓΙΑ ΤΗΝ", "ΔΕΣΜΕΥΣΗ ΠΟΣΟΥ ΓΙΑ ΤΙΣ", "ΔΕΣΜΕΥΣΗ ΠΟΣΟΥ ΓΙΑ", 
        "ΑΠΟΦΑΣΗ", "ΕΓΚΡΙΣΗ", "ΠΡΟΜΗΘΕΙΑ", "ΠΑΡΟΧΗ ΥΠΗΡΕΣΙΩΝ", "ΠΛΗΡΩΜΗ ΓΙΑ"
    ]
    
    # 3. Sentence splitting (improved logic)
    sentences = [s.strip() for s in clean_text.split('.') if len(s.strip()) > 10]
    if not sentences: return None
    
    # 4. Keyword targeting
    keywords = [
        "επισκευή", "συντήρηση", "καθαρισμός", "αγορά", "ανάθεση", "μετάβαση",
        "καύσιμα", "τροφεία", "υπηρεσίες", "δράση", "εκδήλωση", "έργο", "προσωπικό"
    ]
    
    ranked = []
    for i, s in enumerate(sentences[:20]):
        score = 0
        s_upper = s.upper()

        # Boost sentences following a specific marker (common in Diavgeia headers)
        markers = ["ΑΙΤΙΑ ΠΛΗΡΩΜΗΣ", "ΘΕΜΑ:", "ΘΕΜΑ", "ΠΕΡΙΛΗΨΗ"]
        for marker in markers:
            if marker in s_upper:
                score += 15
                # Extract the text after the marker if possible
                parts = re.split(f"{marker}", s, flags=re.IGNORECASE)
                if len(parts) > 1: s = parts[1].strip()

        # Penalty for mostly numeric sentences (budget codes)
        num_count = sum(c.isdigit() for c in s)
        if num_count > len(s) * 0.3:
            score -= 10
            
        # Penalty for extremely short or noisy strings
        if len(s) < 20: score -= 5
        
        # Boost for actual keywords
        for kw in keywords:
            if kw in s.lower():
                score += 5
                
        # Strip junk prefixes from the candidate summary
        for junk in junk_prefixes:
            if s.upper().startswith(junk):
                s = s[len(junk):].strip()
        
        if len(s) > 10:
            ranked.append((score, s))
    
    if not ranked: return None
    
    # Sort and pick the best candidate
    ranked.sort(reverse=True, key=lambda x: x[0])
    summary = ranked[0][1]
    
    # Final cleanup (capitalization and length)
    summary = summary.capitalize().strip()
    if len(summary) > 150:
        summary = summary[:147] + "..."
        
    return summary

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
             
             # Generate AI summary immediately
             summary = generate_ai_summary(full_text)
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
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                for dec in existing_data:
                    ada = dec.get('ada')
                    if ada:
                        unique_decisions[ada] = dec
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
            "issueDate": issue_date_ts,
            "organizationId": d.get('organization', {}).get('uid', d.get('organizationId', '')),
            "documentUrl": d.get('documentUrl', f"https://diavgeia.gov.gr/doc/{ada}")
        }
        if 'documentText' in d:
            dec['documentText'] = d['documentText']
        
        final_list.append(dec)

    # Final cleanup: Ensure everything is since 2022
    start_ts = int(datetime(2022, 1, 1).timestamp() * 1000)
    unique_decisions_list = [v for v in final_list if v.get('issueDate', 0) >= start_ts]

    # 2. Identify missing documentTexts
    missing_texts = [dec for dec in unique_decisions_list if 'documentText' not in dec]
    global total_to_process, processed_count
    total_to_process = len(missing_texts)
    processed_count = 0
    
    print(f"[*] Total unique decisions found since 2022: {len(unique_decisions_list)}")
    print(f"[*] {total_to_process} decisions are missing full-text.")
    
    # Sort for final save
    sorted_decisions = sorted(unique_decisions_list, key=lambda x: x.get('issueDate', 0), reverse=True)

    if total_to_process > 0:
        print("[*] Downloading and parsing PDFs in parallel (limit 1000 per run to prevent timeout)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            to_fetch = missing_texts[:1000] # First 1000 PDFs in this pass
            executor.map(fetch_pdf_text, to_fetch)
            
    # 4. AI Summarization pass - Updating first 1000 items with new cleaner logic
    print("[*] Updating first 1000 AI summaries with new cleaner logic...")
    summarized_count = 0
    for dec in sorted_decisions:
        if 'documentText' in dec:
            summary = generate_ai_summary(dec['documentText'])
            if summary:
                dec['summary'] = summary
                summarized_count += 1
        if summarized_count >= 1000: break
    print(f"[*] AI Summarization pass completed: {summarized_count} summaries updated.")

    # 5. Save
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_decisions, f, ensure_ascii=False, indent=1)
        
    print(f"[*] Completed! Saved {len(sorted_decisions)} decisions to {db_path}")

if __name__ == "__main__":
    main()
