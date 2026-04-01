import requests
import json
import os
import time

def main():
    base_url = "https://diavgeia.gov.gr/opendata/search.json"
    search_terms = ["ΑΓΙΑΣΟΣ", "ΑΓΙΑΣΟΥ", "ΑΓΙΑΣΟ"]
    unique_decisions = {}
    
    print("[*] Starting full data sync for Agiasos...")
    
    for term in search_terms:
        page = 0
        while True:
            params = {
                'term': term,
                'size': 500,
                'page': page
            }
            
            try:
                print(f"[*] Fetching term '{term}', page {page}...")
                response = requests.get(base_url, params=params, headers={'Accept': 'application/json'}, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    decisions = data.get('decisions', [])
                    
                    if not decisions:
                        break # No more results
                    
                    for dec in decisions:
                        ada = dec.get('ada')
                        if ada and ada not in unique_decisions:
                            unique_decisions[ada] = dec
                            
                    info = data.get('info', {})
                    actual_size = info.get('actualSize', 0)
                    total = info.get('total', 0)
                    print(f"    -> Received {actual_size} / {total} total for this query")
                    
                    if len(decisions) < 500:
                        break # Reached the end
                        
                    page += 1
                    time.sleep(1) # Be nice to the API
                    
                else:
                    print(f"[!] API Error {response.status_code} for term '{term}' on page {page}")
                    break
                    
            except Exception as e:
                print(f"[!] Request failed: {str(e)}")
                break
                
    total_records = len(unique_decisions)
    print(f"\n[*] Sync completed! Total unique decisions found: {total_records}")
    
    # Sort descending by date (issueDate or submissionTimestamp mapping to timestamp)
    # The API returns `issueDate` which is timestamp in milliseconds
    sorted_decisions = sorted(
        unique_decisions.values(),
        key=lambda x: x.get('issueDate', 0),
        reverse=True
    )
    
    # Save to JSON
    # It must be saved inside 'web/' directory so the frontend can retrieve it statically
    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'web', 'data.json')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sorted_decisions, f, ensure_ascii=False, indent=2)
        
    print(f"[*] Data saved successfully to: {output_path}")

if __name__ == "__main__":
    main()
