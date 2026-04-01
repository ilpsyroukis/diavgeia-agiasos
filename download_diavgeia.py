import requests
import os
from datetime import datetime, timedelta

def download_agiasos_decisions(target_date):
    """
    Κατεβάζει τις αποφάσεις για την Αγιάσο αξιοποιώντας τις ΕΠΙΣΗΜΕΣ 
    παραμέτρους (term, from_issue_date, to_issue_date) του απλού API.
    """
    base_url = "https://diavgeia.gov.gr/opendata/search.json"
    
    # Ζητάμε μία-μία τις λέξεις. Έτσι το API δεν μπερδεύεται ποτέ.
    search_terms = ["ΑΓΙΑΣΟΣ", "ΑΓΙΑΣΟΥ", "ΑΓΙΑΣΟ"]
    
    # Χρησιμοποιούμε dictionary (λεξικό) με κλειδί τον ΑΔΑ. 
    # Έτσι, αν μια απόφαση περιέχει και το "ΑΓΙΑΣΟΣ" και το "ΑΓΙΑΣΟΥ", 
    # θα την αποθηκεύσουμε μόνο μία φορά.
    unique_decisions = {} 
    
    print(f"[*] Ξεκινάει η σάρωση στο API για την ημερομηνία: {target_date}", flush=True)
    
    # Η "to_issue_date" πρέπει να είναι η επόμενη μέρα, αλλιώς η Διαύγεια ψάχνει 
    # για αποφάσεις που εκδόθηκαν ΑΚΡΙΒΩΣ τα μεσάνυχτα (00:00:00 ώρα).
    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    to_date_str = (target_date_obj + timedelta(days=1)).strftime("%Y-%m-%d")
    
    for term in search_terms:
        # ΑΥΤΕΣ είναι οι μοναδικές παράμετροι που αναγνωρίζει 100% η Διαύγεια
        params = {
            'term': term,
            'from_issue_date': target_date,
            'to_issue_date': to_date_str,
            'size': 500
        }
        
        try:
            response = requests.get(base_url, params=params, headers={'Accept': 'application/json'}, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                decisions = data.get('decisions', [])
                
                for dec in decisions:
                    ada = dec.get('ada')
                    if ada not in unique_decisions:
                        unique_decisions[ada] = dec
            else:
                print(f"[!] Αποτυχία αναζήτησης για τον όρο '{term}'. Κωδικός API: {response.status_code}")
                
        except Exception as e:
            print(f"[!] Σφάλμα επικοινωνίας για τον όρο '{term}': {e}")
            
    total_found = len(unique_decisions)
    print(f"\n[*] ΟΛΟΚΛΗΡΩΘΗΚΕ Η ΑΝΑΖΗΤΗΣΗ!")
    print(f"[*] Βρέθηκαν συνολικά {total_found} μοναδικές αποφάσεις για την Αγιάσο.", flush=True)
    
    if total_found == 0:
        print("[*] Τερματισμός. Δεν υπήρχαν αποφάσεις εκείνη τη μέρα.")
        return
        
    folder_name = f"Agiasos_Decisions_{target_date}"
    os.makedirs(folder_name, exist_ok=True)
    
    # Μετατροπή των μοναδικών αποφάσεων σε λίστα για να προχωρήσουμε στη λήψη
    decisions_list = list(unique_decisions.values())
    
    print("\n[*] Έναρξη λήψης αρχείων PDF...", flush=True)
    for index, dec in enumerate(decisions_list, start=1):
        ada = dec.get('ada')
        print(f"[{index}/{total_found}] Λήψη ΑΔΑ: {ada} ...", end=" ", flush=True)
        
        pdf_url = f"https://diavgeia.gov.gr/doc/{ada}"
        try:
            pdf_response = requests.get(pdf_url, timeout=15)
            if pdf_response.status_code == 200:
                with open(os.path.join(folder_name, f"{ada}.pdf"), 'wb') as f:
                    f.write(pdf_response.content)
                print("Επιτυχία!", flush=True)
            else:
                print(f"Σφάλμα Λήψης (Κωδικός {pdf_response.status_code})", flush=True)
        except Exception as e:
             print("Αποτυχία δικτύου", flush=True)

# --- ΕΚΤΕΛΕΣΗ ---
if __name__ == "__main__":
    hmerominia_endiaferontos = "2026-03-19" 
    download_agiasos_decisions(hmerominia_endiaferontos)
