import requests

def test_diavgeia():
    url = "https://diavgeia.gov.gr/opendata/search.json"
    
    # Test 1: query
    req1 = requests.get(url, params={"query": 'q:"ΑΓΙΑΣΟΣ"'}, headers={'Accept': 'application/json'})
    print("Test 1 query:", req1.json().get('info', {}).get('query'))
    
    # Test 2: q
    req2 = requests.get(url, params={"q": 'ΑΓΙΑΣΟΣ'}, headers={'Accept': 'application/json'})
    print("Test 2 q:", req2.json().get('info', {}).get('query'))
    
    # Test 3: term, from_issue_date
    req3 = requests.get(url, params={"term": 'ΑΓΙΑΣΟΣ'}, headers={'Accept': 'application/json'})
    print("Test 3 term:", req3.json().get('info', {}).get('query'))

if __name__ == "__main__":
    test_diavgeia()
