import requests

url = "https://diavgeia.gov.gr/opendata/search.json"

req_all = requests.get(url, headers={'Accept': 'application/json'})
print("Total ALL:", req_all.json().get('info', {}).get('total'))

req_query = requests.get(url, params={"query": 'q:"ΑΓΙΑΣΟΣ"'}, headers={'Accept': 'application/json'})
print("Total query with q:", req_query.json().get('info', {}).get('total'))

req_query2 = requests.get(url, params={"query": 'ΑΓΙΑΣΟΣ'}, headers={'Accept': 'application/json'})
print("Total query simple:", req_query2.json().get('info', {}).get('total'))

req_q = requests.get(url, params={"q": 'ΑΓΙΑΣΟΣ'}, headers={'Accept': 'application/json'})
print("Total q:", req_q.json().get('info', {}).get('total'))
