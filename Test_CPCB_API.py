import requests
headers = {
   
    "X-API-KEY": "my_api"
}
r = requests.get("https://api.openaq.org/v3/locations", params={"country": "IN", "limit": 10}, headers=headers)
print(r.status_code)
print(r.json())
