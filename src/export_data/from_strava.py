# Example of Strava API connection
import requests
import json

auth_url = "https://www.strava.com/oauth/token"
activites_url = "https://www.strava.com/api/v3/athlete/activities"


# https://www.strava.com/oauth/authorize?client_id=152884&response_type=code&redirect_uri=http://localhost/exchange_token&approval_prompt=force&scope=read,activity:read_all,profile:read_all

payload = {
    'client_id': "152884",
    'client_secret': 'd363624d40119a4535ca3c662a8a516f12234bda',
    # 'refresh_token': 'd977b3016219f31a13056179f0ced636e4ce1f62',
    'code': '55e0d8fc59a5bd6a52ddf456396da3b9e58a8aec',
    'grant_type': "authorization_code",
    # 'f': 'json'
}

# Get the access token
res = requests.post(auth_url, data=payload)
print(res.json())
"""
access_token = res.json()['access_token']

print(f"Access token: {access_token}")
# Get activities
header = {'Authorization': 'Bearer ' + access_token}
param = {'per_page': 200, 'page': 1}
activities = requests.get(activites_url, headers=header, params=param).json()

print(activities)
"""