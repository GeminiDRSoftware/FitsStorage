import requests

cookies = {'gemini_archive_session': 'PUT_COOKIE_VALUE_HERE'}
r = requests.get('https://archive.gemini.edu/whoami', cookies=cookies)
print(f"HTTP response code was: {r.status_code}")
print(f"Who Am I HTML: {r.text}")
