import requests


if __name__ == "__main__":
    program_id = 'GN-2014A-Q-1'
    password='xxxxx'

    url = "https://gnodb.gemini.edu:8443/auth?id=%s&password=%s" % (program_id, password)
    r = requests.get(url)
    html = r.text
    print("reply: %s" % html)
