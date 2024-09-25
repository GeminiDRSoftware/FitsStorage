import json
import requests


if __name__ == "__main__":

    gemini_api_authorization = 'f0a49ab56f80da436b59e1d8f20067f4'

    url = 'http://mkofits-lv1/update_headers'
    filename = 'N20151202S0289.fits'
    datalabel = 'GN-ENG20151202-1-006'
    state = "Usable"

    d = [{"data_label": datalabel, "values": {"qa_state": state}}]

    cookies = dict(gemini_api_authorization=gemini_api_authorization)
    r = requests.post(url, data=json.dumps(d), cookies=cookies, timeout=30)
    response = r.text
    http_status = r.status_code

    print("Status: %d" % http_status)
    print("Response: %s" % response)
