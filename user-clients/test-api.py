import urllib.request, urllib.error, urllib.parse
import json


if __name__ == "__main__":

    gemini_api_authorization = 'f0a49ab56f80da436b59e1d8f20067f4'

    url = 'http://mkofits-lv1/update_headers'
    filename = 'N20151202S0289.fits'
    datalabel = 'GN-ENG20151202-1-006'
    state = "Usable"

    #d = [{"filename": filename, "values": {"qa_state": state}}]
    d = [{"data_label": datalabel, "values": {"qa_state": state}}]

    request = urllib.request.Request(url, data=json.dumps(d))
    request.add_header('Cookie', 'gemini_api_authorization=%s' % gemini_api_authorization)

    u = urllib.request.urlopen(request, timeout=30)
    response = u.read()
    u.close()
    http_status = u.getcode()

    print("Status: %d" % http_status)
    print("Response: %s" % response)
