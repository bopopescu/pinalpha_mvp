import xmltodict
import requests

def get_session_id(ci,ui,pwd):
    url = "http://apidintegra.tkfweb.com/apid/request?method=login&ci=%s&ui=%s-%s&pwd=%s" % (ci,ci,ui,pwd)
    html = requests.get(url)
    accessKey = xmltodict.parse(html.content)['XRF']['A']['@v']
    return accessKey
    