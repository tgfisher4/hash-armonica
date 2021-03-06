import HashArmonicaUtils as utils
import socket
import re
import http.client
import json

class NDCatalog:
    def __init__(self, register_url="catalog.cse.nd.edu:9097", query_url="catalog.cse.nd.edu:9097/query.json"):
        self.register_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        register_addr = self._parse_url(register_url)
        self.register_socket.connect((register_addr['host'], int(register_addr['port'])))

        self.query_addr = self._parse_url(query_url)

    def __del__(self):
        try:
            self.stop_register()
        except:
            pass

    def _parse_url(self, url):
        url_regex = r'(?P<protocol>[A-Za-z]://)?(?P<host>[a-zA-Z.0-9]+)(?::(?P<port>\d{1,5}))?(?P<resource>/.*)?'
        return re.fullmatch(url_regex, url).groupdict()

    def register(self, as_type, as_name, with_port, under_owner, timeout=60):
        self._register(as_type, as_name, with_port, under_owner)
        self.stop_register = utils.repeat(lambda: self._register(as_type, as_name, with_port, under_owner), timeout)

    def _register(self, as_type, as_project, with_port, under_owner):
        registration = {
            "type": as_type,
            "owner": under_owner,
            "port": with_port,
            "project": as_project
        }
        packet = utils.encode_object(registration)
        utils.send_item(self.register_socket, packet)

    def query(self, addr=None):
        ''' Primitive query of all catalog entries.
            Handles networking and expects the caller to do everything else (filtering, etc)
        '''
        if addr is None: addr = self.query_addr
        cxn = http.client.HTTPConnection(addr['host'], addr['port'])
        cxn.request('GET', f"{addr['resource']}")
        rsp = json.loads(cxn.getresponse().read())
        return rsp
        
