import HashArmonicaUtils as utils
from NDCatalog import NDCatalog
import socket
import json
import importlib
import http.client

class RPCClient:
    def __init__(self, chooser=None, addr=None, timeout=None, verbose=False, lazy=True):
        if chooser is not None:
            self.chooser = chooser
            self.catalog = NDCatalog()
            self.addr = None
            self.name = None
        elif addr is not None:
            self.addr = addr
            self.chooser = None
            self.name = ':'.join(map(str, addr))
        else:
            raise ValueError("Must specify one of chooser and addr kwargs")
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.settimeout(timeout)
        self.server_msgs = utils.nl_socket_messages(self.server_socket) # iterator through messages received over socket
        self.verbose = verbose
        self.catalog_url = "catalog.cse.nd.edu:9097/query.json"
        self.connected = False # delay connection lazily until needed

    def __del__(self):
        # When deleted/garbage collected, be sure to close socket
        self.server_socket.close()

    def catalog_lookup(self, picker):
        ''' Look up other node's address from catalog ''' 
        try:
            server_info = picker(self.catalog.query())
        except StopIteration:
            if self.verbose: print(f"Couldn't find appropriate node in catalog")
            raise ConnectionError
        self.name = server_info.get('project', server_info.get('type'))
        return server_info['address'], int(server_info['port'])

        # Assumes url includes port and file
        host, rest = self.catalog_url.split(":")
        port, filename = rest.split('/', maxsplit=1)
        port = int(port)
        cxn = http.client.HTTPConnection(host, port)
        cxn.request('GET', f'/{filename}')
        rsp = json.loads(cxn.getresponse().read())
        server_info = max((entry for entry in rsp if name == entry.get('project')), key = lambda x: x['lastheardfrom'])
        return server_info['address'], int(server_info['port'])

    def connect(self, host, port):
        ''' Connect to <host>:<port> for future RPC invocations. '''
        if self.verbose: print(f"Connecting to {host}:{port}")
        self.server_socket.connect((host, port))
        self.connected = True
        if self.verbose: print(f'[{self.name}] Successfully connected to {host}:{port}')

    def _rpc(self, method, args):
        ''' Invoke RPC <method> with arguments <args>.
            Requires a connection to have been previously established via connect.
        '''
        if not self.connected:
            addr = self.addr if self.addr else self.catalog_lookup(self.chooser)
            try:
                self.connect(*addr)
            except Exception:
                if self.verbose: print(f'Failed to connect to {self.name}')
                raise ConnectionError

        # Ultra generalist approach: just pass a message invokation with an ordered list of its arguments
        # Server will handle/return attr/type errors
        try:
            if self.verbose:
                print(f'[{self.name}] RPC invoked: {method}({", ".join(map(str, args))})')
            request = {
                'method': method,
                'arguments': args
            }
            try:
                encoded_message = utils.encode_object(request)
            except TypeError as e:
                raise TypeError('All arguments must be JSON serializable') from e
            # These may fail, but not much sensible recovery to be done
            utils.send_nl_message(self.server_socket, encoded_message)
            if self.verbose:
                print(f'[{self.name}] Sent request: {request}')
            #response = utils.decode_object(utils.receive_nl_message(self.server_socket))
            response = utils.decode_object(next(self.server_msgs)) #utils.receive_nl_message(self.server_socket))

            if self.verbose:
                print(f'[{self.name}] Received response: {response}')

            # Return result to caller or raise exception
            if response['status'] == 'success':
                return response['result']
            elif response['status'] == 'HashTableNetworkUtils.RequestFormatError':
                # This should never happen with correctly implemented stub
                if self.verbose:
                    print(f'[{self.name}] Client request rejected:\n'
                          + '\n'.join(map(lambda line: ' '*4 + line, json.dumps(request, indent=4).split('\n')))
                          + '\n'
                          + response['description'])
                raise RuntimeError('Client stub defective')
            else:
                module_name, error_name = response['status'].rsplit('.', maxsplit=1) 
                module = importlib.import_module(module_name)
                error = getattr(module, error_name)
                try: # Assume error has one arg constructor for message
                    e = error(response['description']) # Don't raise here in case e is a TypeError itself
                except TypeError: # Fall back to generic RuntimeError
                    raise RuntimeError(f'{error_name}: {response["description"]}')
                raise e

        except (ConnectionError, socket.timeout, StopIteration): # StopIteration in case the next client message is not available bc connection closed
            if self.verbose: print(f'[{self.name}] Connection lost')
            self.connected = False
            raise ConnectionError

    def __getattr__(self, attr):
        ''' Override default __getattr__ implementation.
            __getattr__ is invoked as fallback if attr is not found in object (i.e., it does not override references to connect, _rpc, etc).
            RPC operations are invoked by intercepting client attribute references.
            If the attribute is an RPC operation name, then return a function which invokes _rpc
            to pass a message to the server to perform that operation on the actual hash table.
        '''

        #if attr not in dir(self):
        return lambda *args: self._rpc(attr, args)
        #raise AttributeError(f"'HashTable' object has no attribute {attr}")
