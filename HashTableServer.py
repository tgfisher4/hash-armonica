import socket
import HashTable
import HashTableNetworkUtils as utils
import HashTableRPCDoc
import json
import argparse
import select
import signal

def main():
    args = process_args()
    HashTableServer(args.name, args.verbose).run()

def process_args():
    parser = argparse.ArgumentParser(description="A server that provides access to a local hash table to clients via RPC.")
    parser.add_argument("name", help="Name of the server, which clients will resolve to an address before connecting.")
    parser.add_argument("-v", "--verbose", action='store_true', help="If present, the server will output logging/debugging information to stdout.")
    args = parser.parse_args()
    return args

class BadRequestError(RuntimeError):
    ''' An internal exception used to distinguish exceptions expected due to bad requests. '''
    def __init__(self, cause):
        super().__init__()
        self.cause = cause

    def __str__(self):
        return str(self.cause)


class HashTableServer:
    def __init__(self, name, verbose):
        self.verbose = verbose

        self.table = HashTable.HashTable(self.verbose)
        self.name = name
        self.port = 0

        register_addr = "catalog.cse.nd.edu:9097"
        self.register_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        skt_addr = ((a := register_addr.split(":"))[0], int(a[1]))
        # Set up this UDP socket to only w/r from this specific addr
        # In particular, it creates an association between the socket and an
        # address so that it can be treated like a TCP skt by my utility fxns
        self.register_socket.connect(skt_addr)
        self.register_timeout = 60

        self.owner_id = "tfisher4"
        self.registration_socket = None

    def __del__(self):
        self.register_socket.close()

    def run(self):
        ''' Accept a cxn and process messages from that cxn until cxn lost,
            then accept another cxn.
        '''
        listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_to_addr = {listening_socket: None}
        socket_to_msgs = {}
        i = 0
        with listening_socket: # Clean up socket when done
            listening_socket.bind((socket.gethostname(), self.port))
            listening_socket.listen()
            self.port = listening_socket.getsockname()[1]
            if self.verbose:
                print(f'Listening on port {self.port}...')
            self.register()
            if self.verbose:
                print(f'Registered as {self.name} to catalog service...')
            signal.signal(signal.SIGALRM, self.wrap_register())
            signal.alarm(self.register_timeout)

            while True: # Poll forever
                readable, _, _ = select.select(socket_to_addr, [], []) # blocks until >= 1 skt ready
                for rd_skt in readable:
                    if socket_to_addr[rd_skt] is None:
                        new_skt, addr = listening_socket.accept()
                        socket_to_addr[new_skt] = addr
                        socket_to_msgs[new_skt] = utils.nl_socket_messages(new_skt)
                        if self.verbose:
                            print(f'Accepted connection with {addr[0]}:{addr[1]}...')
                        continue

                    addr = socket_to_addr[rd_skt]
                    #print(f'{addr} readable')
                    #if i >= 20:
                    #    print(f'ready: "{rd_skt.recv(1024).decode("utf-8")}"')
                    #i += 1
                    
                    try: # Assume cxn unbreakable, client waiting for rsp
                        try: # Assume request is valid JSON in correct format corresponding to valid operation given hash table state
                            try: # Assume request is valid JSON encoded via utf-8
                                #request = utils.decode_object(utils.receive_nl_message(rd_skt))
                                #print("receiving message...")
                                request = utils.decode_object(next(socket_to_msgs[rd_skt]))
                            except utils.RequestFormatError as e:
                                raise BadRequestError(e)
                            if self.verbose:
                                print(f'Received request {request} from {addr[0]}:{addr[1]}...')
                            res = self.dispatch(request)
                        except BadRequestError as e:
                            res = e
                        rsp = self.build_response(res)
                        utils.send_nl_message(rd_skt, utils.encode_object(rsp))
                        if self.verbose:
                            print(f'Sent response {rsp} to {addr[0]}:{addr[1]}...')
                    except (ConnectionError, StopIteration):
                        if self.verbose:
                            print(f'Lost connection with {addr[0]}:{addr[1]}.')
                        rd_skt.close()
                        socket_to_addr.pop(rd_skt)
                    # Other exceptions are unexpected: let them run their course

    def wrap_register(self):
        def wrapper(signum, frame):
            self.register()
            signal.alarm(self.register_timeout)
        return wrapper
    
    def register(self):
        registration = {
            "type": "hashtable",
            "owner": self.owner_id,
            "port": self.port,
            "project": self.name,
        }
        packet = json.dumps(registration).encode('utf-8')
        # Shouldn't receive ConnectionError because there is no connection
        utils.send_item(self.register_socket, packet)

    def dispatch(self, req_obj):
        ''' Process request <req_obj> by dispatching to appropriate HashTable method. '''
        try:
            return utils.execute_operation(req_obj, self.table)
        except Exception as e:
            raise BadRequestError(e)

    def build_response(self, result):
        ''' Builds a response object from a result.
            A result may either be a BadRequestError wrapping an underlying error,
            or the result returned by a valid operation.
        '''
        if isinstance(result, BadRequestError):
            cause = result.cause
            return {
                'status': f'{type(cause).__module__}.{type(cause).__name__}', # Identify error to be raised client-side
                'description': utils.err_desc(cause) # Pass also the error description to report client-side
            }
        return {
            'status': 'success',
            'result': result
        }


if __name__ == "__main__":
    main()
