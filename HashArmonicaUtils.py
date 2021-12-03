import socket
import json
import sys

''' HashArmonicaNetworkUtils provides common, opinionated networking utilities that both the client and server RPC stubs utilize.
    Here, we encapsulate our networking protocol, by which we mean how a message should be formatted over a TCP stream, and not the exact format of the message.
    We define a message to be a Python JSON parsable object.
    But again, these networking utilities merely provide convenience functions to send and receive such messages, but place no restrictions on the contents of the message itself (is it a dictionary? an array? what should the message shape be?).

    We use a communication protocol we call L3 that describes a packet format, i.e., the relationship between bytes sent across the network (via a TCP connection, it is imagined) and the message communicated.
    The format is as follows:
        length of length (1 byte) | length (length of length bytes) | message (length bytes)
    We call it 'L3' because every message is prefixed by its length and its length's length: the format is [L]ength of [L]ength, [L]ength, message.
    This format is neat because it allows for very long messages with very little overhead.
    L3 can support messages up to 2^255 - 1 bytes (length of length may be up to 255, meaning length may be up to 2^255 - 1), which seems orders of magnitude greater than anything we'd need (we read that the total amount of data is the word is a few zeta - (2^70) - bytes).
    The only price we pay for this flexibility, however, is a single extra byte in every packet.
    That is, however many bytes it would take to send a message prepended by its length, it takes only one more to send a packet adhering to this app's communication protocol.
    This constrasts a protocol that includes the message length in a constant number of bytes at the beginning of a packet: this constant number will need to be large to accomodate large messages, but this means that small messages waste most of this space.
    The L3 protocol gives us the ability both to support large messages but keep small ones compact.
'''

# Custom Exceptions to our communication protocol
class RequestFormatError(RuntimeError):
    ''' Indicates a request was ill-formatted (not JSON, or wrong JSON shape) '''
    def __init__(self, msg=None, obj=None):
        super().__init__()
        if msg:
            self.args = (msg,)
        elif obj:
            self.args = (self._expected_format(obj),)

    def _expected_format(self, obj):
        doc = extract_doc(obj)
        return 'Expected the request to have one of the following forms:\n' + '\n'.join(map(str, (
            {
                'method': method,
                'arguments': {
                    arg: 'XXXXX'
                    for arg in doc[method]
                }
            }
            for method in doc
        ))).replace("'XXXXX'", "XXXXX")


# Utility reflection functions
def params_of_fxn(fxn):
    return params_of_code(fxn.__code__)

def params_of_code(code):
    # A code's arguments are its first 'argcount' non-self local vars.
    return [ param for param in code.co_varnames[:code.co_argcount] if param != 'self' ]

def execute_operation(oper, obj):
    # May raise Attribute error if method doesn't exist
    # May also raise TypeError if wrong # args, or any number of errors from fxn
    return getattr(obj, oper['method'])(*oper['arguments'])

def extract_doc(obj):
    return {
        method_name: params_of_fxn(method)
        # Methods "hidden" with _ are ignored.
        for method_name in dir(obj) if callable(method := getattr(obj, method_name)) and not method_name.startswith("_")
    }

def myip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    myip = s.getsockname()[0]
    s.close()
    return myip

def project_eq(x):
    return lambda itr: next(filter(lambda y: x == y.get('project'), itr))

# a la https://stackoverflow.com/a/22498708
from threading import Thread, Event
def repeat(fxn, every):
    stopped = Event()
    def do():
        while not stopped.wait(every):
            fxn()
    Thread(target=do).start()
    return stopped.set

# Utility networking functions
''' Fundamental packet read/writes are implemented as file read/writes.
    Files were decided to be the least common denominator because most
    resources have the ability to be treated as a file.
    The network level send/recv's, then, wrap these fundamental operations
    by creating a file abstraction from the socket and, upon failure,
    raise connection oriented errors.

    A5: now that I use timeouts on my sockets, I should not use makefile on a socket
    according to the documentation, as it may results in an inconsistent internal buffer.
'''

# nl (newline) protocol
BUFSIZ = 1024
def nl_messages(get_fxn):
    ''' "Gets" and returns a newline delimited message from a function.
        'from_fxn' should be a function accepting one integer argument ('size')
        and returning a byte-string up to length 'size'.
    '''
    # Use generator to maintain state between calls in case we receive too many bytes.
    # E.g.: many messages waiting to be received, many fitting within 1024 bytes.
    #print("generator init")
    extra = b''
    while piece := get_fxn():
        extra += piece
        while b'\n' in extra:
            msg, extra = extra.split(b'\n', maxsplit=1)
            yield msg            

def nl_file_messages(from_file):
    ''' Reads a newline delimited message from a file. '''
    yield from nl_messages(lambda: from_file.read(BUFSIZ))

def nl_socket_messages(from_skt):
    ''' Receives a newline delimited message from a socket. '''
    yield from nl_messages(lambda: from_skt.recv(BUFSIZ))
    #print(type(rlt))
    #return rlt

def put_item(put_fxn, item):
    ''' "Puts" a byte string 'item' by passing it to 'put_fxn'. '''
    # Bit roundabout since there's no extra logic here as in get_item.
    # However, can easily facilitate adding extra logic to ALL fxns that
    # put (send/write) items in one place.
    #print(item)
    return put_fxn(item)

def write_item_2(to_file, item):
    ''' Writes byte string 'item' to a file opened for binary writing. '''
    return put_item(lambda item: to_file.write(item), item)

def send_item_2(to_skt, item):
    ''' Sends byte string 'item' to a socket. '''
    return put_item(lambda item: to_skt.sendall(item), item)

def build_nl_packet(raw_pyld):
    ''' Builds an nl protocl packet containing the payload 'raw_pyld'. '''
    return raw_pyld + b'\n'

def send_nl_message(to_skt, msg):
    send_message(to_skt, msg, build_nl_packet)

def send_message(to_skt, msg, packet_builder):
    send_item_2(to_skt, packet_builder(msg))

# L3 (length of length, length) Protocol
def read_L3_message(from_file):
    ''' Read an L3 communication packet from file and return its binary message.
        The file should be opened in binary mode with read permission.
    '''
    # Full packet format: length of length (1 byte) | length (length of length bytes) | message (length bytes)
    # First byte specifies length length
    pyld_len_len_bytes = read_n_bytes(from_file, 1)
    pyld_len_len = int.from_bytes(pyld_len_len_bytes, 'big')

    # Next length length bytes specify length
    pyld_len_bytes = read_n_bytes(from_file, pyld_len_len)
    pyld_len = int.from_bytes(pyld_len_bytes, 'big')

    # Next length bytes specify msg
    pyld_bytes = read_n_bytes(from_file, pyld_len)
    return pyld_bytes

def read_n_bytes(from_file, n):
    ''' Read and return exactly n bytes from a file.
        The file should be opened in binary mode with read permission.
    '''
    # I wonder which is more efficient: continuing to append to a string and taking len, or appending to a list and summing lens
    pieces = []
    n_bytes_read = 0
    while n_bytes_read < n:
        next_bytes = from_file.read(n - n_bytes_read)
        if len(next_bytes) == 0:
            raise EOFError
        pieces.append(next_bytes)
        n_bytes_read += len(next_bytes)
    return b''.join(pieces)

def receive_L3_message(from_skt):
    ''' Receive an L3 communication packet from socket and return its binary message.
        Wrapper for read_item.
    '''
    try:
        with from_skt.makefile('rb') as skt_file:
            return read_L3_message(skt_file)
    except EOFError:
        # Some basic research suggests that while in general a socket may recv 0 bytes for reasons other than the cxn breaking,
        # this is the only reason a *blocking, streaming* socket like this one will recv 0 bytes (o/w it will block until it recvs)
        raise ConnectionError(f'Lost connection with {":".join(map(str, from_skt.getsockname()))}')

def decode_object(raw_obj):
    ''' Convert bytes into app-level communication message Python data structure via JSON. '''
    try:
        return json.loads(raw_obj.decode('utf-8'))
    except (json.decoder.JSONDecodeError, UnicodeDecodeError) as e:
        raise RequestFormatError('object is not valid JSON encoded in utf-8') from e

def encode_object(obj):
    ''' Convert app-level communication message Python data structure into bytes via JSON. '''
    return json.dumps(obj).encode('utf-8') # bubbles stringifying exceptions

def write_item(to_file, raw_item):
    ''' Write a binary item to a file.
        File should be opened in binary mode with write permission.
    '''
    # Write handles retries as needed (buffers full, disk busy, etc)
    to_file.write(raw_item) # May raise OSError if write fails

def send_item(to_skt, raw_item):
    ''' Send a binary sequence to a socket (handling retries and such).
        Wrapper for write_item.
    '''
    try:
        with to_skt.makefile('wb') as skt_file:
            write_item(skt_file, raw_item)
    except OSError:
        # Some basic research suggests that while in general a socket may send 0 bytes for reasons other than the cxn breaking,
        # this is the only reason a *blocking* socket like this one will send 0 bytes (o/w it will block until it can be sent)
        raise ConnectionError(f'Lost connection with {":".join(map(str, to_skt.getsockname()))}') 

def build_L3_packet(raw_pyld):
    ''' Build L3 communication packet from binary message. '''
    pyld_len = len(raw_pyld)
    pyld_len_len = (pyld_len.bit_length() + 7) // 8
    # Pretty unreasonable for this to happen since it would require a message of length more than 2^255 - 1, when all the data in the world can currently fit into a few zeta (2^70) bytes
    if pyld_len_len > 255:
        RuntimeError('Payload size exceeds the maximum the protocol can support')
    # Full packet format (as described above): length of length (1 byte) | length (length of length bytes) | message (length bytes)
    packet = b''.join((
        pyld_len_len.to_bytes(1, 'big'),
        pyld_len.to_bytes(pyld_len_len, 'big'),
        raw_pyld,
    ))
    return packet

def send_L3_message(to_skt, raw_msg):
    ''' Wrap a message in an L3 communication packet and write it to a socket.
        Wrapper for send_item.
    '''
    #send_item(to_skt, build_L3_packet(raw_msg))
    send_message(to_skt, raw_msg, build_L3_packet)

def err_desc(err):
    ''' Returns the error message, without annoying string wrapping by KeyError.
        Ex: str(KeyError('string'))      = "'string'".
            err_desc(KeyError('string')) =  'string'.
    '''
    return str(err) if type(err) is not KeyError else ",".join(map(str, err.args))





''' Server 
'''
class Server():
    def __init__(self, program):
        # open listening socket
        new_cxns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_to_addr = {new_cxns: None}
        socket_to_msgs = {}
        # socket cxt mgr forces harder to read level of indentation: TODO: cleanup socket, then, in destructor
        new_cxns.bind((socket.gethostname(), 0))
        new_cxns.listen()
        program.port = new_cxns.getsockname()[1]
        if program.verbose: print(f'Listening on port {self.port}...')
        if program.catalog:
            program.catalog.register('chord', program.cluster_name + str(program.nodeid), program.port, 'tfisher4') # Spawns registration thread
        if program.verbose: print(f'Registered as {self.cluster_name+str(self.nodeid)} to catalog service...')
        while True: # Poll forever
            readable, _, _ = select.select(socket_to_addr, [], []) # blocks until >= 1 skt ready
            for rd_skt in readable:
                if socket_to_addr[rd_skt] is None:
                    new_skt, addr = new_cxns.accept()
                    socket_to_addr[new_skt] = addr
                    socket_to_msgs[new_skt] = nl_socket_messages(new_skt)
                    if program.verbose: print(f'Accepted connection with {addr[0]}:{addr[1]}...')
                    continue
                addr = socket_to_addr[rd_skt]
                    
                try: # Assume cxn unbreakable, client waiting for rsp
                    try: # Assume request is valid JSON in correct format corresponding to valid operation 
                        try: # Assume request is valid JSON encoded via utf-8
                            request = decode_object(next(socket_to_msgs[rd_skt]))
                        except RequestFormatError as e:
                                raise BadRequestError(e)
                        if program.verbose: print(f'Received request {request} from {addr[0]}:{addr[1]}...')
                        res = self.dispatch(program, request)
                    except BadRequestError as e:
                        res = e
                    rsp = self.build_response(res)
                    send_nl_message(rd_skt, encode_object(rsp))
                    if program.verbose: print(f'Sent response {rsp} to {addr[0]}:{addr[1]}...')
                except (ConnectionError, StopIteration):
                    if program.verbose: print(f'Lost connection with {addr[0]}:{addr[1]}.')
                    rd_skt.close()
                    socket_to_addr.pop(rd_skt)
                    socket_to_msgs.pop(rd_skt)
                # Other exceptions are unexpected: let them run their course
        new_cxns.close()

    def dispatch(self, program, request):
        ''' Process request <req_obj> by dispatching to appropriate method. '''
        try:
            return execute_operation(request, program)
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
                'description': err_desc(cause) # Pass also the error description to report client-side
            }
        return {
            'status': 'success',
            'result': result
        }
