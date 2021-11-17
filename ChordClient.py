import HashArmonicaNetworkUtils as utils
import RPCClient
import hashlib
import NDCatalog
import threading

# Might be worth taking notes from
# https://cit.dixie.edu/cs/3410/asst_chord.html

class FingerTableEntry:
    def __init__(self, cluster_name, start, timeout):
        self.nodeid = None
        self.rpc = None
        self.cluster_name = cluster_name
        self.start = start # cache to prevent unnecessary "pows" - doesn't seem to help that much: powers of 2 ez to compute, addition ez too
        self.timeout = timeout

    def bind(self, new_nodeid):
        self.nodeid = new_nodeid
        self.rpc = RPCClient.RPCClient(utils.eq(self.cluster_name + self.nodeid), timeout=timeout)


class ChordClient:
    def inrange(self, num, start, end):

    def hash(self, datum):
        return hashlib.sha1(datum)

    def __init__(self, bitwidth, 
        self.system_bitwidth = bitwidth
        self.node_id = self.hash(utils.myip()) % 2 ** m
        self.fingers = [FingerTableEntry(self.nodeid + 2 ** i, cxn_timeout) for i in range(self.system_bitwidth)]
        self.pred = None
        #self.start # I don't think we know our own start
        self.succlist = [None for _ in range(succlistlen)]
        self.cluster_name = cluster_name # armonica pun here?
        self.leaving = False

        # Join before resistering: otherwise, other nodes seeking to join may ask us to perform a lookup for them, which we cannot do unless we've joined ourselves
        self.join()
        self.catalog = NDCatalog.NDCatalog() # save to instance variable so it doesn't go out of scope and get garbage collected
        self.catalog.register('chord', self.cluster_name + self.nodeid, 'tfisher4') # Spawns registration thread
        # Start listening before stabilizing because the initial stabilization is how other nodes learn of us.
        # We should be listening before they try to contact us, lest they think we're dead.
        threading.Thread.start(target=server).start() # Start listening
        threading.Thread.start(target=stabilizer).start() # Start stabilizing periodically
        threading.Thread.start(target=fixer).start() # Start fixing fingers periodically

    def join(self):
        # Find an existing node in cluster via catalog.
        # Make a random choice to hopefully distribute the load of bootstrapping joining nodes.
        try:
            liaison = RPCClient(lambda peers: random.choice([node for node in peers if self.cluster_name in node.project]))
            self.fingers[0].bind(liaison.lookup(self.nodeid + 1)) # fix successor
            # NOTE: pred stays None: perioidic stabilizes will fix this later
        except: # node is first member of chord cluster
            # NOTE: FingerTableEntry creates a socket: is it fine to have the same process at both ends of a socket?
            # https://stackoverflow.com/a/8118180 suggests YES, fine
            # Not sure we get any actual benefit from this besides uniform handling of degenerate case
            self.fingers[0].bind(self.nodeid)

    def lookup(self, key):
        hashed_key = self.hash(key) % 2 ** m
        node = self
        nodeid = self.nodeid
        # Failure handling:
        # If node down, find cpf(node).
        #   If this one is up, continue
        # If cpf(node) is down, look up cpf(cpf(node))?
        # 
        # If node down, try node = self.cpf(node)
        # Q: how to ensure we are making progress?
        # This way, I am worried about loop:
        #   - A contacts B, A's A.cpf(key), and B recommends C = B.cpf(key)
        #   - A cannot contact C
        #   - so, A contacts A.cpf(C) = B, asks for recommendation. recommends C
        # In essence: do we ever go back to the node we previously tried to talk to?
        # If we introduce memory of last node, we now need to worry about both failing
        # Should always be able to make progress by chaining succ's,
        # but how to detect when we are not making progress via FT?
        # Remembering last node:
        #   - contact self.cpf(key). they recommend node
        #   - if node down,
        #       - ask pvsnode for cpf(node). They are likely to have better answer than us (closer)
        #       - note that every cpf call should walk us pvs.fgrtbl. each call gets us one finger earlier
        #       - if we reach pvs.cpf(node) = pvs, then pvs's succ has failed and this lookup has failed
        #   - now, what if node down and pvs down before we can contact one of its fingers? Ideally, want to go to pvs^2 to ask for new anchor in 
        #   - the length of the stack this constructs will be n-1 in degenerate case: is this too much mem?
        #   - could also, if we reach pvs.cpf(node) = pvs, backtrack to pvs^2 and hope it has better info
        #   - could also, if pvs fails while we need it, could just fail on the spot and ask for retry in a few seconds: gives other's time to stabolize
        while key not in range
            try:
                nodeid = node.closest_preceding_finger(key)
            except ConnectionError:
                # what now?
                # let's check our own finger table for node preceding the one we were trying to talk to
                # Is this guaranteed to get us where we need to go?
                # What if we keep asking the same node for info and it keeps giving us the same crashed node?
                # In this case, ask for their finger that procedes the one they keep referring us to: nodeid closest pred
                # if nodeid closest pred IS the node we're talking to, this seems to indicate it does not have its succ in order and we should fail
                # In this case, we walk down the finger table of the last contacted node until we get a 
                # What if this other node fails?
                # Worried about:
                # 
                # If someone's succ is down, need to wait for them to stabilize: fail


            # TODO: optimize to use already established cxn, if available
            # To achieve this, might be nice to have a fingertable class which maintains a list and a dict
            # to achieve an orderred dict.
            # Supports indexing with nodeids via __getindex__/__setindex__,
            # and also supports use of 'in' with __contains__.
            # Problem: distinguishing nodeid access vs fgr_idx access
            node = RPCClient(utils.eq(self.cluster_name + nodeid))
        return nodeid

    def closest_preceding_finger
        #TODO: modular arithmetic
        for i in range(self.bidwidth-1, -1, -1):
            if key > fingers[i].start:
                return fingers[i]
        return self # otherwise its behind you: also, do we need to do some modular arithmetic - ex: you are at 6, keyspace is 0 - 7. your 1st cxn is 7, but second is 1. So if you want to look up 0, need to look > 7 (-1), < 1

    def leave(self):
        # any implementation at the chord client level? maybe flip on "leaving" switch?
        self.leaving = True # tells listener to respond as if node had left the network

    def fixer(self):
        while True:
            sleep(self.fixer_timeout)
            self.fix_finger()

    def fix_finger(self)
        fgr_idx = random.choice([i for i, fgr in enumerate(self.fingers)])
        corr_fgr = self.lookup(self.fingers[i].start)
        if corr_fgr != self.fingers[fgr_idx].nodeid:
            self.fingers[fgr_idx] = FingerTableEntry(...)
            # correct as many fingers as able at the current time
            while still succ:
                self.fingers[fgr_idx + i] = self.fingers[fgr_idx] # share socket/finger table entries between fingers?

    def stabilizer(self):
        while True:
            sleep(self.stabilizer_timeout)
            self.stabilize()

    def stabilize(self)
        # TODO: handle failure of successor
        # specifically with RPC client, how do we expect errors to propogate
        try:
            succ_pred = self.fingers[0].rpc.predecessor()
        except ConnectionError:
            del(self.succclients[self.succlist[0]])
            self.succlist = self.succlist[1:]
            self.fingers[0].bind(self.succlist[0]) # also use succclients to populate FingerTableEntry
            return stabilize() # retry stabilization
        # TODO: modular arithmetic
        if nodeid < succ_pred and < succ_pred < self.fingers[0].nodeid: # maybe cache successor's nodeid? can't change unless we change whole succ
            self.fingers[0].bind(succ_pred)
            self.succ

        try:
            self.fingers[0].rpc.suspected_predecessor(self.nodeid)
        except ConnectionError:
            # could not contact successor: same situation as above.
            # could be nice to encapsulate this for reuse. something like succlist.pop(); self.fingers[0].bind(succlist.head())
            del(self.succclients[self.succlist[0]])
            self.succlist = self.succlist[1:]
            self.fingers[0] = self.succlist[0] # also use succclients to populate FingerTableEntry
            return stabilize() # retry stabilization

        # Also maintain succlist
        # naive: reconstruct whole succlist
        # better (not my idea): copy succ's succlist
        new_succidlist = [self.fingers[0].nodeid] + self.fingers[0].rpc.succlist()[:-1] #always chop or just sometimes?
        # maintain succclients LUT so we don't have to recreate RPCs/sockets for connections we already have
        new_succlist = [
            self.succclients.get(nodeid, FingerTableEntry(nodeid))
            for nodeid in new_succidlist
        ]
        new_succclients = {
            fte.nodeid: fte
            for fte in new_succlist
        }
        higher_level_succlist_changed_handler(self.succlist, new_succlist)
        self.succlist = new_succlist #     together, lose references to obsolete rpcs, closing sockets
        self.succclients = new_succclients


    def suspected_predecessor(self, nodeid)
        if self.pred is None or self.pred < src and src < self.nodeid:
            self.pred = src

    def predecessor(self): return self.pred

    def successor(self): return self.fingers[0].nodeid

    # SERVER THREAD FUNCTIONS: Handle communication from other nodes

    # Big Q: how to balance time spent listening vs computing
    # Another big Q: do we need to ensure only one stablize/fix_finger check in-flight at a time?
    #   - certainly wasteful to have multiple going, but each should return the same result
    #   - if returning different results, next iteration will revert back to correct, guaranteed
    #   - eventual consistency?
    # Both of these resolved via threads
    def server(self):
        # TODO: return fake error if self.leaving
        # open listening socket
        new_cxns = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_to_addr = {new_cxns: None}
        socket_to_msgs = {}
        # socket cxt mgr forces harder to read level of indentation: TODO: cleanup socket, then, in destructor
        new_cxns.bind((socket.gethostname(), self.port))
        new_cxns.listen()
        self.port = new_cxns.getsockname()[1]
        if self.verbose: print(f'Listening on port {self.port}...')
        self.catalog.register() # NOTE: register here or in constructer?
        if self.verbose: print(f'Registered as {self.name} to catalog service...')
        while True: # Poll forever
            readable, _, _ = select.select(socket_to_addr, [], []) # blocks until >= 1 skt ready
            for rd_skt in readable:
                if socket_to_addr[rd_skt] is None:
                    new_skt, addr = new_cxns.accept()
                    socket_to_addr[new_skt] = addr
                    socket_to_msgs[new_skt] = utils.nl_socket_messages(new_skt)
                    if self.verbose: print(f'Accepted connection with {addr[0]}:{addr[1]}...')
                    continue
                addr = socket_to_addr[rd_skt]
                    
                try: # Assume cxn unbreakable, client waiting for rsp
                    try: # Assume request is valid JSON in correct format corresponding to valid operation 
                        try: # Assume request is valid JSON encoded via utf-8
                            request = utils.decode_object(next(socket_to_msgs[rd_skt]))
                        except utils.RequestFormatError as e:
                                raise BadRequestError(e)
                        if self.verbose: print(f'Received request {request} from {addr[0]}:{addr[1]}...')
                        res = self.dispatch(request)
                    except BadRequestError as e:
                        res = e
                    rsp = self.build_response(res)
                    utils.send_nl_message(rd_skt, utils.encode_object(rsp))
                    if self.verbose: print(f'Sent response {rsp} to {addr[0]}:{addr[1]}...')
                except (ConnectionError, StopIteration):
                    if self.verbose: print(f'Lost connection with {addr[0]}:{addr[1]}.')
                    rd_skt.close()
                    socket_to_addr.pop(rd_skt)
                    socket_to_msgs.pop(rd_skt)
                # Other exceptions are unexpected: let them run their course
        new_cxns.close()

    def dispatch(self, request):
        ''' Process request <req_obj> by dispatching to appropriate HashTable method. '''
        try:
            return utils.execute_operation(req_obj, self)
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



class BadRequestError(RuntimeError):
    ''' An internal exception used to distinguish exceptions expected due to bad requests. '''
    def __init__(self, cause):
        super().__init__()
        self.cause = cause

    def __str__(self):
        return str(self.cause)


        """
        new_succlist = [node]
        new_succclients = {}
        node = self.fingers[0]
        for i in range(self.succlistlen - 1):
            next_nodeid = node.rpc.successor()
            new_succlist.append(next_nodeid)
            if next_nodeid in self.succclient:
                next_node = FingerTableEntry(next_nodeid, self.succclient[next_nodeid])
            else:
                next_node = FingerTableEntry(next_nodeid)
            new_succlist.append(next_node)
            new_succclient[next_nodeid] = next_node.rpc
            # have to establish connection with them anyway to ask for their successor, so might as well store it for later
        higher_level_succlist_changed_handler(self.succlist, new_succlist)
        self.succlist = new_succlist # assuming this loses references to FingerTableEntries, which in turn loses references to their RPCClients, destroying them and closing their connections
        """
