import HashArmonicaUtils as utils
from RPCClient import RPCClient
import hashlib
from NDCatalog import NDCatalog
import threading
import time
import http.client
import random
import socket
import select

# Might be worth taking notes from
# https://cit.dixie.edu/cs/3410/asst_chord.html

class FingerTableEntry:
    def __init__(self, cluster_name, start, timeout, verbose=False, suffix=''):
        self.nodeid = None
        self.rpc = None
        self.cluster_name = cluster_name
        self.suffix = suffix
        self.start = start # cache to prevent unnecessary "pows" - doesn't seem to help that much: powers of 2 ez to compute, addition ez too
        self.timeout = timeout
        self.verbose = verbose

    def bind(self, new_nodeid, addr=None):
        self.nodeid = new_nodeid
        kwargs = {
            'verbose': self.verbose,
            'timeout': self.timeout,
        }
        if addr: kwargs['addr'] = addr
        else: kwargs['chooser'] = utils.project_eq(self.cluster_name + str(self.nodeid) + self.suffix)
        self.rpc = RPCClient(**kwargs)
        return self


class ChordClient:
    # add/subtract 1 from start/end to determine where the = goes (integers)
    def inrange(self, num, start, end):
        if start < end:
            return start < num and num < end
        else:
            return start < num or num < end

    def hash(self, data):
        the_hash = hashlib.sha1()
        the_hash.update(data)
        return int.from_bytes(the_hash.digest(), byteorder="big")

    def __init__(self, cluster_name=None, bitwidth=128, succlistlen=4, stabilizer_timeout=1, fixer_timeout=3, lookup_timeout=3, failure_timeout=5, verbose=False, callback_fxn=lambda x,y: None):
        if cluster_name is None:
            raise ValueError("keyword argument cluster_name must be supplied")
        self.system_bitwidth = bitwidth
        self.myip = utils.myip()
        self.nodeid = self.hash(self.myip.encode('utf-8')) % (2 ** self.system_bitwidth)
        #self.nodeid = 6 # TODO: CHANGE
        self.fingers = [FingerTableEntry(cluster_name, (self.nodeid + 2 ** i) % (2 ** self.system_bitwidth), failure_timeout, verbose=verbose) for i in range(self.system_bitwidth)]
        self.pred = None
        self.succlist = []
        self.succclients = {}
        self.succlistlen = succlistlen
        self.cluster_name = cluster_name 
        self.leaving = False
        self.verbose = verbose
        self.failure_timeout = failure_timeout
        self.callback_fxn = callback_fxn

        # We actually need to register first, before we join.
        # This is for the case that we are the first node to join the cluster.
        # In this case, the join procedure creates an RPCClient to ourselves,
        # for which it is necessary that our server information be documented in the catalog.
        # To handle the case that for n > 1, other nodes may ask us to perform a lookup on their behalf,
        # so that they may join the cluster, we simply rely on the fact that lookup sleeps while it cannot 
        # serve the request. Thus, we simply delay our response until we have an answer.
        # Also, start listening before stabilizing because the initial stabilization is how other nodes learn of us.
        # We should be listening before they try to contact us, lest they think we're dead.


        # Start listening first. This is so we can connect to ourselves later. Do not register yet.
        # Then, join. If no catalog entries exist, we are the first node in the cluster.
        # Then, create RPCClient to successor. If we are the first node, we are our own successor, so use our known host/port (not yet registered)
        #   - doesn't seem there is anything preventing us from registering immediately after join
        # Then, register.
        # Then, start stabilizing. This ensures that if other nodes know of our existence (via stabilize), then they can contact us (via catalog).
        self.catalog = NDCatalog() # save to instance variable so it doesn't go out of scope and get garbage collected
        self.lookup_timeout = lookup_timeout
        threading.Thread(target=self.server, daemon=True).start() # Start listening: fine to register
        self.join() # Join, taking no catalog entries as the hint that we 
        self.stabilizer_timeout = stabilizer_timeout
        threading.Thread(target=self.stabilizer, daemon=True).start() # Start stabilizing periodically
        self.fixer_timeout = fixer_timeout
        threading.Thread(target=self.fixer, daemon=True).start() # Start fixing fingers periodically

    def join(self):
        if self.verbose: print(f"Attempting to join cluster {self.cluster_name}...")

        # Find an existing node in cluster via catalog.
        # Make a random choice to hopefully distribute the load of bootstrapping joining nodes.
        try:
            liaison = RPCClient(lambda peers: random.choice([node for node in peers if self.cluster_name in node.get('project', '') and 'chordclient' in node.get('project', '') and not str(self.nodeid) in node.get('project', '')]))
            self.fingers[0].bind(liaison.lookup(self.nodeid + 1)) # fix successor
            if self.verbose: print(f"Successfully joined cluster with {self.fingers[0].nodeid} as successor...")
            # NOTE: pred stays None: perioidic stabilizes will fix this later
        except IndexError: # could not choose from peers because node is first member of chord cluster
            # NOTE: FingerTableEntry creates a socket: is it fine to have the same process at both ends of a socket?
            # https://stackoverflow.com/a/8118180 suggests YES, fine
            # Not sure we get any actual benefit from this besides uniform handling of degenerate case
            if self.verbose: print("First node in cluster, setting self as succ...")
            self.fingers[0].bind(self.nodeid, addr=(self.myip, self.port))
        except ConnectionError:
            if self.verbose: print(f"Couldn't connect to liaison, retrying...")
            self.join()

    # Iterative lookup: return next node to talk to (if that person is me -- same node twice in a row, HTC will get the value from me)
    #   - just return from cpf
    # Recursive lookup:
    #   - benefit: sockets already open
    def lookup(self, hashed_key):
        if self.verbose: print(f"Looking up {hashed_key}...")
        #hashed_key = self.hash(key.encode('utf-8')) % (2 ** self.system_bitwidth)
        node = self
        nodeid = self.nodeid
        prev_nodeid = -1
        
        while nodeid != prev_nodeid:
            prev_nodeid = nodeid
            try:
                nodeid = node.closest_preceding_finger(hashed_key) # return nodeid instead to find connection
            except ConnectionError:
                if self.verbose: print(f"Node down: trying last node ({prev_nodeid})...")
                # Node down...find cpf(cpf(node)) and so on
                nodeid = last_node.closest_preceding_finger(nodeid-1)
                # If successor is down wait for next stabilize() and retry
                if nodeid == prev_nodeid:
                    if self.verbose: print(f"Previous node's successor is down: waiting so we can stabilize and succeed soon...")
                    # Failed lookup; retry 
                    time.sleep(self.lookup_timeout)
                    return self.lookup(hashed_key)

            # TODO: optimize to use already established cxn, if available
            # To achieve this, might be nice to have a fingertable class which maintains a list and a dict
            # to achieve an orderred dict.
            # Supports indexing with nodeids via __getindex__/__setindex__,
            # and also supports use of 'in' with __contains__.
            # Problem: distinguishing nodeid access vs fgr_idx access

            last_node = node
            node = RPCClient(utils.project_eq(self.cluster_name + str(nodeid) + 'chordclient'))
        return nodeid

    def closest_preceding_finger(self, key):
        if self.verbose: print(f"Finding closest preceding finger of {key}...")
        # Logic (see p.249 in textbook)
        try:
            if self.inrange(key, self.pred, self.nodeid+1) or self.pred == self.nodeid:
                if self.verbose: print(f"I am responsible for that key ({key})...")
                return self.nodeid # we are responsible
        except TypeError:
            if self.verbose: print(f"predecessor is None, sleeping so we can stabilize and have a predecessor soon...")
            time.sleep(self.lookup_timeout)
            return self.closest_preceding_finger(key)

        if self.inrange(key, self.nodeid, self.fingers[0].nodeid+1):
            if self.verbose: print(f"My successor is responsible for that key ({key})...")
            return self.fingers[0].nodeid # successor is responsible

        # Find cpf to talk to for more accurate info
        for i in range(self.system_bitwidth):
            try:
                if self.inrange(key, self.fingers[i].nodeid-1, self.fingers[(i+1)%self.system_bitwidth].nodeid):
                    if self.verbose: print(f"I am recommending you talk to {self.fingers[i].nodeid} for more information...")
                    return self.fingers[i].nodeid
            except TypeError:
                return self.fingers[i].nodeid

    def leave(self):
        # any implementation at the chord client level? maybe flip on "leaving" switch?
        self.leaving = True # tells listener to respond as if node had left the network

    def fixer(self):
        if self.verbose: print("Starting fixer...")
        while not self.leaving:
            self.fix_finger()
            time.sleep(self.fixer_timeout)

    def fix_finger(self):
        fgr_idx = random.choice([i for i, _ in enumerate(self.fingers)])
        corr_fgr = self.lookup(self.fingers[fgr_idx].start)
        if self.verbose: print(f"Set finger {fgr_idx} from {self.fingers[fgr_idx].nodeid} to {corr_fgr}...")
        if corr_fgr != self.fingers[fgr_idx].nodeid:
            self.fingers[fgr_idx].bind(corr_fgr)
            # correct as many fingers as able at the current time
            i = 1
            n = self.system_bitwidth
            while self.inrange(self.fingers[(fgr_idx+i)%n].start, self.fingers[fgr_idx].start, corr_fgr):
                if self.verbose: print(f"\t While at it, also set finger {fgr_idx+i} to {corr_fgr}, as this was the successor of a smaller finger start, meaning finger {fgr_idx+1}'s data must be stale...")
                self.fingers[fgr_idx + i] = self.fingers[fgr_idx] # share socket/finger table entries between fingers?
                i += 1

    def stabilizer(self):
        if self.verbose: print("Starting stabilizer...")
        while not self.leaving:
            self.stabilize()
            time.sleep(self.stabilizer_timeout)

    def pop_succ(self):
        del(self.succclients[self.succlist[0]])
        self.succlist = self.succlist[1:]
        self.fingers[0] = self.succclients[self.succlist[0]] # also use succclients to populate FingerTableEntry


    def stabilize(self):
        if self.verbose: print("Stabilizing...")
        try:
            succ_pred = self.fingers[0].rpc.predecessor()
            if succ_pred is not None and self.inrange(succ_pred, self.nodeid, self.fingers[0].nodeid):
                if self.verbose: print(f"Found that {succ_pred} is a better successor than {self.fingers[0].nodeid}...")
                self.fingers[0].bind(succ_pred)
            self.fingers[0].rpc.suspected_predecessor(self.nodeid) 
        except ConnectionError:
            if self.verbose: print("Lost successor...")
            self.pop_succ()
            return self.stabilize() # retry stabilization

        # Also maintain succlist
        # naive: reconstruct whole succlist
        # better (not my idea): copy succ's succlist
        succ_succlist = self.fingers[0].rpc.successor_list()
        # Only chop off end if spilling over: faciliates build up of succlist in small cluster cases
        truncated_succ_succlist_ids = succ_succlist[:min(len(succ_succlist), self.succlistlen-1)]

        new_succlist_ids = [self.fingers[0].nodeid] + truncated_succ_succlist_ids
        new_succclients = {
            nodeid: self.succclients.get(nodeid, FingerTableEntry(self.cluster_name, None, timeout=self.failure_timeout, verbose=self.verbose, suffix='chordclient').bind(nodeid))
            for nodeid in new_succlist_ids
        }
        new_succlist = [
            new_succclients[nodeid]
            for nodeid in new_succlist_ids
        ]
        # maintain succclients LUT so we don't have to recreate RPCs/sockets for connections we already have
        if self.verbose: print(f"Change succlist {self.succlist} --> {new_succlist}...")
        # Update in-place so that current iterator references to the list reflect the updates made,
        # if stabilize preempts the thread and changes the succlist.
        # Also important is that we perform the update before upcalling to HashArmonica to ensure
        # specifically so that we change our list of successors before telling old succlist members
        # to drop their associated replicas.
        # First, make a copy of the old_succlist
        old_succlist = self.succlist.copy()
        for i, new_succ in enumerate(new_succlist):
            if i >= len(self.succlist):
                self.succlist.append(new_succ)
            else:
                self.succlist[i] = new_succ
        # lose references to obsolete rpcs, closing sockets
        self.succclients = new_succclients
        
        self.callback_fxn(old_succlist, new_succlist)


    def suspected_predecessor(self, src):
        # ping pred to ensure alive
        try:
            FingerTableEntry(self.cluster_name, None, self.failure_timeout).bind(src).rpc.successor()
        except ConnectionError:
            if self.verbose: print(f"Unable to contact pred, accepting node {src} as pred")
            self.pred = src
            return

        if self.pred is None or self.pred == self.nodeid or self.inrange(src, self.pred, self.nodeid):
            if self.verbose: print(f"Contacted by better predecessor ({self.pred} --> {src})...")
            self.pred = src

    def predecessor(self): return self.pred

    def successor(self): return self.fingers[0].nodeid

    def successor_list(self): return self.succlist

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
        new_cxns.bind((socket.gethostname(), 0))
        new_cxns.listen()
        self.port = new_cxns.getsockname()[1]
        if self.verbose: print(f'Listening on port {self.port}...')
        self.catalog.register('chord', self.cluster_name + str(self.nodeid) + 'chordclient', self.port, 'tfisher4') # Spawns registration thread
        if self.verbose: print(f'Registered as {self.cluster_name+str(self.nodeid)}chordclient to catalog service...')
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
        ''' Process request <req_obj> by dispatching to appropriate method. '''
        try:
            return utils.execute_operation(request, self)
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
