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

# TODO: Node mutable or immutable (i.e., allow changes like bind?)
class Node:
    # packages a nodeid and connection together nicely
    def __init__(self, nodeid, addr, timeout=5, verbose=False):
        self.nodeid = nodeid
        kwargs = {
            'verbose': verbose,
            'timeout': timeout,
            'addr': addr,
        }
        self.rpc = RPCClient(**kwargs)
        self.addr = self.rpc.addr

class ChordClient:
    # add/subtract 1 from start/end to determine where the = goes (integers)
    @staticmethod
    def inrange(num, start, end):
        if start < end:
            return start < num and num < end
        else:
            return start < num or num < end

    @staticmethod
    def hash(data):
        the_hash = hashlib.sha1()
        the_hash.update(data)
        return int.from_bytes(the_hash.digest(), byteorder="big")

    def finger_start(self, fgr_idx):
        return (self.nodeid + 2 ** fgr_idx) % (2 ** self.system_bitwidth)

    def __init__(self, cluster_name=None, bitwidth=128, succlistlen=4, stabilizer_timeout=1, fixer_timeout=3, lookup_timeout=3, failure_timeout=5, verbose=False, callback_fxn=lambda x,y: None):
        if cluster_name is None:
            raise ValueError("keyword argument cluster_name must be supplied")
        self.system_bitwidth = bitwidth
        self.myip = utils.myip()
        self.nodeid = self.hash(self.myip.encode('utf-8')) % (2 ** self.system_bitwidth)

        #self.nodeid = 6 # TODO: CHANGE
        #self.fingers = [FingerTableEntry(cluster_name, (self.nodeid + 2 ** i) % (2 ** self.system_bitwidth), failure_timeout, verbose=verbose) for i in range(self.system_bitwidth)]
        self.fingers = [None for _ in range(self.system_bitwidth)]
        self.pred = None
        self.succlist = []
        self.succclients = {}
        self.succlistlen = succlistlen
        self.cluster_name = cluster_name 
        self.leaving = False
        self.verbose = verbose
        self.failure_timeout = failure_timeout
        self.callback_fxn = callback_fxn

        self.cxn_kwargs = {
            'timeout': self.failure_timeout,
            'verbose': self.verbose,
        }

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
            self.fingers[0] = Node(*liaison.lookup(self.nodeid + 1), **self.cxn_kwargs) # fix successor
            if self.verbose: print(f"Successfully joined cluster with {self.fingers[0].nodeid} as successor via liason {liason.addr[0]}:{liason.addr[1]}...")
            # NOTE: pred stays None: perioidic stabilizes will fix this later
        except IndexError: # could not choose from peers because node is first member of chord cluster
            # NOTE: Node creates a socket: is it fine to have the same process at both ends of a socket?
            # https://stackoverflow.com/a/8118180 suggests YES, fine
            # Not sure we get any actual benefit from this besides uniform handling of degenerate case
            if self.verbose: print("First node in cluster, setting self as succ...")
            self.fingers[0] = Node(self.nodeid, (self.myip, self.port), **self.cxn_kwargs)
        except ConnectionError:
            if self.verbose: print(f"Couldn't connect to liaison, retrying...")
            self.join()

    # Iterative lookup: return next node to talk to (if that person is me -- same node twice in a row, HTC will get the value from me)
    #   - just return from cpf
    # Recursive lookup:
    #   - benefit: sockets already open
    def lookup(self, hashed_key):
        # TODO: re-examine whole lookup process to be sure that it works and can tolerate one bad finger via last_node
        if self.verbose: print(f"Looking up {hashed_key}...")
        #hashed_key = self.hash(key.encode('utf-8')) % (2 ** self.system_bitwidth)
        try:
            last_node = node = Node(self.nodeid, (self.myip, self.port), **self.cxn_kwargs)
        except AttributeError:
            if self.verbose: print("Tried lookup before server running, sleeping and will try again...")
            time.sleep(self.lookup_timeout)
            return self.lookup(hashed_key)
 
        while last_node and node.nodeid != last_node.nodeid:
            prev_nodeid = node.nodeid
            try:
                nxt_nodeid, nxt_nodeaddr = node.closest_preceding_finger(hashed_key) # return nodeid instead to find connection
            except ConnectionError:
                if self.verbose: print(f"Node down: trying last node ({prev_nodeid})...")
                # Node down...find cpf(cpf(node)) and so on
                nxt_node = last_node.closest_preceding_finger(node.nodeid-1)
                # If successor is down wait for next stabilize() and retry
                if node.nodeid == prev_nodeid:
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
            node = Node(nxt_nodeid, nxt_nodeaddr, **self.cxn_kwargs)
            #node = RPCClient(utils.project_eq(self.cluster_name + str(nodeid) + 'chordclient'))
        return node.nodeid, node.addr

    def closest_preceding_finger(self, key):
        if self.verbose: print(f"Finding closest preceding finger of {key}...")
        # Logic (see p.249 in textbook)
        try:
            if self.inrange(key, self.pred, self.nodeid+1) or self.pred == self.nodeid:
                if self.verbose: print(f"I am responsible for that key ({key})...")
                while self.port is None: # in case server has not yet started
                    time.sleep(self.lookup_timeout)
                return self.nodeid, (self.myip, self.port) # we are responsible
        except TypeError:
            if self.verbose: print(f"predecessor is None, sleeping so we can stabilize and have a predecessor soon...")
            time.sleep(self.lookup_timeout)
            return self.closest_preceding_finger(key)

        if self.inrange(key, self.nodeid, self.fingers[0].nodeid+1):
            if self.verbose: print(f"My successor is responsible for that key ({key})...")
            return self.fingers[0].nodeid, self.fingers[0].addr # successor is responsible

        # Find cpf to talk to for more accurate info
        for i in range(self.system_bitwidth):
            try:
                if self.inrange(key, self.fingers[i].nodeid-1, self.fingers[(i+1)%self.system_bitwidth].nodeid):
                    if self.verbose: print(f"I am recommending you talk to {self.fingers[i].nodeid} for more information...")
                    return self.fingers[i].nodeid, self.fingers[i].addr
            except TypeError: # occurs when next finger has not yet been determined (is None)
                return self.fingers[i].nodeid, self.fingers[i].addr

    def leave(self):
        # any implementation at the chord client level? maybe flip on "leaving" switch?
        self.leaving = True # tells listener to respond as if node had left the network

    def fixer(self):
        if self.verbose: print("[Poker] Starting poker...")
        while not self.leaving:
            self.fix_finger()
            time.sleep(self.fixer_timeout)

    def fix_finger(self):
        fgr_idx = random.choice([i for i, _ in enumerate(self.fingers)])
        if self.verbose: print(f"[Poker] Fixing finger {fgr_idx}")
        corr_fgr, corr_addr = self.lookup(self.finger_start(fgr_idx))
        if self.fingers[fgr_idx] is None or corr_fgr != self.fingers[fgr_idx].nodeid:
            if self.verbose: print(f"[Poker] Set finger {fgr_idx} from {self.fingers[fgr_idx].nodeid if self.fingers[fgr_idx] else None} to {corr_fgr}...")
            self.fingers[fgr_idx] = Node(corr_fgr, corr_addr, **self.cxn_kwargs)
            # correct as many fingers as able at the current time
            i = 1
            n = self.system_bitwidth
            #while self.inrange(self.fingers[(fgr_idx+i)%n].start, self.fingers[fgr_idx].start, corr_fgr):
            # if my succ succeeds next guy's start, then it is also next guy's succ
            # however, stop chain if next guy already has correct finger
            # if next guy has correct finger, then if this finger applies to next next guy, it will have already been updated by this same process when next guy updated his
            # note that we use self.nodeid+1 as the (exclusive) end bound because the furthest clkwise any finger can be is ourself
            check_idx = (fgr_idx + i) % n
            check_fgr = self.fingers[check_idx]
            while self.inrange(self.finger_start(check_idx), self.nodeid+1, corr_fgr) and (check_fgr is None or check_fgr.nodeid != corr_fgr):
                if self.verbose: print(f"\t [Poker] While at it, also set finger {fgr_idx+i} to {corr_fgr}, as this is the (closest) successor of a smaller finger's start, meaning it this finger must be stale...")
                self.fingers[check_idx] = self.fingers[fgr_idx] # TODO: is this dangerous to share socket/Node objs between fingers?
                i += 1
                check_idx = (fgr_idx + i) % n
                check_fgr = self.fingers[check_idx]
        elif self.verbose: print(f"[Poker] Finger {fgr_idx} was already correct")

    def stabilizer(self):
        if self.verbose: print("[Stabilizer] Starting stabilizer...")
        while not self.leaving:
            self.stabilize()
            time.sleep(self.stabilizer_timeout)

    def pop_succ(self):
        print("Removing lost successor")
        try:
            del(self.succclients[self.succlist[0].nodeid])
            self.succlist = self.succlist[1:]
            # TODO: is it potentially dangerous to share Node objects here?
            self.fingers[0] = self.succlist[0] # use already open connection to populate finger table
        except IndexError: # if we've lost all of succlist, attempt to find succ via join
            if self.verbose: print("Lost all successors, rejoining system...")
            self.join()


    def stabilize(self):
        if self.verbose: print("[Stabilizer] Stabilizing...")
        try:
            succ_pred_id, succ_pred_addr = self.fingers[0].rpc.predecessor()
            if succ_pred_id is not None and self.inrange(succ_pred_id, self.nodeid, self.fingers[0].nodeid):
                if self.verbose: print(f"[Stabilizer] Found that {succ_pred_id} is a better successor than {self.fingers[0].nodeid}...")
                self.fingers[0] = Node(succ_pred_id, succ_pred_addr, **self.cxn_kwargs)
            while self.port is None: time.sleep(self.lookup_timeout) # delay until server running
            self.fingers[0].rpc.suspected_predecessor(self.nodeid, (self.myip, self.port))
        except ConnectionError:
            if self.verbose: print("[Stabilizer] Lost successor...")
            self.pop_succ()
            return self.stabilize() # retry stabilization

        # Also maintain succlist
        # naive: reconstruct whole succlist
        # better (not my idea): copy succ's succlist
        succ_succlist = self.fingers[0].rpc.successor_list()
        # Only chop off end if spilling over: faciliates build up of succlist in small cluster cases
        truncated_succ_succlist_tuples = succ_succlist[:min(len(succ_succlist), self.succlistlen-1)]

        new_succlist_tuples = [(self.fingers[0].nodeid, self.fingers[0].addr)] + truncated_succ_succlist_tuples
        print(succ_succlist, new_succlist_tuples)
        new_succclients = {
            nodeid: self.succclients.get(nodeid, Node(nodeid, nodeaddr, **self.cxn_kwargs))
            for nodeid, nodeaddr in new_succlist_tuples
        }
        new_succlist = [
            new_succclients[nodeid]
            for nodeid, nodeaddr in new_succlist_tuples
        ]
        # maintain succclients LUT so we don't have to recreate RPCs/sockets for connections we already have
        if self.verbose: print(f"[Stabilizer] Change succlist {[succ.nodeid for succ in self.succlist]} --> {[t[0] for t in new_succlist_tuples]}...")
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


    def suspected_predecessor(self, src_id, src_addr):
        # ping pred to ensure alive
        try:
            if self.verbose: print(f"[Server] Pinging pred {self.pred.nodeid}...")
            self.pred.rpc.ping()
            if self.verbose: print(f"[Server] Successfully contacted pred {self.pred.nodeid}")
        except ConnectionError:
            if self.verbose: print(f"[Server] Unable to contact pred {self.pred.nodeid} ({self.pred.nodeid} --> None)")
            self.pred = None
        except AttributeError: # self.pred is None
            pass

        if self.pred is None or self.pred.nodeid == self.nodeid or self.inrange(src_id, self.pred.nodeid, self.nodeid):
            if self.verbose: print(f"[Server] Contacted by better predecessor ({self.pred.nodeid if self.pred else 'None'} --> {src_id})...")
            self.pred = Node(src_id, src_addr, **self.cxn_kwargs)

    def predecessor(self): return (self.pred.nodeid, self.pred.addr) if self.pred is not None else (None, None)

    def successor(self): return self.fingers[0].nodeid, self.fingers[0].addr

    def successor_list(self): return [[succ.nodeid, succ.addr] for succ in self.succlist]

    def ping(self): pass

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
        if self.verbose: print(f'[Server] Listening on port {self.port}...')
        self.catalog.register('chord', self.cluster_name + str(self.nodeid) + 'chordclient', self.port, 'tfisher4') # Spawns registration thread
        if self.verbose: print(f'[Server] Registered as {self.cluster_name+str(self.nodeid)}chordclient to catalog service...')
        while True: # Poll forever
            readable, _, _ = select.select(socket_to_addr, [], []) # blocks until >= 1 skt ready
            for rd_skt in readable:
                if socket_to_addr[rd_skt] is None:
                    new_skt, addr = new_cxns.accept()
                    socket_to_addr[new_skt] = addr
                    socket_to_msgs[new_skt] = utils.nl_socket_messages(new_skt)
                    if self.verbose: print(f'[Server] Accepted connection with {addr[0]}:{addr[1]}...')
                    continue
                addr = socket_to_addr[rd_skt]
                    
                try: # Assume cxn unbreakable, client waiting for rsp
                    try: # Assume request is valid JSON in correct format corresponding to valid operation 
                        try: # Assume request is valid JSON encoded via utf-8
                            request = utils.decode_object(next(socket_to_msgs[rd_skt]))
                        except utils.RequestFormatError as e:
                                raise BadRequestError(e)
                        if self.verbose: print(f'[Server] Received request {request} from {addr[0]}:{addr[1]}...')
                        res = self.dispatch(request)
                    except BadRequestError as e:
                        res = e
                    rsp = self.build_response(res)
                    utils.send_nl_message(rd_skt, utils.encode_object(rsp))
                    if self.verbose: print(f'[Server] Sent response {rsp} to {addr[0]}:{addr[1]}...')
                except (ConnectionError, StopIteration):
                    if self.verbose: print(f'[Server] Lost connection with {addr[0]}:{addr[1]}.')
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

