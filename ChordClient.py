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
    # packages a nodeid and rpcclient together nicely
    def __init__(self, nodeid, addr, timeout=5, verbose=False):
        self.nodeid = nodeid
        self.timeout = timeout
        self.verbose = verbose
        kwargs = {
            'verbose': verbose,
            'timeout': timeout,
            'addr': addr,
        }
        self.rpc = RPCClient(**kwargs)
        self.addr = self.rpc.addr

    def copy(self):
        return Node(self.nodeid, self.addr, self.timeout, self.verbose)

    def __repr__(self):
        return f'{self.nodeid}@{self.addr[0]}:{self.addr[1]}'

    def __eq__(self, other):
        return self.nodeid == other.nodeid

    def __hash__(self):
        return self.nodeid

class ChordClient:
    # TODO: check if threads are ever reusing same rpc clients: can cause problems since they can both run same generator at once when trying to recv

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

    def __str__(self):
        lines = [
            f"===== CHORD PEER =====",
            f"-- Me --",
            f"\tID: {self.nodeid}",
            f"\tAddress: {(self.myip, self.port)}",
            f"\tRedirects to: {self.redirect_to}",
            f"\tSystem info",
            f"\t\tCluster: {self.cluster_name}",
            f"\t\tBitwidth: {self.system_bitwidth}",
            f"\t\tlen(successor list): {self.succlistlen}",
            f"\t\tTimeouts:",
            f"\t\t\tFailure threshold: {self.failure_timeout}",
            f"\t\t\tBackoff timer: {self.lookup_timeout}",
            f"\t\t\tStabilizer period: {self.stabilizer_timeout}",
            f"\t\t\tPoker period: {self.fixer_timeout}",
            f"-- Successor --",
            f"\t{self.fingers[0]}",
            f"-- Predecessor --",
            f"\t{self.pred}",
            f"-- Successor List --",
            *[
                f"\t{i}: {self.succlist[i]}"
                for i in range(self.succlistlen)
            ],
            f"-- Finger table --",
        ]
        last_fingerid = None
        printed_dots = False
        for i in range(self.system_bitwidth):
            fgr = self.fingers[i]
            if fgr and last_fingerid == fgr.nodeid and i != self.system_bitwidth-1:
                if not printed_dots: lines.append("\t...")
                printed_dots = True
            else:
                printed_dots = False
                last_fingerid = fgr and fgr.nodeid
                lines.append(f"\t{i} --> {self.finger_start(i)}: {fgr}")
        return '\n'.join(lines)

    def __contains__(self, key):
        # If a ndoe is alone in the cluster, it will be its own pred.
        # In this situation, it is responsible for the whole ring.
        # However, the inrange check will then be checking the range (me, me+1), which contains nothing.
        # So, we need to consider this case separately.
        try:
            return self.pred.nodeid == self.nodeid or self.inrange(key, self.pred.nodeid, self.mod(self.nodeid+1))
        except AttributeError:
            return utils.TryAgainError

    def __init__(self, cluster_name=None, force_nodeid=None, redirect_to=None, bitwidth=128, succlistlen=4, stabilizer_timeout=1, fixer_timeout=3, lookup_timeout=3, failure_timeout=5, verbose=False, succs_callback=lambda x,y: None, pred_callback=lambda x: None):
        if cluster_name is None:
            raise ValueError("keyword argument cluster_name must be supplied")
        self.system_bitwidth = bitwidth
        self.myip = utils.myip()
        self.destruct = False

        self.fingers = [None for _ in range(self.system_bitwidth)]
        self.pred = None
        self.port = None
        self.succlistlen = succlistlen
        self.succlist = [None for _ in range(self.succlistlen)]
        self.cluster_name = cluster_name 
        self.leaving = False
        self.verbose = verbose
        self.redirect_to = redirect_to
        self.failure_timeout = failure_timeout
        self.stabilizer_timeout = stabilizer_timeout
        self.fixer_timeout = fixer_timeout
        self.succs_callback = succs_callback
        self.pred_callback = pred_callback
        self.cxn_kwargs = {
            'timeout': self.failure_timeout,
            'verbose': self.verbose,
        }
        self.catalog = NDCatalog() # save to instance variable so it doesn't go out of scope and get garbage collected

        # First, start listening so we know our own port number to hash into our ip address.
        # Also important to listen before stabilizing so that if other nodes learn of our existence via stabilize, then they can contact us
        
        # Start listening
        self.lookup_timeout = lookup_timeout
        threading.Thread(target=self.server, daemon=True).start()
        # Determine nodeid: allow multiple nodes on same machine by including port in address to be hashed
        while not self.port: time.sleep(1)
        self.nodeid = self.hash(f'{self.myip}:{self.port}'.encode('utf-8')) % (2 ** self.system_bitwidth) if force_nodeid is None else force_nodeid
        # Setup
        self.setup() 
        # Start fixing fingers periodically
        threading.Thread(target=self.fixer, daemon=True).start() 

    def join(self):
        # TODO: look into more effecient use of time for stabilizer and fixer than sleeping? what does the mechanism in utils.repeat do?

        # Don't register until we're ready to also start stabilizing.
        # Consider the following:
        #  - system consisting of node A and node B
        #  - node B fail, unbeknownst to node A
        #  - node C attempts to join the system, learning A is its succ and attempts to push_keys
        #  - node A stabilizes, sees it has no succs,
        #    and attempts to find another node from the catalog,
        #    and so does not setup an empty system with itself as pred.
        #  - here is the problem: node C is not yet running stabilize, so even if
        #    it sets node A as its succ, node A will never learn C is its pred.
        #  - meanwhile, node C has been retrying push_keys this whole time,
        #    waiting until node A has a new pred via another node's stabilize.
        #  - however, no stabilize is coming, and push_keys will never succeed.
        #  - TL;DR INFINITE LOOP BAD

        # Register with catalog (allows other nodes to use us to join system)
        project = f'{self.cluster_name}:{str(self.nodeid)}:{self.port}:chordclient'
        self.catalog.register('chord', project, self.port, 'tfisher4') # Spawns registrar thread
        if self.verbose: print(f'[{utils.now()}][Registrar] Registered as {project} to catalog service...')
        # Start stabilizing periodically: announcing presence to other nodes
        threading.Thread(target=self.stabilizer, daemon=True).start()

    def finger_start(self, fgr_idx):
        return self.mod(self.nodeid + 2 ** fgr_idx)

    def mod(self, n): return n % (2 ** self.system_bitwidth)

    def setup(self, blacklist=set()):
        # TODO: copy succ's finger table when setting up for some baseline approximations?
        #   - will have to be careful in degenerate (first node) case that there will be no FT to copy
        if self.verbose: print(f"[{utils.now()}][Setup: Client (first) or Stabilizer (rest)] Attempting to set up self for participation in cluster {self.cluster_name}...")

        # Attempt to locate nonself node in same cluster from catalog.
        # Make a random choice to hopefully distribute the load of bootstrapping joining nodes.
        # If none exist, we are the first node in the cluster.
        try: 
            '''
            def choose(peers):
                suitable = [node for node in peers if self.cluster_name in node.get('project', '') and 'chordclient' in node.get('project', '') and not f'{self.nodeid}:{self.port}' in node.get('project', '') and node.get('project', '') not in blacklist]
                if self.verbose: print(f"[{utils.now()}][Join] Found {len(suitable)} suitable peers: {suitable}")
                choice = random.choice(suitable)
                if self.verbose: print(f"[{utils.now()}][Join] Chose {choice}")
                return choice
            liaison = RPCClient(choose)
            '''
            def is_valid_liaison(node):
                project = node.get('project', '')
                is_in_cluster = self.cluster_name in project
                is_chordclient = 'chordclient' in project
                is_self = f'{self.nodeid}:{self.port}' in project
                is_blacklisted = project in blacklist
                return is_in_cluster and is_chordclient and not is_self and not is_blacklisted
            choices = [node for node in self.catalog.query() if is_valid_liaison(node)]
            if self.verbose: print(f"[{utils.now()}][Setup] Found {len(choices)} suitable peers to act as liaison: {choices}")
            liaison_info = random.choice(choices)
            if self.verbose: print(f"[{utils.now()}][Setup] Chose {liaison_info}")
            # Add liaison as successor so it will naturally be involved in our first lookup of our successor (so we can stabilize later to that successor and thus gradually join)
            while True:
                try:
                    #self.fingers[0] = Node(*liaison.lookup(self.nodeid + 1), **self.cxn_kwargs)
                    #self.pred = Node(self.nodeid+1, (self.myip, self.port), **self.cxn_kwargs)
                    # Node with addr None will raise CxnErr when rpc attempted
                    self.pred = Node(self.mod(self.nodeid+1), (None, None), **self.cxn_kwargs)
                    if self.verbose: print(f"[Setup] Setting succ to liaison: {self.fingers[0]} --> {liaison_info['address']}:{liaison_info['port']}")
                    # Choose nodeid of -1 so that it is obvious this is a dummy node.
                    # We could probably ask liaison for nodeid, or parse from catalog project name or something, but unneeded.
                    # Set to -1 to ensure that we don't accidentally interpret this as a real node.
                    self.fingers[0] = Node(-1, (liaison_info['address'], liaison_info['port']), **self.cxn_kwargs)
                    # Ping here so we don't panic if we can't connect to liaison in lookup
                    self.fingers[0].rpc.ping()
                    self.fingers[0] = Node(*self.lookup(self.mod(self.nodeid+1)), **self.cxn_kwargs)
                    if self.verbose: print(f"[Setup] Looked up succ and now changing {liaison_info['address']}:{liaison_info['port']} --> {self.fingers[0].nodeid}")
                    # Note that 'rest' means that i starts at 1:
                    # we do not allow our own successor to be overriden.
                    for i, fgr in utils.rest(enumerate(self.fingers[0].rpc.finger_table())):
                        self.fingers[i] = fgr and Node(*fgr, **self.cxn_kwargs)
                    if self.verbose: print("Finished setting up!")
                    if self.verbose: print(self)

                    break
                except utils.TryAgainError:
                    if self.verbose: print(f"[{utils.now()}][Setup] Liaison reported ring inconsistency, waiting and trying again...")
                    time.sleep(self.lookup_timeout)
            # TODO: fix this \ in final go around
            if self.verbose: print(f"[{utils.now()}][Setup] Successfully set up self on Chord level to join cluster with {self.fingers[0].nodeid} \
                    as successor via liaison {liaison_info['address']}:{liaison_info['port']}...")
        # Note that we set pred to ourself so that when we go to push_keys from ourself BEFORE stabilize runs (since we delay stabilizing until we have the keys are responsible for, lest our succ forgets about pvs pred and cannot accurately report all keys we now own), we don't infinite loop waiting for someone else's stabilize to set our pred so we can run.
        # TODO: delete this comment: note that pred stays None: stabilizes will fix this later.
        # Once we have integrated as succ's pred, then succ's former pred will contact us with suspected_pred
        except IndexError: # could not choose from empty peerlist: node is first member of chord cluster
            # The use of a Node/RPCClient to ourselves allows uniform handling of degenerate case
            #  - https://stackoverflow.com/a/8118180 suggests it is fine to have same process at both ends of a socket
            if self.verbose: print(f"[{utils.now()}][Setup] First node in cluster, setting self as succ and pred...")
            self.fingers[0] = Node(self.nodeid, (self.myip, self.port), **self.cxn_kwargs)
            self.pred = self.fingers[0].copy()
        except ConnectionError: # found liaison in catalog but couldn't connect
            if self.verbose: print(f"[{utils.now()}][Setup] Couldn't connect to liaison, retrying...")
            self.setup(blacklist | {liaison_info['project']})

    # Iterative lookup: return next node to talk to (if that person is me -- same node twice in a row, HTC will get the value from me)
    #   - just return from cpf
    # Recursive lookup:
    #   - benefit: sockets already open
    def lookup(self, hashed_key):
        if self.destruct: raise utils.SelfDestructError
        if self.verbose: print(f"Looking up {hashed_key}...")
        #print(f"[{utils.now()}][CClient] Looking up {hashed_key}...")
        #hashed_key = self.hash(key.encode('utf-8')) % (2 ** self.system_bitwidth)
        try:
            # TODO: a priori this seems fine, but maybe a little worrisome given how flaky it seems self-sockets are
            last_node = None
            node = Node(self.nodeid, (self.myip, self.port), **self.cxn_kwargs)
            #addr_to_return = self.redirect()
         # Shouldn't ever happen that we are asked to perform a lookup before our server is running,
         # bc the server is how we receive requests.
         # Note also that our constructor blocks until server is running before starting other threads,
         # so no chance we could ask ourselves to lookup before server running.
        except AttributeError: print("SDKJBSDF"); sys.exit()
        #    if self.verbose: print(f"[{utils.now()}][Server] Tried lookup before server running, sleeping and will try again...")
        #    time.sleep(self.lookup_timeout)
        #    return self.lookup(hashed_key)
 
        while not last_node or node.nodeid != last_node.nodeid:
            try:
                if self.verbose: print(f"[{utils.now()}][CClient] Trying {node.nodeid}...")
                nxt_nodeid, nxt_nodeaddr = node.rpc.closest_preceding_finger(hashed_key) # return nodeid instead to find connection
                #addr_to_return = node.rpc.redirect()
            except ConnectionError:
                # Note that as soon as we wind up here, we know that this won't be the last iteration of the while loop.
                # The later 'if' condition matches the while exit condition,
                # so if we were going to exit the while loop we would hit that 'if' first.
                #if self.verbose: print(f"Node down: trying last node ({prev_nodeid})...")
                if last_node is None:
                    if self.verbose: print(f"[{utils.now()}][CClient] Node {node.nodeid} down with last_node being None, meaning we cannot contact ourselves :/")
                    raise utils.TryAgainError

                # Node down...find cpf(cpf(node)) and so on
                try:
                    if self.verbose: print(f"[{utils.now()}][CClient] Node {node.nodeid} down: trying last node ({last_node.nodeid})...")
                    nxt_nodeid, nxt_nodeaddr = last_node.rpc.closest_preceding_finger(self.mod(node.nodeid-1))
                except ConnectionError:
                    #if self.verbose: print(f"Previous node is down: waiting so we can stabilize and retrying...")
                    if self.verbose: print(f"[{utils.now()}][CClient] Previous node {last_node.nodeid} is down: waiting so we can stabilize and retry...")
                    raise utils.TryAgainError
                    # Failed lookup; retry 
                    #time.sleep(self.lookup_timeout)
                    #return self.lookup(hashed_key)

                # If the node is last_node.cfp(node-1), that means last_node is trying to redirect us to its successor
                # (look at special case for successor in cfp)
                # If successor is down wait for a while (for stabilize()/poke() to straighten things out) and retry
                if nxt_nodeid == node.nodeid:
                    #if self.verbose: print(f"Previous node's successor is down: waiting so we can stabilize and retrying...")
                    if self.verbose: print(f"[{utils.now()}][CClient] Previous node's {last_node.nodeid} successor is down: waiting so we can stabilize and retrying...")
                    raise utils.TryAgainError
                    # Failed lookup; retry 
                    #time.sleep(self.lookup_timeout)
                    #return self.lookup(hashed_key)

            # TODO: optimize to use already established cxn, if available
            # To achieve this, might be nice to have a fingertable class which maintains a list and a dict
            # to achieve an orderred dict.
            # Supports indexing with nodeids via __getindex__/__setindex__,
            # and also supports use of 'in' with __contains__.
            # Problem: distinguishing nodeid access vs fgr_idx access

            last_node = node
            node = Node(nxt_nodeid, nxt_nodeaddr, **self.cxn_kwargs)
            if self.verbose: print(f"[{utils.now()}][CClient] Advancing node to {nxt_nodeid}, last_node to {last_node.nodeid}")
        return node.nodeid, node.addr

    def closest_preceding_finger(self, key):
        if self.verbose: print(f"[{utils.now()}][CClient] Finding closest preceding finger of {key}...")
        # Logic (see p.249 in textbook)
        try:
            if self.inrange(key, self.pred.nodeid, self.mod(self.nodeid+1)) or self.pred.nodeid == self.nodeid:
                # In our range: we are responsible.
                # Note that returning ourselves here, and succ when succ is responsible in next case,
                # contracts the name "closest preceding finger", since actually the node returned in these
                # cases (ourself or succ) SUCCeeds the key.
                # However, the important part here is that we know we are not skipping over the node responsible,
                # as may happen if one returns the succeeding finger.
                # Indeed, lookup will ask us or succ to find key, they will themselves, and then search will stop.
                #if self.verbose: print(f"I am responsible for that key ({key})...")
                if self.verbose: print(f"[{utils.now()}][Server] I am responsible for that key ({key})...")
                # TODO: remove if we don't encounter problems without it.
                # Wait until server has started.
                # Shouldn't actually happen since we wait for server to start before starting other threads
                #while self.port is None: 
                #    time.sleep(self.lookup_timeout)
                return self.nodeid, (self.myip, self.port) 
        except AttributeError:
            if self.verbose: print(f"[{utils.now()}][Server] No predecessor right now, telling original node to try again in a few")
            # TODO: something more efficient than sleeping?
            raise utils.TryAgainError
            #time.sleep(self.lookup_timeout)
            #return self.closest_preceding_finger(key)
        except AttributeError: raise utils.TryAgainError

        try:
            if self.inrange(key, self.nodeid, self.mod(self.fingers[0].nodeid+1)):
                # In successor's range: return successor. See note in above case.
                #if self.verbose: print(f"My successor is responsible for that key ({key})...")
                if self.verbose: print(f"[{utils.now()}][CServer] My successor is responsible for that key ({key})...")
                return self.fingers[0].nodeid, self.fingers[0].addr
        except AttributeError: # No successor: retry, but not at me
            if self.verbose: print(f"[{utils.now()}][CServer] No successor right now, telling original node to try again in a few")
            raise utils.TryAgainError

        # Find cpf to talk to for more accurate info
        for i in range(self.system_bitwidth):
            try:
                if self.inrange(key, self.mod(self.fingers[i].nodeid-1), self.fingers[(i+1)%self.system_bitwidth].nodeid):
                    #if self.verbose: print(f"I am recommending you talk to {self.fingers[i].nodeid} for more information...")
                    if self.verbose: print(f"[{utils.now()}][CServer] I am recommending you talk to {self.fingers[i].nodeid} for more information...")
                    return self.fingers[i].nodeid, self.fingers[i].addr
            except AttributeError: # occurs when next finger has not yet been determined (is None)
                # go ahead and stop the search here. TODO: thoughts on trying to find the next non-None finger?
                try:
                    return self.fingers[i].nodeid, self.fingers[i].addr
                except AttributeError:
                    #print("[CPF] Successor down, sleeping and retrying...")
                    if self.verbose: print(f"[{utils.now()}][CPF] Successor down, failing...")
                    raise utils.TryAgainError
                    #time.sleep(self.lookup_timeout)
                    #return self.closest_preceeding_finger(key)
        if self.verbose: print("Reached end of cpf, meaning our last finger (and the rest, too) is equal to our successor. Returning last finger, then.")
        return self.fingers[-1].nodeid, self.fingers[-1].addr

    def leave(self):
        # any implementation at the chord client level? maybe flip on "leaving" switch?
        self.leaving = True # tells listener to respond as if node had left the network

    def fixer(self):
        if self.verbose: print(f"[{utils.now()}][Poker] Starting poker...")
        while not self.leaving:
            self.fix_finger()
            # TODO: something more efficient than sleeping?
            time.sleep(self.fixer_timeout)

    def fix_finger(self):
        # if system is shot, shut it down
        if self.destruct:
            if self.verbose: print(f"[{utils.now()}][Poker] Shutting down...")
            return
        fgr_idx = random.choice([i for i, _ in enumerate(self.fingers)])
        if self.verbose: print(f"[{utils.now()}][Poker] Fixing finger {fgr_idx}")
        while True:
            try:
                corr_fgr, corr_addr = self.lookup(self.finger_start(fgr_idx))
                #corr_fgr, corr_addr = Node(self.nodeid, (self.myip, self.port)).rpc.lookup(self.finger_start(fgr_idx))
                break
            except (utils.TryAgainError, ConnectionError):
                return # this will sleep the fixer thread and try again in a few
                #time.sleep(self.lookup_timeout)
        if self.fingers[fgr_idx] is None or corr_fgr != self.fingers[fgr_idx].nodeid:
            if self.verbose: print(f"[{utils.now()}][Poker] Set finger {fgr_idx}: {self.fingers[fgr_idx].nodeid if self.fingers[fgr_idx] else None} --> {corr_fgr}...")
            self.fingers[fgr_idx] = Node(corr_fgr, corr_addr, **self.cxn_kwargs)
            # Slight optimization: correct as many fingers as able at the current time.
            # If fgr i's (closest) succ succeeds fgr i+1's start, then it is also fgr i+1's (closest) succ,
            # so we can go ahead and adjust fgr i+1 also.
            # Continue down the line as long as we are making useful adjustments.
            # Note that this is an optimization because we can fix several fingers with only the original lookup call.
            # However, stop chain if fgr i+1 already has correct finger: we can get no more use of succ(fgr i) in this case.
            # If fgr i+1 has correct finger, then if this succ(fgr i) also succeeds fgr i+2,
            # it will have already been updated by this same process, when fgr i+1 last updated its successor.

            #i = 1
            n = self.system_bitwidth
            # Note that we use self.nodeid+1 as the (exclusive) end bound,
            # because the furthest clkwise any finger can point is ourself (otherwise, ourself is a closer successor).
            check_idx = (fgr_idx + 1) % n
            check_fgr = self.fingers[check_idx]
            while self.inrange(self.finger_start(check_idx), self.mod(self.nodeid+1), corr_fgr) and (check_fgr is None or check_fgr.nodeid != corr_fgr):
                #if self.verbose: print(f"\t [Poker] While at it, also set finger {fgr_idx+i} to {corr_fgr}, as this is the (closest) successor of a smaller finger's start, meaning it this finger must be stale...")
                self.fingers[check_idx] = self.fingers[fgr_idx] # TODO: is this dangerous to share socket/Node objs between fingers?
                #i += 1
                check_idx = (check_idx+1) % n
                check_fgr = self.fingers[check_idx]
        elif self.verbose: print(f"[Poker] Finger {fgr_idx} was already correct")

    def stabilizer(self):
        if self.verbose: print("[Stabilizer] Starting stabilizer...")
        while not self.leaving:
            # if system is shot, shut it down
            if self.destruct:
                if self.verbose: print(f"[{utils.now()}][Stabilizer] Shutting down...")
                return
            self.stabilize()
            # TODO: something more efficient than sleeping?
            time.sleep(self.stabilizer_timeout)

    def pop_succ(self):
        # Next man up in succlist is taken as new succ
        self.succlist = self.succlist[1:] + [None]
        if self.verbose: print(f"My successor {self.fingers[0]} died, so I am changing my successor to {self.succlist[0]}")
        self.fingers[0] = self.succlist[0]
        #print(self.fingers)
        #print(self.succlist)
        #import sys
        #sys.exit()
        if self.fingers[0] is None:
            # TODO: think harder about implications heree and if anything can go wrong,
            # since join is originally called without possibility of server/stabilize/finger interrupting
            #  - server bc we are not yet part of circle or registered with catalog, so no one can find us
            if self.verbose: print(f"[{utils.now()}][Stabilizer][REJOIN|BAD] Lost all successors, rejoining system...")
            self.setup()
            if self.verbose: print("Our succlist has been completely depleted. We cannot guarantee all data has been retained. Preparing to self-destruct.")
            self.self_destruct = True
        return

    def stabilize(self):
        if self.verbose: print(f"[{utils.now()}][Stabilizer] Stabilizing...")

        # Maintain succ and sync their pred
        try:
            succ_pred = self.fingers[0].rpc.predecessor()
            if succ_pred is not None and self.inrange(succ_pred[0], self.nodeid, self.fingers[0].nodeid):
                if self.verbose: print(f"[{utils.now()}][Stabilizer] Found that succ.pred {succ_pred[0]} is a better successor than {self.fingers[0].nodeid}...")
                self.fingers[0] = Node(*succ_pred, **self.cxn_kwargs)
            # TODO: remove if no problems arise: should be fine here bc we delay stabilizing until port is known
            #while self.port is None: time.sleep(self.lookup_timeout) # delay until server running
            self.fingers[0].rpc.suspected_predecessor(self.nodeid, (self.myip, self.port))
        except ConnectionError:
            # Retry with new succ
            if self.verbose: print(f"[{utils.now()}][Stabilizer] Lost successor (when asking for pred/reporting self as pred)...")
            self.pop_succ()
            return self.stabilize()

        # Maintain succlist: copy succ's, then add them to front
        # Handle this case separately because everything else assumes
        # succlist should be [succ] + succ.succlist[1:].
        # Also saves us some work. After all, no succlist to maintain.
        if self.succlistlen == 0: return
        try:
            succ_succlist_tuples = self.fingers[0].rpc.successor_list()
        except ConnectionError:
            # Retry with new succ
            if self.verbose: print(f"[{utils.now()}][Stabilizer] Lost successor (when requesting succlist)...")
            self.pop_succ()
            return self.stabilize()

        # with static length, succ_succlist will be always full (and so should always chop off last entry)
        new_succlist_tuples = [(self.fingers[0].nodeid, self.fingers[0].addr)] + succ_succlist_tuples[:-1] 

        # Create temporary succclients LUT so we don't have to recreate Node/RPCClients we already have
        succclients = {
            node.nodeid: node
            for node in [succ for succ in self.succlist if succ is not None]
        }

        new_succlist = [None for _ in range(self.succlistlen)]
        first_succ_id = new_succlist_tuples[0][0]

        for i, succ in enumerate(new_succlist_tuples):
            if succ is None: break
            succ_id, succ_addr = succ
            if i > 0 and succ_id == first_succ_id: break
            new_succlist[i] = succclients.get(succ_id, Node(succ_id, succ_addr, **self.cxn_kwargs))

        #new_succlist = [
        #    # succ tuples of form: [id, addr] | None
        #    succclients.get(succ[0], Node(succ[0], succ[1], **self.cxn_kwargs))
        #    if succ is not None else
        #    None
        #    for succ in new_succlist_tuples
        #]
        if self.verbose: print(f"[{utils.now()}][Stabilizer] Changing succlist {self.succlist} --> {new_succlist}...")

        # TODO: was this actually necessary?
        # If the stabilizer thread performs the upcall, then sleeps, then runs stabilize again, how can it interrupt the upcall?
        #   - neat concept and thought experiment, but if stabilize instances interrupt each other we'll have much bigger problems
        # Update in-place so that current iterator references to the list reflect the updates made,
        # if stabilize preempts the thread and changes the succlist.
        # Also important is that we perform the update before upcalling to HashArmonica to ensure
        # specifically so that we change our list of successors before telling old succlist members
        # to drop their associated replicas.
        # First, make a copy of the old_succlist
        old_succlist = self.succlist.copy()
        for i, new_succ in enumerate(new_succlist): # (static length succlist)
            # this loses references to obsolete rpcs, closing sockets via __del__
            self.succlist[i] = new_succ
        self.succs_callback(old_succlist, new_succlist)

    def suspected_predecessor(self, src_id, src_addr):
        # TODO: make sure to handle case that pred is wrong, so lookup returns self when it shouldn't have
        #  - already done by utils.TryAgainError?

        # Ping pred to ensure alive
        # Moved to predecessor fxn so that pred will get set to None in stabilize, and then suspected_pred can change on next invocation
        """
        try:
            if self.verbose: print(f"[Server] Pinging pred {self.pred.nodeid}...")
            self.pred.rpc.ping()
            if self.verbose: print(f"[Server] Successfully contacted pred {self.pred.nodeid}")
        except ConnectionError: # If not, we'll accept this pred (better than nothing)
            if self.verbose: print(f"[Server] Unable to contact pred {self.pred.nodeid} ({self.pred.nodeid} --> None)")
            self.pred = None
        except AttributeError: # self.pred is None
            pass
        """

        if self.pred is None or self.pred.nodeid == self.nodeid or self.inrange(src_id, self.pred.nodeid, self.nodeid):
            old_pred = self.pred and self.pred.copy()
            if self.verbose: print(f"[{utils.now()}][Server] Contacted by better predecessor ({self.pred.nodeid if self.pred else 'None'} --> {src_id})...")
            #print(f"[Server] Contacted by better predecessor ({self.pred.nodeid if self.pred else 'None'} --> {src_id})...")
            self.pred = Node(src_id, src_addr, **self.cxn_kwargs)
            self.pred_callback(old_pred)
        else:
            if self.verbose: print(f"[{utils.now()}][Server] Contacted by a worse predecessor (keeping {self.pred.nodeid} in favor of {src_id})...")
        #else: print(f"[Server] Contacted by a worse predecessor (keeping {self.pred.nodeid} in favor of {src_id})...")

    def predecessor(self):
        # We ping our predecessor here to ensure we don't give back bad info
        # Normally, not a big deal to tell someone to switch their succ to someone dead
        # However, in 2-node case, this causes the live node to think dead pred is a better succ,
        # then find out succ is dead, then set succ back to self (next in line), then re-stabilize and 
        # Possible alternative: just finish the current instance of stabilize with new succ and 
        if self.pred and self.pred.nodeid == self.nodeid: return (self.pred.nodeid, self.pred.addr)
        try:
            self.pred.rpc.ping()
            if self.verbose: print(f"[{utils.now()}][Server] Pang pred {self.pred.nodeid}")
            #print(f"[Server] Pang pred {self.pred.nodeid}")
            return (self.pred.nodeid, self.pred.addr)
        except ConnectionError:
            if self.verbose: print(f"[{utils.now()}][Server] Unable to contact pred {self.pred.nodeid} ({self.pred.nodeid} --> None)")
            #print(f"[Server] Unable to contact pred {self.pred.nodeid} ({self.pred.nodeid} --> None)")
            self.pred = None
        except AttributeError: pass # attribute error if pred is none, so no rpc/nodeid attr
        
        return None

    def successor(self): return self.fingers[0].nodeid, self.fingers[0].addr

    def successor_list(self): return [[succ.nodeid, succ.addr] if succ else None for succ in self.succlist]

    def finger_table(self):
        return [
            [fgr.nodeid, fgr.addr]
            if fgr is not None else None
            for fgr in self.fingers
        ]

    def redirect(self):
        # Note: should be fine to return self.port here,
        # since this function is only accessed as an RPC.
        return self.redirect_to or (self.myip, self.port)

    def ping(self): pass

    # SERVER THREAD FUNCTIONS: Handle communication from other nodes
    # TODO: switch over to utils' version?
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
        if self.verbose: print(f'[{utils.now()}][Server] Listening on port {self.port}...')
        while True: # Poll forever
            # if system is shot, shut it down
            if self.destruct:
                if self.verbose: print(f"[{utils.now()}][Server] Shutting down...")
                return
            readable, _, _ = select.select(socket_to_addr, [], []) # blocks until >= 1 skt ready
            for rd_skt in readable:
                if socket_to_addr[rd_skt] is None:
                    new_skt, addr = new_cxns.accept()
                    socket_to_addr[new_skt] = addr
                    socket_to_msgs[new_skt] = utils.nl_socket_messages(new_skt)
                    if self.verbose: print(f'[{utils.now()}][Server] Accepted connection with {addr[0]}:{addr[1]}...')
                    continue
                addr = socket_to_addr[rd_skt]
                    
                try: # Assume cxn unbreakable, client waiting for rsp
                    try: # Assume request is valid JSON in correct format corresponding to valid operation 
                        try: # Assume request is valid JSON encoded via utf-8
                            request = utils.decode_object(next(socket_to_msgs[rd_skt]))
                        except utils.RequestFormatError as e:
                                raise BadRequestError(e)
                        if self.verbose: print(f'[{utils.now()}][Server] Received request {request} from {addr[0]}:{addr[1]}...')
                        res = self.dispatch(request)
                    except BadRequestError as e:
                        res = e
                    rsp = self.build_response(res)
                    utils.send_nl_message(rd_skt, utils.encode_object(rsp))
                    if self.verbose: print(f'[{utils.now()}][Server] Sent response {rsp} to {addr[0]}:{addr[1]} regarding {request}...')
                except (ConnectionError, StopIteration):
                    if self.verbose: print(f'[{utils.now()}][Server] Lost connection with {addr[0]}:{addr[1]}.')
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
            if self.verbose: print(f"Dispatcher got {type(e).__name__}")
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

