from ChordClient import ChordClient, Node
import HashArmonicaUtils as utils
from RPCClient import RPCClient
import threading

from sortedcontainers import SortedDict
import collections
import time

class HashArmonica:
    def __init__(self, cluster_name=None, bitwidth=128, replication_factor=4, stabilizer_timeout=2, fixer_timeout=5, wait_for_stable_timeout=10, failure_timeout=15, verbose=False):
        self.bitwidth = bitwidth
        self.verbose = verbose
        self.replication_factor = replication_factor
        self.replicas = [None for _ in range(replication_factor-1)] 
        
        # Store the data that we own/have replicas
        self.table = SortedDict({})

        # Spawn server thread to listen
        #self.catalog = NDCatalog()
        # TODO: don't register with catalog from utils.Server
        self.port = None
        threading.Thread(target=lambda: utils.Server(self, 'hasharmonicaclient'), daemon=True).start()
        # delay until server is up and running
        # TODO: something more efficient than sleeping?
        while not self.port: time.sleep(1)
        self.wait_for_stable_timeout = wait_for_stable_timeout
       
        # Pass in callback function to ChordPeer to call when alert is needed
        # Counting self into the replication count (but not the replicas array), so only need replication_factor-1 extra replicas (where each member of succlist will be an extra replica)
        self.chord = ChordClient(cluster_name=cluster_name, redirect_to=(utils.myip(), self.port), bitwidth=bitwidth, succlistlen=replication_factor-1, stabilizer_timeout=stabilizer_timeout, fixer_timeout=fixer_timeout, lookup_timeout=wait_for_stable_timeout, failure_timeout=failure_timeout, verbose=verbose, callback_fxn=self.new_succlist_callback_fxn)


        
        # TODO: ChordClient returns nodeid, successor
        # succ is a tuple with ?
        self.nodeid = self.chord.nodeid
        print("I am {self.nodeid}")

        
        # TODO: Query successor with our nodeid to obtain the data that we now own

        # TODO: don't reuse chord rpc clients: create own
        self.failure_timeout = failure_timeout
        while True:
            try:
                init_data = Node(self.chord.fingers[0].nodeid, self.chord.fingers[0].copy().rpc.redirect(), **self.chord.cxn_kwargs).rpc.push_keys(self.nodeid)
                break
            except (AttributeError, ConnectionError):
                time.sleep(self.wait_for_stable_timeout)

        #init_data = self.replicas[0].rpc.push_keys(self.nodeid)
        self.mass_raw_insert(init_data['data']) # gonna need to send something to signify joining

        wrapped = False
        for i, node in enumerate(init_data['replicas']):
            if node and node[0] == self.nodeid: wrapped = True
            self.replicas[i] = Node(*node, **self.chord.cxn_kwargs) if not wrapped and node else None

    ''' CLIENT FACING METHODS '''

    ''' Implements insertion into the hash table with chain replication
    '''

    def insert(self, key, value):
        return self.perform(lambda node, k_hash, val: node.rpc.store(k_hash, val), key, value)

    def delete(self, key):
        return self.perform(lambda node, k_hash: node.rpc.remove(k_hash, val), key)

    def lookup(self, key):
        return self.perform(lambda node, k_hash: node.rpc.map(k_hash), key)


    ''' REMOTE PROCEDURES '''

    def store(self, hashed_key, value):
        # If a node is alone in the cluster, it will be its own pred.
        # In this situation, it is responsible for the whole ring.
        # However, the inrange check below checks the range (me, me+1), which contains nothing.
        # So, consider the special case separately.
        if not (self.chord.pred.nodeid == self.nodeid or self.chord.inrange(hashed_key, self.chord.pred.nodeid, self.nodeid+1)):
            print(f"Was told to store {hashed_key}, which falls outside my range of ({self.chord.pred.nodeid} --> {self.nodeid}]")
            raise utils.TryAgainError
        self.raw_insert(hashed_key, value)

        # This iterator will reflect in-place updates to succlist that are performed by stabilize.
        # For this reason, as long as we are careful to update out succlist in-place and before
        # dropping keys from previous replicas, we can be sure that previous replicas either
        # don't receive the insert, or receive it before the stabilize thread tells it to drop
        # its keys, meaning that there can be no weird card where we start this loop,
        # then the stabilize thread runs, discovers a replica not et reached by this loop that
        # to drop a key, then when resuming this loop, asks it to insert that key again.
        for replica in self.replicas:
            if replica is None: break
            try:
                replica.rpc.raw_insert(hashed_key, value)
            except ConnectionError: # TODO: exception here?
                pass
        print(f"Stored and replicated {hashed_key}->{value}. My new table:")
        print(self.table)

    def raw_insert(self, key, value):
        self.table[key] = value

    def remove(self, hashed_key):
        if not (self.chord.pred.nodeid == self.nodeid or self.chord.inrange(hashed_key, self.chord.pred.nodeid, self.nodeid+1)):
            raise utils.TryAgainError
        to_return = self.raw_delete(hashed_key)
        for replica in self.replcas:
            try:
                replica.rpc.raw_delete(key)
            except (ConnectionError, KeyError):
                # Replica down or key missing --> key already dropped
                pass
        return to_return

    def raw_delete(self, key):
        return self.table.pop(key)

    def map(self, hashed_key):
        if not (self.chord.pred.nodeid == self.nodeid or self.chord.inrange(hashed_key, self.chord.pred.nodeid, self.nodeid+1)):
            raise utils.TryAgainError
        return self.table[hashed_key]

    def drop(self, rg):
        print(f"Dropping keys in the range {rg}")
        if rg[0] > rg[1]:
            rg = [(None, rg[1]), (rg[0], None)]
        else:   rg = [rg]
        for r in rg:
            for key in list(self.table.irange(minimum=r[0], maximum=r[1])):
                self.raw_delete(key)

    ''' Used when new node joins the system and is now responsible
    for some of the data items that its successor was holding on to
    '''
    def push_keys(self, joiner):
        # Also return the succ list
        while self.chord.pred is None: time.sleep(self.wait_for_stable_timeout)
        return {
            'data': [[k, self.table[k]] for k in self.table.irange(self.chord.pred.nodeid+1, joiner)],
            'replicas': [[replica.nodeid, replica.addr] if replica else None for replica in self.replicas]
        }

    def mass_raw_insert(self, k_v_pairs):
        for k, v in k_v_pairs:
            self.raw_insert(k, v)
        print("Table after mass-insert:")
        print(self.table)


    ''' INTERNAL UTILITIES '''


    def perform(self, fxn, key, *args):
        hashed_key = self.chord.hash(key.encode('utf-8')) % (2 ** self.bitwidth)
        while True:
            owner_id, owner_addr = self.chord.lookup(hashed_key)
            print(f"[Client] {hashed_key} belongs to {owner_id}")
            print(f"[Client] Ring: {self.chord.succlist}")
            node = Node(
                owner_id,
                Node(owner_id, owner_addr, **self.chord.cxn_kwargs).rpc.redirect(),
                **self.chord.cxn_kwargs
            )
            try:
                return fxn(node, hashed_key, *args)
            except (utils.TryAgainError, ConnectionError, AttributeError) as e: # AttributeError in case pred.nodeid fails bc pred is None
                print(f"[Client] Operation failed: waiting for stabilize and retrying...")
                print(str(e))
                time.sleep(self.wait_for_stable_timeout)

    ''' For scan we would need to either sort with the hash function or 
    store string keys in the value field of the SortedDict table
    ''' 
    
    ''' UPCALL FUNCTIONS '''

    def new_succlist_callback_fxn(self, old_succlist, new_succlist):
        #old_succs = set(old_succlist)
        #new_succs = set(new_succlist)
        # NOTE: may only need new_succlist if self.replicas is essentially old_succlist

        old_replicas_dict = {
            node.nodeid: node
            for node in [n for n in self.replicas if n]
        }

        new_replicas = [None for _ in range(self.replication_factor-1)]
        idx = 0
        for node in new_succlist:
            if not node: continue
            if node.nodeid == self.nodeid: break
            try:
                node_hasharmonica_addr = node.rpc.redirect()
            except ConnectionError:
                continue
            node = old_replicas_dict.get(node.nodeid, Node(node.nodeid, node_hasharmonica_addr, **self.chord.cxn_kwargs))
            new_replicas[idx] = node
            idx += 1 
        if self.verbose: print(f"[Stabilizer] New replica list: {self.replicas} --> {new_replicas}")

        # update replicas in-place here?
        wrapped = False
        for i, new_replica in enumerate(new_replicas):
            if new_replica and new_replica.nodeid == self.nodeid:
                wrapped = True
            self.replicas[i] = new_replica if not wrapped else None

        while True:
            try:
                pred_id = self.chord.pred.nodeid
                break
            except AttributeError:
                if self.verbose: print(f"[Stabilizer] Lost connection with predecessor, waiting for new one...")
                time.sleep(self.wait_for_stable_timeout)

        k_v_pairs = [[k, self.table[k]] for k in self.table.irange(pred_id+1, self.nodeid)]

        new_replicas_set = set(new_replicas) - {None}
        old_replicas_set = set(old_replicas_dict.values()) - {None}

        # Note: create new RPCClient.scket for each rpc here bc the ones in new/old_replicas_dict could be in use by the server
        for newbie in new_replicas_set - old_replicas_set:
            #if self.verbose: print(f"[Stabilizer] Copying keys to {newbie}...")
            print(f"[Stabilizer] Copying keys to {newbie}...")
            try: newbie.copy().rpc.mass_raw_insert(k_v_pairs)
            except ConnectionError: pass
        for olbie in old_replicas_set - new_replicas_set:
            #if self.verbose: print(f"[Stabilizer] Dropping replicas from {olbie}...")
            print(f"[Stabilizer] Dropping replicas from {olbie}...")
            try: olbie.copy().rpc.drop([pred_id+1, self.nodeid])
            except ConnectionError: pass
