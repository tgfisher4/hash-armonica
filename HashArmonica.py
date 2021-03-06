from ChordClient import ChordClient, Node
import HashArmonicaUtils as utils
from RPCClient import RPCClient
import threading

from sortedcontainers import SortedDict
import collections
import time

class HashArmonica:
    def __init__(self, cluster_name=None, force_nodeid=None, bitwidth=128, replication_factor=5, stabilizer_timeout=2, fixer_timeout=5, wait_for_stable_timeout=10, failure_timeout=15, verbose=False):
        self.bitwidth = bitwidth
        self.verbose = verbose
        self.replication_factor = replication_factor
        # TODO: include self in replicas
        
        # Store the data that we own/have replicas
        self.table = SortedDict({})

        # TODO: wrap ALL rpcs in CxnErr handlers

        # Spawn server thread to listen
        # TODO: don't register with catalog from utils.Server
        self.port = None
        threading.Thread(target=lambda: utils.Server(self, 'hasharmonicaclient'), daemon=True).start()
        # delay until server is up and running
        # TODO: something more efficient than sleeping?
        while not self.port: time.sleep(1)
        self.wait_for_stable_timeout = wait_for_stable_timeout
       
       ## SETUP
        # Pass in callback function to ChordPeer to call when alert is needed
        # Counting self into the replication count (but not the replicas array), so only need replication_factor-1 extra replicas (where each member of succlist will be an extra replica)
        # Chord system is failstop: fails when all members of succlist dies.
        # So, a succlistlen of min(2, replication_factor) ensures that the system
        # stays up if and only if it can guarantee it has lost no information.
        # The minimum of 2 ensures we can always tolerate one node failure,
        # so that the system can be used for non-replicated data in a meaningful way.
        self.chord = ChordClient(
            cluster_name=cluster_name,
            force_nodeid=force_nodeid,
            redirect_to=(utils.myip(), self.port),
            bitwidth=bitwidth,
            succlistlen=max(2, replication_factor),
            stabilizer_timeout=stabilizer_timeout,
            fixer_timeout=fixer_timeout,
            lookup_timeout=wait_for_stable_timeout,
            failure_timeout=failure_timeout,
            verbose=verbose,
            succs_callback=self.new_succlist_callback_fxn,
            pred_callback=self.new_pred_callback_fxn
        )

        self.nodeid = self.chord.nodeid
        if self.verbose: print(f"I am {self.nodeid}")
        self.replicas = [None for _ in range(replication_factor)]

        
        # TODO: Query successor with our nodeid to obtain the data that we now own

        # TODO: don't reuse chord rpc clients: create own
        self.failure_timeout = failure_timeout
        while True:
            try:
                init_data = Node(
                    self.chord.fingers[0].nodeid,
                    self.chord.fingers[0].copy().rpc.redirect(),
                    **self.chord.cxn_kwargs
                ).rpc.push_keys(self.nodeid)
                break
            # AttrErr: succ is None (joining)
            # CxnErr: cannot reach succ: stabilize will resolve
            # TryAgainErr: succ.pred is None, so succcan't know range of
            #   keys to push to us. stabilizes will resolve
            except (AttributeError, ConnectionError, utils.TryAgainError):
                time.sleep(self.wait_for_stable_timeout)
        if self.verbose: print("INIT DATA: " + str(init_data))
        self.mass_raw_insert(init_data['data']) # gonna need to send something to signify joining

        wrapped = False
        for i, node in enumerate(init_data['replicas']):
            if node and node[0] == self.nodeid: wrapped = True
            self.replicas[i] = Node(*node, **self.chord.cxn_kwargs) if not wrapped and node else None

        ## OFFICIALLY JOIN (kick off stabilizer to be let into ring, start serving other HAPeers' requests)
        self.chord.join()

    def __str__(self):
        lines = [
            f"===== HASH ARMONICA PEER =====",
            f"-- Me --",
            f"\tReplication factor: {self.replication_factor}",
            f"-- Stored Data --",
        ]
        for k, v in self.table.items():
            is_mine = (k in self.chord if self.chord.pred is not None
                       else "unknown: no pred")
            lines.append(f"\t{k}: {v} | owned? {is_mine}")
        lines.append(f"-- Replicas --")
        for i, r in enumerate(self.replicas):
            lines.append(f"\t{i}: {self.replicas[i]}")
        lines += [
            f"-- Underlying Chord Peer --",
            str(self.chord),
            ''
        ]
        return '\n'.join(lines)


    ''' CLIENT FACING METHODS '''

    ''' Implements insertion into the hash table with chain replication
    '''

    def insert(self, key, value):
        return self.perform(lambda node, k_hash, val: node.rpc.store(k_hash, val), key, value)

    def delete(self, key):
        return self.perform(lambda node, k_hash: node.rpc.remove(k_hash), key)

    def lookup(self, key):
        return self.perform(lambda node, k_hash: node.rpc.map(k_hash), key)


    ''' REMOTE PROCEDURES '''

    def store(self, hashed_key, value):
        # TODO: catch TryAgainError?
        if not hashed_key in self.chord:
            if self.verbose: print(f"Was told to store {hashed_key}, which falls outside my range of ({self.chord.pred.nodeid} --> {self.nodeid}]")
            raise utils.TryAgainError

        # This iterator will reflect in-place updates to succlist that are performed by stabilize.
        # For this reason, as long as we are careful to update out succlist in-place and before
        # dropping keys from previous replicas, we can be sure that previous replicas either
        # don't receive the insert, or receive it before the stabilize thread tells it to drop
        # its keys, meaning that there can be no weird card where we start this loop,
        # then the stabilize thread runs, discovers a replica not et reached by this loop that
        # to drop a key, then when resuming this loop, asks it to insert that key again.

        # Must make a special case for self bc we are in server thread, so we can't service any request we make here
        self.raw_insert(hashed_key, value)
        for replica in utils.rest(self.replicas):
            if replica is None: break
            try: replica.rpc.raw_insert(hashed_key, value)
            # replica down: we'll discover on next stabilize
            except ConnectionError:
                if self.verbose: print(f"disconnected from {replica}")
                pass
        if self.verbose: print(f"Stored and replicated {hashed_key}->{value}. My new table:\n{self.table}")

    def raw_insert(self, key, value):
        self.table[key] = value

    def remove(self, hashed_key):
        # TODO: catch TryAgainError?
        if not hashed_key in self.chord:
            if self.verbose: print(f"Was told to remove {hashed_key}, which falls outside my range of ({self.chord.pred.nodeid} --> {self.nodeid}]")
            raise utils.TryAgainError
        # Must make a special case for self bc we are in server thread, so we can't service any request we make here
        to_return = self.raw_delete(hashed_key)
        for replica in utils.rest(self.replicas):
            # Replicas list is frontloaded: one None means rest None
            if replica is None: break
            try: to_return = replica.rpc.raw_delete(hashed_key)
            # Replica down or key missing --> key already dropped
            except (ConnectionError, KeyError): pass    
                
        if self.verbose: print(f"Removed and unreplicated {hashed_key}->{to_return}. My new table:")
        return to_return

    def raw_delete(self, key):
        return self.table.pop(key)

    def map(self, hashed_key):
        # TODO: catch TryAgainError?
        if not hashed_key in self.chord:
            raise utils.TryAgainError
        return self.table[hashed_key]

    def drop(self, rg):
        if self.verbose: print(f"Dropping keys in the range {rg}")
        for key in self.keys_in_range(*rg):
            self.raw_delete(key)
        """
        if rg[0] > rg[1]:
            rg = [(None, rg[1]), (rg[0], None)]
        else:   rg = [rg]
        for r in rg:
            for key in list(self.table.irange(minimum=r[0], maximum=r[1])):
                self.raw_delete(key)
        """

    ''' Used when new node joins the system and is now responsible
    for some of the data items that its successor was holding on to
    '''
    def push_keys(self, joiner):
        # Also return the succ list
        # NOTE: THIS BLOCKS, WAITING, IN HANDLING OF SVR REQ
        #   - don't think it should be an issue because lower level chord server is the one that updated chord.pred, but something to watch out for
        #while self.chord.pred is None: time.sleep(self.wait_for_stable_timeout)
        # Alternatively, could return TryAgainError and have client try again,
        # which seems to mesh better with our general strategy anyway. Doing it!
        try: pred_id = self.chord.pred.nodeid
        except AttributeError: raise utils.TryAgainError
        data_tuples = [[k, self.table[k]] for k in self.keys_in_range(self.chord.mod(pred_id+1), joiner)]
        replica_tuples = [[replica.nodeid, replica.addr] if replica else None for replica in self.replicas]
        if self.verbose: print(f"Pushing {data_tuples} to {joiner} with {replica_tuples}")
        return {
            'data': data_tuples,
            'replicas': replica_tuples
        }

    def mass_raw_insert(self, k_v_pairs):
        if self.verbose: print(f"Asked to mass_insert: {k_v_pairs}")
        for k, v in k_v_pairs:
            self.raw_insert(k, v)
        if self.verbose: print("Table after mass-insert:")
        if self.verbose: print(self.table)


    ''' INTERNAL UTILITIES '''


    def perform(self, fxn, key, *args):
        hashed_key = self.chord.hash(key.encode('utf-8')) % (2 ** self.bitwidth)
        while True:
            try:
                owner_id, owner_addr = self.chord.lookup(hashed_key)
                if self.verbose: print(f"[HAClient] {hashed_key} belongs to {owner_id}")
                if self.verbose: print(f"[HAClient] Ring: {self.chord.succlist}")
                node = Node(
                    owner_id,
                    Node(owner_id, owner_addr, **self.chord.cxn_kwargs).rpc.redirect(),
                    **self.chord.cxn_kwargs
                )
                return fxn(node, hashed_key, *args)
            except (utils.TryAgainError, ConnectionError) as e:#, AttributeError) as e: # AttributeError in case pred.nodeid fails bc pred is None
                if self.verbose: print(f"[HAClient] Operation failed: waiting for stabilize and retrying ({str(e)})...")
                #print(str(e))
                time.sleep(self.wait_for_stable_timeout)

    ''' For scan we would need to either sort with the hash function or 
    store string keys in the value field of the SortedDict table
    ''' 

    def keys_in_range(self, start, end):
        if self.verbose: print(self)
        if start <= end:
            res = list(self.table.irange(start, end))
        else:
            res = list(self.table.irange(start, None)) + list(self.table.irange(None, end))
        if self.verbose: print(f"k-v pairs found in range [{start}, {end}]: {res}")
        return res

    def my_keys(self): 
        try:
            pred_id = self.chord.pred.nodeid
            # Hacky and not satisfying soln that needs full knowledge of
            # cleverness used in ChordPeer.setup :(
            if self.chord.pred.addr[0] is None: raise utils.TryAgainError
        except AttributeError: raise utils.TryAgainError
        return self.keys_in_range(self.chord.mod(pred_id+1), self.nodeid)
    
    ''' UPCALL FUNCTIONS '''
    def new_pred_callback_fxn(self, old_pred):
        if old_pred is not None:
            if self.verbose: print(f"Adjusted pred from {old_pred}, not None, so skipping insert")
            return
        if self.verbose: print(f"Sending all my keys to my replicas")
        for replica in self.replicas:
            if replica is None: break
            try:
                replica.rpc.mass_raw_insert([[k, self.table[k]] for k in self.my_keys()])
            except ConnectionError:
                # if replica has gone down, we'll learn in next stabilize
                pass

    # TODO: remove new_succlist param since this can be accessed via self.chord?
    def new_succlist_callback_fxn(self, old_succlist, new_succlist):
        #old_succs = set(old_succlist)
        #new_succs = set(new_succlist)
        # NOTE: may only need new_succlist if self.replicas is essentially old_succlist

        old_replicas_dict = {
            node.nodeid: node
            for node in [n for n in self.replicas if n]
        }

        new_replicas = [None for _ in range(self.replication_factor)]
        new_replicas[0] = (self.replicas[0]
            if self.replicas[0] and self.replicas[0].nodeid == self.chord.nodeid
            else Node(self.nodeid, (self.chord.myip, self.port), **self.chord.cxn_kwargs)
        )

        idx = 1
        for node in new_succlist:
            if idx == len(new_replicas): break
            # succlist is frontloaded: this None --> rest None
            if node is None: break
            # Don't allow ourselves to be repeated in our replicas.
            if node.nodeid == self.nodeid: break
            try:
                node_hasharmonica_addr = node.rpc.redirect()
            except ConnectionError:
                # Replica can't be reached: we'll discover on next stabilize,
                # or get a head start and just disclude from replicas now to avoid
                # needless mass_insert/drop calls.
                continue
            node = old_replicas_dict.get(node.nodeid, Node(node.nodeid, node_hasharmonica_addr, **self.chord.cxn_kwargs))
            new_replicas[idx] = node
            idx += 1 
        if self.verbose: print(f"[Stabilizer] New replica list: {self.replicas} --> {new_replicas}")

        try: k_v_pairs = [[k, self.table[k]] for k in self.my_keys()]
        # Pred is None, so cannot determine my responsible range of keys.
        # Other nodes' stabilizes will correct my pred: wait for these.
        # Better luck next stabilize!
        # Have not updated replicas, so will retry copying to new replicas next time.
        except utils.TryAgainError:
            return

        # Update replicas in-place here, so that user requests to insert/delete a key
        # can view self.replicas without a lock and with the knowledge that a stale
        # read cannot mess up our system's correctness.
        #wrapped = False
        for i, new_replica in enumerate(new_replicas):
            #if new_replica and new_replica.nodeid == self.nodeid:
            #    wrapped = True
            self.replicas[i] = new_replica #if not wrapped else None
        if self.verbose: print(f"updated self.replicas to {self.replicas}")

        # No pred case is handled above when we search my_keys()
        """
        while True:
            try:
                pred_id = self.chord.pred.nodeid
                break
            except AttributeError:
                if self.verbose: print(f"[Stabilizer] Lost connection with predecessor, waiting for new one...")
                time.sleep(self.wait_for_stable_timeout)
        """

        new_replicas_set = set(new_replicas) - {None}
        old_replicas_set = set(old_replicas_dict.values()) - {None}

        # Note: create new RPCClient/socket for each rpc here bc the ones in new_replicas/old_replicas_dict could be in use by the server
        for newbie in new_replicas_set - old_replicas_set:
            #if self.verbose: print(f"[Stabilizer] Copying keys to {newbie}...")
            if self.verbose: print(f"[Stabilizer] Copying {k_v_pairs} to {newbie}...")
            try: newbie.copy().rpc.mass_raw_insert(k_v_pairs)
            except ConnectionError:
                if self.verbose: print(f"[Stabilizer] Couldn't copy into replica {newbie}, we'll try again next stabilization...")
                pass
        for olbie in old_replicas_set - new_replicas_set:
            #if self.verbose: print(f"[Stabilizer] Dropping replicas from {olbie}...")
            if self.verbose: print(f"[Stabilizer] Dropping replica of {k_v_pairs} from {olbie}...")
            try: olbie.copy().rpc.drop([self.chord.mod(self.chord.pred.nodeid+1), self.nodeid])
            except ConnectionError: pass # When nodes fail, they drop all anyway
            #except AttributeError: return # No pred: try again next time
