from ChordClient import ChordClient
import HashArmonicaUtils as utils
from RPCClient import RPCClient
import threading

from sortedcontainers import SortedDict

#TableEntry = collections.NamedTuple('TableEntry', 'value is_replica')

class HashArmonica:
    def __init__(self, cluster_name=None, bitwidth=128, succlistlen=4, stabilizer_timeout=1, fixer_timeout=3, failure_timeout=10, verbose=False):
        
        # Store the data that we own/have replicas
        self.table = SortedDict({})
       
        # Pass in callback function to ChordPeer to call when alert is needed
        #callback = lambda old_succlist, new_succlist: self.new_succlist_callback_fxn(old_succlist, new_succlist)
        self.chord = ChordClient(cluster_name, bitwidth, succlistlen, stabilizer_timeout, fixer_timeout, failure_timeout, verbose, self.new_succlist_callback_fxn)

        # Spawn server thread to listen
        threading.Thread(target=lambda: utils.Server(self), daemon=True).start()

        
        # TODO: ChordClient returns nodeid, successor
        # succ is a tuple with ?
        self.chord.join()
        self.nodeid = self.chord.nodeid
        
        # TODO: Query successor with our nodeid to obtain the data that we now own
        self.failure_timeout = failure_timeout
            
        self.mass_raw_insert(self.chord.fingers[0].rpc.push_keys(nodeid)) # gonna need to send something to signify joining

    ''' CLIENT FACING METHODS '''

    ''' Implements insertion into the hash table with chain replication
    '''

    def insert(self, key, value):
        return perform(self.retry_until_stable(lambda node, k_hash, val: node.rpc.store(k_hash, val)))

    def delete(self, key):
        perform(self.retry_until_stable(lambda node, k_hash: node.rpc.remove(k_hash, val)))

    def lookup(self, key):
        return perform(self.retry_until_stable(lambda node, k_hash: node.rpc.map(k_hash)))


    ''' REMOTE PROCEDURES '''

    def store(self, hashed_key, value): # TODO: name
        self.raw_insert(hashed_key, value, primary=True)

        # This iterator will reflect in-place updates to succlist that are performed by stabilize.
        # For this reason, as long as we are careful to update out succlist in-place and before
        # dropping keys from previous replicas, we can be sure that previous replicas either
        # don't receive the insert, or receive it before the stabilize thread tells it to drop
        # its keys, meaning that there can be no weird card where we start this loop,
        # then the stabilize thread runs, discovers a replica not et reached by this loop that
        # to drop a key, then when resuming this loop, asks it to insert that key again.
        for replica in self.chord.succlist:
            try:
                replica.rpc.raw_insert(hashed_key, value)
            except ConnectionError: # TODO: exception here?
                pass

    def raw_insert(self, key, value):
        self.table[key] = value

    def remove(self, hashed_key):
        to_return = self.raw_delete(hashed_key)
        for replica in self.chord.succlist:
            try:
                replica.rpc.raw_delete(key)
            except (ConnectionError, KeyError):
                pass
        return to_return

    def raw_delete(self, key):
        return self.table.pop(key)

    def map(self, key):
        return self.table[key]

    def drop(self, rg):
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
        return [k, self.table[k] for k in self.table.irange(self.chord.pred+1, joiner)]

    def mass_raw_insert(self, k_v_pairs):
        for [k, v] in k_v_pairs:
            self.raw_insert(k, v)


    ''' INTERNAL UTILITIES '''

    class TryAgainError(Exception): pass

    def perform(self, fxn, key, *args):
        hashed_key = self.chord.hash(key)
        while True:
            node = self.chord.lookup(hashed_key)
            try:
                return fxn(node, hashed_key, *args)
            except self.TryAgainError:
                time.sleep(self.failure_timeout)

    def retry_until_stable(self, fxn):
        def wrapper(*args):
            if not self.chord.inrange(hashed_key, self.chord.pred, self.nodeid+1): raise self.TryAgainError
            fxn(*args)
        return wrapper

    ''' For scan we would need to either sort with the hash function or 
    store string keys in the value field of the SortedDict table
    ''' 
    
    ''' UPCALL FUNCTIONS '''

    def new_succlist_callback_fxn(self, old_succlist, new_succlist):
        old_succs = set(old_succlist)
        new_succs = set(new_sucs)
        k_v_pairs = [k, self.table[k] for k in self.table.irange(self.chord.pred+1, self.nodeid)]
        for newbie in new_succs - old_succs:
            newbie.rpc.mass_raw_insert(k_v_pairs)
        for olbie in old_succs - new_succs:
            olbie.rpc.drop(self.chord.pred+1, self.nodeid)
