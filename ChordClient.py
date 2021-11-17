import HashArmonicaNetworkUtils as utils
import RPCClient
import hashlib
import NDCatalog

# Might be worth taking notes from
# https://cit.dixie.edu/cs/3410/asst_chord.html

class FingerTableEntry:
    def __init__(self, name, timeout):
        self.nodeid
        self.start # cache to prevent unnecessary "pows"
        self.rpc = RPCClient.RPCClient(name)

class ChordClient:
    def __init__
        self.system_bitwidth
        self.node_id
        self.fingers
        self.pred
        self.start
        self.succlist
        self.cluster_name
        self.listen
        self.catalog = NDCatalog.NDCatalog() # save to instance variable so it doesn't go out of scope and get garbage collected
        self.catalog.register('chord', self.cluster_name + self.nodeid, 'tfisher4')

    def join
        # Find an existing node in cluster.
        # Make a random choice to hopefully distribute the load of bootstrapping joining nodes.
        try:
            liaison = RPCClient(lambda servers: random.choice([node for node in self.catalog.query_all() if node.project == self.cluster_name]))
            self.fingers[0] = FingerTableEntry(liaison.lookup(self.nodeid + 1)) # fix successor
            # NOTE: pred stays None: perioidic stabilizes will fix this later
        except: # node is first member of chord cluster
            # NOTE: FingerTableEntry creates a socket: is it fine to have the same process at both ends of a socket?
            # https://stackoverflow.com/a/8118180 suggests YES, fine
            # Not sure we get any actual benefit from this besides uniform handling of degenerate case
            self.fingers[0] = FingerTableEntry(self.nodeid)

        
        # in concurrent join (general) case, don't construct finger table: allow it to be fixed
        #pvs_fgr = -1
        #for fgr_idx in range(system.bidwidth): # for each finger, if pvs finger not next, lookup finger
        #    fgr_key = self.node_id + 2 ** fgr_idx
        #    if pvs_fgr < fgr_key:
        #        pvs_fgr = FingerTableEntry(liaison.lookup(fgr_key))
        #    self.fingers[fgr_idx] = pvs_fgr


    def lookup
        node = self
        nodeid = self.nodeid
        while key not in range
            nodeid = node.closest_preceding_finger(key)
            node = RPCClient(...)
        return nodeid

    def closest_preceding_finger
        for i in range(self.bidwidth-1, -1, -1):
            if key > fingers[i].start:
                return fingers[i]
        return self # otherwise its behind you: also, do we need to do some modular arithmetic - ex: you are at 6, keyspace is 0 - 7. your 1st cxn is 7, but second is 1. So if you want to look up 0, need to look > 7 (-1), < 1

    def leave
        # any implementation at the chord client level? maybe flip on "leaving" switch?

    def fix_finger
        fgr_idx = random.choice([i for i, fgr in enumerate(self.fingers)])
        corr_fgr = self.lookup(self.fingers[i].start)
        if corr_fgr != self.fingers[fgr_idx].nodeid:
            self.fingers[fgr_idx] = FingerTableEntry(...)
            # correct as many fingers as able at the current time
            while still succ:
                self.fingers[fgr_idx + i] = self.fingers[fgr_idx] # share socket/finger table entries between fingers?

    def stabilize
        # TODO: handle failure of successor
        # specifically with RPC client, how do we expect errors to propogate
        try:
            succ_pred = self.fingers[0].rpc.predecessor()
        except ConnectionError:
            del(self.succclients[self.succlist[0]])
            self.succlist = self.succlist[1:]
            self.fingers[0] = self.succlist[0] # also use succclients to populate FingerTableEntry
            return stabilize() # retry stabilization
        if nodeid < succ_pred and < succ_pred < self.fingers[0].nodeid: # maybe cache successor's nodeid? can't change unless we change whole succ
            self.fingers[0] = FingerTableEntry(succ_pred)
            self.succ

        try:
            self.fingers[0].rpc.suspected_predecessor(self.nodeid)
        except ConnectionError:
            # could not contact successor
            del(self.succclients[self.succlist[0]])
            self.succlist = self.succlist[1:]
            self.fingers[0] = self.succlist[0] # also use succclients to populate FingerTableEntry
            return stabilize() # retry stabilization


        # TODO: also maintain succlist
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

    # Handled communication

    # Big Q: how to balance time spent listening vs computing
    # Another big Q: do we need to ensure only one stablize/fix_finger check in-flight at a time?
    #   - certainly wasteful to have multiple going, but each should return the same result
    #   - if returning different results, next iteration will revert back to correct, guaranteed
    #   - eventual consistency?
    def listen
        # open listening socket
        # for readable sockets
        #   

    def suspected_predecessor(self, nodeid)
        if self.pred is None or self.pred < src and src < self.nodeid:
            self.pred = src

    def predecessor(self): return predecessor
