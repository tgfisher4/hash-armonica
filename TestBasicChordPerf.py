import sys
from ChordClient import ChordClient
import time
import random
from tqdm import tqdm

cluster_name = sys.argv[1]
n_iters = sys.argv[2] if len(sys.argv) > 2 else 1000
client = ChordClient(cluster_name, verbose=False)

max_key = 2 ** client.system_bitwidth - 1
start = time.perf_counter()
for _ in tqdm(range(n_iters)):
    client.lookup(random.randint(0, max_key))
stop = time.perf_counter()

print(n_iters/(stop - start))
