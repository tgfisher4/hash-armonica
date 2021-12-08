from HashArmonica import HashArmonica
from time import perf_counter as now, sleep
import sys
import json

config = json.loads(open(sys.argv[1]).read())

ha = HashArmonica(**config)
sleep(10)
print(ha)

# number of nodes fixed

print("starting evaluation")

num = 1000
start = now()
for i in range(num):
    ha.insert(str(i), str(i))
stop = now()
print(f"insertion latency: {(stop-start)/num}")

# no key errors currently
num = 1000
start = now()
for i in range(num):
    try: ha.lookup(str(i))
    except KeyError: pass
stop = now()
print(f"lookup latency: {(stop-start)/num}")

num = 1000
start = now()
for i in range(num):
    ha.delete(str(i))
stop = now()
print(f"deletion latency: {(stop-start)/num}")

