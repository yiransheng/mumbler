import time
import nodeconfig

with open("/gpfs/gpfsfpo/aa", 'w') as f:
    time.sleep(25)
    f.write(nodeconfig.localnode.name)
    time.sleep(300)
