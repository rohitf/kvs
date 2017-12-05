from globals import *


def generateGlobalView(all_views):
    split = REPLICAS_PER_PART
    local_views = []
    GV = META.GLOBAL_VIEW

    for i in range(len(all_views)):
        if i % split == 0:  # partition is full, so create new one
            new_partition_id = len(GV) + 1
            GV[new_partition_id] = []
        
        GV[len(GV)].append(all_views[i])

    # check if last partition needs to become proxies
    last_partition = len(GV)
    if len(GV[last_partition]) < REPLICAS_PER_PART:
        temp = GV[last_partition]
        del GV[last_partition]
        GV[0] = temp

VIEWS = ["10.0.0.21:8080", "10.0.0.22:8080",
         "10.0.0.23:8080", "10.0.0.24:8080"]
REPLICAS_PER_PART = 2

generateGlobalView(VIEWS)
print(META.GLOBAL_VIEW)
