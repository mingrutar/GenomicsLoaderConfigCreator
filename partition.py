#! /usr/bin/bash

import sys
import re
from pprint import pprint
from functools import reduce

histogram_fn = '1000_histogram'
TILE_WORKSPACE = "/mnt/app_hdd1/scratch/mingperf/tiledb-ws/"

def make_col_partition(bin_num):
    bin_num = int(bin_num)
    with open(histogram_fn, 'r') as rfd:
        context = rfd.readlines()
    lines = [ l.split(',') for l in context ]
    hgram = [ (x[0], x[1], float(x[2].rstrip()) ) for x in lines if len(x) == 3 ]
    bin_size = sum( [ x[2] for x in hgram] ) / bin_num
    partitions = []       
    subtotal = 0
    parnum = 0
    begin = 0
    for item in hgram:
        if subtotal == 0 :
            begin = item[0]
        subtotal += item[2]
        if (parnum < bin_num-1) and (subtotal > bin_size) :
            partitions.append({"array" :"TEST%d" % parnum,
                "begin" : begin, "workspace" : TILE_WORKSPACE })
            parnum += 1
            subtotal = 0
    if (subtotal > 0) :
        partitions.append({"array" :"TEST%d" % parnum,
            "begin" : begin, "workspace" : TILE_WORKSPACE })
    return partitions

if __name__ == "__main__" :
    func_name = 'make_col_partition'
    print(locals())
    parts = locals()[func_name](sys.argv[1])
    pprint(parts)
