#! /usr/bin/bash

import sys
import os, os.path
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

working_dir = os.getcwd()
def __proc_template( templateFile, sub_key_val = None) :
        tf_path = templateFile.replace("$WS_HOME", working_dir)
        if os.path.isfile(tf_path) :
            if tf_path[-5:] == '.temp' :
                with open(tf_path, 'r') as fd :
                    context = fd.readlines()
                jf_path = re.sub(".temp", ".json", tf_path) 
                if sub_key_val:
                    for key, val in sub_key_val.items() :
                        context = re(key, val, context)
                with open(jf_path, 'w') as ofd:
                    ofd.write(context)
                print("--converted %s to %s" % (tf_path, jf_path) )    
                return jf_path
            else:
                print("--file %s exists" % tf_path)
                return tf_path
        else :
            print("WARN: template file %s not found" % tf_path)
            return None

if __name__ == "__main__" :
    for fname in ['$WS_HOME/templates/vid.json', '$WS_HOME/templates/template_vcf_header.vcf','/data/broad/samples/joint_variant_calling/broad_reference/Homo_sapiens_assembly19.fasta'] :
        ret =__proc_template(fname)
        print(" template=%s, ret=%s " % (fname, ret))
    ret = __proc_template('$WS_HOME/templates/callsets.temp', '{"@data_dir@" : "/scratch/1000genome" }')
    print(" template=callsets.temp {\"@data_dir@\" : \"/scratch/1000genome\" }s, ret=%s " % (ret))

    func_name = 'make_col_partition'
    print(locals())
    parts = locals()[func_name](sys.argv[1])
    pprint(parts)
