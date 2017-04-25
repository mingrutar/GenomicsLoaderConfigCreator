import os, os.path

fn = 'query_gen_msg.txt'
if os.path.isfile(fn):
#    print(fn)
    tags = set()
    otags = set()
    rfd = open(fn, 'r')
    for  line in rfd:
#        print(line)
        tokens = line.split(',')
        if tokens[0] == "GENOMICSDB_TIMER":
            tags.add(tokens[1])
            for ii in range(2, len(tokens), 2):
                otags.add(tokens[ii])
    rfd.close()
    print(tags)
    print("-- other --")
    print(otags)
else:
    print("no found %s" %fn)

#    {'GenomicsDB cell fill timer', 'bcf_t serialization', 'Operator time', 'Sweep at query begin position', 'TileDB iterator', 'Total scan_and_produce_Broad_GVCF time for rank 0', 'TileDB to buffer cell', 'bcf_t creation time'}