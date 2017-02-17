#! /usr/bin/python

def __str2num(x) :
    try:
        return int(x)
    except ValueError:
        try:
            return float(x)
        except ValueError:
            return None
def make_col_partition(bin_num):
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

transformer = {'String' : lambda x : x if isinstance(x, str) else None,
        'Number' : __str2num ,
        'Boolean' : lambda x: x.lower() == 'true' ,
        'Template' : lambda x: my_templates[x] , 
        'MB' : lambda x: int(x) * one_MB }

def __getValue(itemType, itemVal) :
    ''' String, Number, Boolean, Template, MB, func() '''
    if itemType in transformer:
        return transformer[itemType](itemVal)
    elif itemType[-2:] == '()':
        return locals()[itemType](itemVal)
    else:
        return None

ret = __getValue("Number", '100')
print("Number=100: %s" % ret)
ret = __getValue("Boolean", 'true')
print("Boolean=true: %s" % ret)
ret = __getValue("Boolean", 'false')
print("Boolean=false %s" % ret)
ret = __getValue("Number", '2.41')
print("Number=2.41: %s" % ret)
ret = __getValue("MB", '10')
print("MB=10: %s" % ret)
ret = __getValue(T)

        