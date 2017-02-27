#! /usr/bin/python3
import sys
import result_handler as rh

if len(sys.argv) > 1 :
    runid = sys.argv[1] 
    resultData = rh.TimeResultHandler()
    csv_file = resultData.export2csv(int(runid))

    resultData.close()
    print("generated csv file %s" % csv_file)
else:
    print("Usage: python generate_csv.py runid")

 