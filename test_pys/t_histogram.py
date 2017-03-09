import os, os.path
from histogram import HistogramManager

# will not work due to path issues
if __name__ == '__main__' :
    working_dir = os.getcwd()
    histogram_fn = os.path.join(working_dir, 'templates', '1000_histogram')
    hm = HistogramManager(histogram_fn)
    bin_list = hm.calc_bin_idx_pos(4)
    first = bin_list[0]['pos']
    last = bin_list[1]['pos']-1
    randoms = hm.getPositions(HistogramManager.DIST_RANDOM, 10, first, last)

    first = bin_list[1]['pos']
    last = bin_list[2]['pos']-1
    dense = hm.getPositions(HistogramManager.DIST_DENSE, 8, first, last)

    first = bin_list[3]['pos']
    last = None
    sparse = hm.getPositions(HistogramManager.DIST_SPARSE, 4, first, last)

    print('DONE')