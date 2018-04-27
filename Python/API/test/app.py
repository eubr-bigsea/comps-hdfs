# -*- coding: utf-8 -*-

__author__ = "Lucas Miguel S Ponce"
__email__ = "lucasmsp@gmail.com"

from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from hdfspycompss.HDFS import HDFS
from hdfspycompss.Block import Block
import time


def test01_mkdir_and_write():
    """Test: create a new folder and write splitted data."""
    numFrag = 4
    path = '/test-hdfspycompss'
    dfs = HDFS(host='localhost', port=9000)
    dfs.mkdir(path)

    suffixes = dfs.get_suffixes(numFrag)
    filename = '/test-hdfspycompss/file'

    start = time.time()
    info = [writeData(suff, filename) for suff in suffixes]
    info = compss_wait_on(info)
    print info
    end = time.time()
    print "Time to write: {}".format(end-start)


@task(returns=int)
def writeData(id_block, filename):
    """Create a DataFrame."""
    filename = '{}_{}'.format(filename, id_block)
    data = "Lorem ipsum dolor sit amet, "\
           "consectetur adipiscing elit.\nIn a mi eget libero"\
           "volutpat efficitur.\n"
    return HDFS(host='localhost', port=9000).writeBlock(filename, data)


def test02_merge():
    """Merge multiple files in a specific folder in one file."""
    src = '/test-hdfspycompss/file\*'
    dst = '/test-hdfspycompss/test02_merged'
    wait = True
    rm = False
    start = time.time()
    info = HDFS().mergeFiles(src, dst, wait, rm)
    info = compss_wait_on(info)
    print info

    end = time.time()
    print "Time to merge: {}".format(end-start)


def test03_read():
    """Test: read a file in HDFS."""
    numFrag = 3
    path = '/test-hdfspycompss/test02_merged'
    start = time.time()

    HDFS_BLOCKS = HDFS().findNBlocks(path, numFrag)
    print "[INFO] - Number of Blocks: {}".format(len(HDFS_BLOCKS))
    for b in HDFS_BLOCKS:
        print "[INFO] - Block: {}".format(b)

    data = [readData(block) for block in HDFS_BLOCKS]
    from pycompss.api.api import compss_wait_on
    data = compss_wait_on(data)
    end = time.time()

    for e, frag in enumerate(data):
        print "frag: #", e
        for line in frag:
            print (">"+line),

    print "Time to read: {}".format(end-start)


@task(returns=list)
def readData(block):
    """Read fragment."""
    return Block(block).readBlock()


if __name__ == "__main__":
    test01_mkdir_and_write()
    test02_merge()
    test03_read()
