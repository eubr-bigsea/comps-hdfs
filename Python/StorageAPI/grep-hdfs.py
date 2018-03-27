#!/usr/bin/python
# -*- coding: utf-8 -*-
u"""Example of how to use the HDFS with Storage API."""
__author__ = "Lucas Miguel Simões Ponce"
__email__ = "lucasmsp@gmail.com"

import time
import re
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.functions.reduce import mergeReduce
from storage.BlockLocality import BlockLocality, ListBlocks


@task(returns=list)
def GREP(blk, word):
    """Using the Storage API, blk will be a BlockLocality object."""
    print "Block:", blk.toString()
    start = time.time()
    text = blk.readBlock()
    partialResult = 0
    for line in text:
        # line = re.split('[?!:;\s]|(?<!\d)[,.]|[,.](?!\d)', line)
        line = line.split(' ')
        for entry in line:
            if entry == word:
                partialResult += 1
    end = time.time()
    print "Inside task: it took %d seconds" % (end-start)
    return [partialResult]


if __name__ == "__main__":
    from pycompss.api.api import compss_wait_on as sync

    #HDFS_BLOCKS = ListBlocks.findBlocks('/files_256_r1')
    HDFS_BLOCKS = ListBlocks.HDFS_BLOCKS
    word = "world"
    numFrag = len(HDFS_BLOCKS)

    print """
    # ----- grep-hdfs-locality -----#
    - Word: {}
    - numFrag: {}
    # --------------------------#
    """.format(word, numFrag)
    start = time.time()

    result = [{} for f in range(numFrag)]
    for f, blk in enumerate(HDFS_BLOCKS):
        o = BlockLocality(blk)
        o.makePersistent()
        result[f] = GREP(o, word)

    result = sync(result)
    count = 0
    for r in result:
      count+=r[0]
    print "Result:", count
    end = time.time()
    print "Inside COMPSs: it took %d seconds" % (end-start)
