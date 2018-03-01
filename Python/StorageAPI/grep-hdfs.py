#!/usr/bin/python
# -*- coding: utf-8 -*-
u"""Algoritmo utilizado como experimento de dissertação de mestrado."""
__author__ = "Lucas Miguel Simões Ponce"
__email__ = "lucasmsp@gmail.com"

import time
import re
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.functions.reduce import mergeReduce
from storage.BlockLocality import BlockLocality, ListBlocks


@task(returns=list)
def find(blk, word):
    """Using the Storage API, blk will be a BlockLocality object."""
    print """Using the Storage API, blk will be a BlockLocality object."""
    print "Block:", blk.toString()
    text = blk.readBlock()
    partialResult = 0
    for line in text:
        # line = re.split('[?!:;\s]|(?<!\d)[,.]|[,.](?!\d)', line)
        line = line.split(' ')
        for entry in line:
            if entry == word:
                partialResult += 1

    return [partialResult]


if __name__ == "__main__":
    from pycompss.api.api import compss_wait_on as sync

    HDFS_BLOCKS = ListBlocks.findBlocks('/JavaIntegration.txt')

    word = "ipsum"
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
        result[f] = find(o, word)

    result = sync(result)
    print result

    end = time.time()
    print "Elapsed time: ", end-start
