#!/usr/bin/python
# -*- coding: utf-8 -*-
"""COMPSs Storage API."""
__author__ = "Lucas Miguel Simões Ponce"
__email__ = "lucasmsp@gmail.com"
from BlockLocality import BlockLocality, Blocks


class StorageException(Exception):
    """StorageException class."""
    pass


def init(config_file_path=None):
    """It is executed at the beginning of the application."""
    import hdfs_pycompss.hdfsConnector as hdfs
    if config_file_path is None:
        ValueError("No config storage file.""")
    else:
        config_file_handler = open(config_file_path)
    filesList = [x.strip() for x in config_file_handler.readlines()]
    config_file_handler.close()

    HDFS_BLOCKS = []
    settings = {}
    head, sep, tail = filesList[0].replace("hdfs://", "").partition(':')
    for i in range(1, len(filesList)):
        settings['host'] = head  # 'default'
        settings['port'] = int(tail)  # 0
        settings['path'] = filesList[i]
        HDFS_BLOCKS += hdfs.findBlocks(settings)

    Blocks.HDFS_BLOCKS = HDFS_BLOCKS
    pass


def finish():
    """It is executed at the end of the application."""
    pass


def initWorker(config_file_path=None):
    """It is executed at the beginning of the application."""
    pass


def finishWorker():
    """It is executed at the end of the application."""
    pass


def start_task(config_file_path):
    """start_task.

    Initializes, if needed, the global vars for prefetch and batch, and
    starts the context if batch is activated
    Args:
        params: a list of objects (Blocks, StorageObjs, strings, ints, ...)
    """
    pass


def end_task(params):
    """end_task.

    Terminates, if needed, the context (to save all data
    remaining in the batch) and the prefetch. It also prints
    the statistics of the StorageObjs if desired.
    Args:
        params: a list of objects (Blocks, StorageObjs, strings, ints, ...)
    """
    pass


def getByID(objid):
    """Retrieve the object that has the given identifier from the HDFS API."""
    try:
        block = {}
        objs = objid.split(":")
        block['idBlock'] = int(objs[0])
        block['lastBlock'] = objs[1] is "True"
        block['start'] = int(objs[2])
        block['length'] = int(objs[3])
        block['host'] = objs[4]
        block['port'] = int(objs[5])
        block['path'] = objs[6]
        block['locality'] = objs[7].replace("<", "")\
                                   .replace(">", "")\
                                   .split(",")
        return BlockLocality(block)
    except Exception:
        return ValueError("No id block found!")


def makePersistent(obj, *args):
    """The obj is already persistent."""
    if obj.pycompss_psco_identifier is None:

        workers = ",".join(obj.blk['locality'])
        msg = "{}:{}:{}:{}".format(obj.blk['idBlock'], obj.blk['lastBlock'],
                                   obj.blk['start'], obj.blk['length'])
        msg += ":{}:{}:{}:<{}>".format(obj.blk['host'], obj.blk['port'],
                                       obj.blk['path'], workers)

        obj.pycompss_psco_identifier = msg
    else:
        pass


def deletePersistent(obj):
    """Nothing will be done."""
    pass


class TaskContext(object):
    """TaskContext: PyCOMPSs logger."""

    def __init__(self, logger, values, config_file_path=None):
        """Init the TaskContext."""
        self.logger = logger
        self.values = values
        self.config_file_path = config_file_path


    def __enter__(self):
        """Do something prolog."""
        #start_task(self.config_file_path)
        # Ready to start the task
        self.logger.info("Prolog BlockLocality finished")
        pass

    def __exit__(self, type, value, traceback):
        """Do something epilog."""
        # Finished
        self.logger.info("Epilog BlockLocality finished")
        pass


task_context = TaskContext
init_worker = initWorker
delete_persistent = deletePersistent
make_persistent = makePersistent
get_by_ID = getByID
finish_worker = finishWorker
