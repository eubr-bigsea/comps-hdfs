#!/usr/bin/python
# -*- coding: utf-8 -*-
"""BlockLocality.py: Abstraction to use the HDFS data locality."""
__author__ = "Lucas Miguel Simões Ponce"
__email__ = "lucasmsp@gmail.com"

import api


class ListBlocks(object):
    HDFS_BLOCKS = None

    @staticmethod
    def findBlocks(name):
        """Get blocks by name."""
        tmp = []
        for block in ListBlocks.HDFS_BLOCKS:
            if block['path'] == name:
                tmp.append(block)
        return tmp


class StorageObject(object):
    """Storage Object Interface."""

    def __init__(self):
        """Id will be None until persisted."""
        self.pycompss_psco_identifier = None

    def makePersistent(self, identifier=None):
        """Simulate a store object in the HDFS."""
        api.makePersistent(self, identifier)

    def make_persistent(self, identifier=None):
        """Support for underscore notation."""
        self.makePersistent(identifier)

    def deletePersistent(self):
        """Do nothing."""
        api.deletePersistent(self)

    def delete_persistent(self):
        """Do nothing."""
        self.deletePersistent()

    def getID(self):
        """Get the ID of the object."""
        return self.pycompss_psco_identifier


class BlockLocality(StorageObject):
    """BlockLocality Class: HDFS Block Locality implementation."""

    blk = None

    def __init__(self, blk):
        """Initialize the BlockLocality object."""
        super(BlockLocality, self).__init__()
        self.blk = blk

    def get_content(self):
        """Get the dictionary."""
        return self.blk

    def set_content(self, content):
        """Change the block."""
        self.blk = content

    def toString(self):
        """Only for test purposes."""
        return self.blk

    def readBlock(self):
        """Read Block from HDFS as a common file."""
        import hdfs_pycompss.hdfsConnector as hdfs
        return hdfs.readBlock(self.blk)

    def readBinary(self):
        """Read a Binary Block from HDFS."""
        import hdfs_pycompss.hdfsConnector as hdfs
        return hdfs.readBinary(self.blk)

    def readDataFrame(self, settings_df):
        """Read a DataFrame from HDFS."""
        import hdfs_pycompss.hdfsConnector as hdfs
        return hdfs.readDataFrame(self.blk, settings_df)
