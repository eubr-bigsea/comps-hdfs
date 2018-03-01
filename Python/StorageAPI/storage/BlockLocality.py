#!/usr/bin/python
# -*- coding: utf-8 -*-
"""BlockLocality.py: Abstraction to use the HDFS data locality."""
__author__ = "Lucas Miguel Simões Ponce"
__email__ = "lucasmsp@gmail.com"

import api

class Blocks(object):
    HDFS_BLOCKS = None

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

    def getInfo(self):
        """Only for test purposes."""
        print self.blk
