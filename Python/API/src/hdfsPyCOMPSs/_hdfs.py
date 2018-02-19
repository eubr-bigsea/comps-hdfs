# -*- coding: utf-8 -*-

from hdfs_pycompss._cbase import *

class HDFS(object):

    def __init__(self, hostname='default', port=0):
        self.hostname = hostname
        self.port = port
        self.fs = APILibHdfs.hdfsConnect(hostname, port)

    def __del__(self):
        if self.fs:
            self.disconnect()

    def capacity(self):
        """
        Return the raw capacity of the filesystem.

        :param fs   The configured filesystem handle.
        :return     Returns the raw-capacity; None on error.
        """

        cap = APILibHdfs.hdfsGetCapacity(self.fs)
        if cap != -1:
          return cap
        else:
          return None


    def connect(self, hostname, port):
        """
        Connect to a hdfs file system.

        :param host     A string containing either a host name, or an ip address
                        of the namenode of a hdfs cluster. 'host' should be passed
                        as NULL if you want to connect to local filesystem.
                        'host' should be passed as 'default' (and port as 0) to
                        used the 'configured' filesystem (core-site.xml).
        :param port     The port on which the server is listening.
        :return         Returns True in success, otherwise is False.
        """

        self.fs = APILibHdfs.hdfsConnect(hostname, port)
        if not self.fs:
            return False
        else:
            return True


    def delete(self, path):
        """
        :param fs   The configured filesystem handle.
        :param path The path of the file.
        :return     Returns True on success, False on error.
        """
        if APILibHdfs.hdfsDelete(self.fs, path) == 0:
          return True
        else:
          return False

    def disconnect(self):
        if APILibHdfs.hdfsDisconnect(self.fs) == -1:
            return False


    def exists(self, path):
        """
        @param fs The configured filesystem handle.
        @param path The path to look for
        @return Returns True on success, False on error.
        """
        if APILibHdfs.hdfsExists(self.fs, path) == 0:
          return True
        else:
          return False


    def listdir(self, path):
        """
        Get list of files/directories for a given directory-path.
        hdfsFreeFileInfo should be called to deallocate memory.

        :param path The path of the directory.
        :return     Returns a dynamically-allocated array of hdfsFileInfo objects;
                    None on error.
        """
        if not self.exists(path):
          return None

        path = c_char_p(path)
        num_entries = c_int()
        entries = []
        entries_p = APILibHdfs.hdfsListDirectory(self.fs, path, pointer(num_entries))
        [entries.append(entries_p[i].mName) for i in range(num_entries.value)]
        return sorted(entries)

    def mkdir(self, path):
        """
        Make the given file and all non-existent parents into directories.

        :param fs       The configured filesystem handle.
        :param path     The path of the directory.
        :return         Returns True on success, False on error.
        """
        if APILibHdfs.hdfsCreateDirectory(self.fs, path) == 0:
          return True
        else:
          return False

    def rename(self, oldPath, newPath):
        """
        Rename a file.

        :param fs       The configured filesystem handle.
        :param oldPath  The path of the source file.
        :param newPath  The path of the destination file.
        :return         Returns True on success, False on error.
        """
        if APILibHdfs.hdfsRename(self.fs, oldPath, newPath) == 0:
          return True
        else:
          return False

    def stat(self, path):
        """
        Get file status.

        :param path The path of the file.
        :return Returns a hdfsFileInfo structure.
        """

        return APILibHdfs.hdfsGetPathInfo(self.fs, path).contents

    def getHosts(self, path, begin, offset):
        """
            Used to retrieve the hosts of a particular file
        """
        r = APILibHdfs.hdfsGetHosts(self.fs, path, begin, offset)
        i = 0
        ret = []
        while r[0][i]:
            ret.append(r[0][i])
            i += 1
        # if r:
        #     libhdfs.hdfsFreeHosts(r)
        return ret
