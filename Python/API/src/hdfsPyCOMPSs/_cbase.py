# -*- coding: utf-8 -*-
"""This file contains all conversions and relations between C and python.

The follow functions are the interface with the C functions.

One of the differences of this lib is that assumes the premise that
in /usr/local/lib/hdfs-pycompss/libhdfs folder there will be a jar
containing the jars classpath required for the operation.
"""

from ctypes import cdll, c_int32, c_long, c_int64, c_uint16, c_int
from ctypes import c_void_p, c_char, c_char_p, c_short, POINTER, Structure
from ctypes import create_string_buffer, cast

"""
Convertion to C types
"""

tSize = c_int32
tTime = c_long
tOffset = c_int64
tPort = c_uint16
hdfsFS = c_void_p
hdfsFile = c_void_p


"""
    hdfsFileInfo - Information about a file/directory.
"""


class tObjectKind(Structure):
    """Definition of tObjectKind."""

    _fields_ = [('kObjectKindFile', c_char),
                ('kObjectKindDirectory', c_char)]


class hdfsStats(Structure):
    """Definition of hdfsStats."""

    _fields_ = [('Kind', tObjectKind),  # file or directory
                ('Name', c_char_p),  # the name of the file
                ('LastMod', tTime),  # the last modification time in seconds
                ('Size', tOffset),          # the size of the file in bytes
                ('Replication', c_short),   # the count of replicas
                ('BlockSize', tOffset),     # the block size for the file
                ('Owner', c_char_p),        # the owner of the file
                ('Group', c_char_p),        # the group associated
                ('Permissions', c_short),   # the permissions associated
                ('LastAccess', tTime)]  # the last access time in seconds


######################################################################

try:
    APILibHdfs = cdll.LoadLibrary('/usr/local/lib/hdfs-pycompss/'
                                  'libhdfs/libhdfs.so')
except Exception:
    print "Error: libhdfs not found at /usr/local/lib/hdfs-pycompss/libhdfs/"
    raise

"""
int hdfsAvailable(hdfsFS fs, hdfsFile file):

    Number of bytes that can be read from this input stream without blocking.

    :param fs   The configured filesystem handle.
    :param file The file handle.
    :return     Returns available bytes; -1 on error.

"""

APILibHdfs.hdfsAvailable.argtypes = [hdfsFS, hdfsFile]

######################################################################

"""
int hdfsCloseFile(hdfsFS fs, hdfsFile file):

    Close an open file.
    :param fs   The configured filesystem handle.
    :param file The file handle.
    :return     Returns 0 on success, -1 on error.

"""
APILibHdfs.hdfsCloseFile.argtypes = [hdfsFS, hdfsFile]

######################################################################
"""
hdfsFS hdfsConnect(const char* host, tPort port):

    Connect to a hdfs file system.

    :param host A string containing either a host name, or an ip address
                of the namenode of a hdfs cluster. 'host' should be passed
                as NULL if you want to connect to local filesystem.
                'host' should be passed as 'default' (and port as 0) to
                used the 'configured' filesystem (core-site/core-default.xml).
    :param port The port on which the server is listening.
    :return     Returns a handle to the filesystem or NULL on error.

"""

APILibHdfs.hdfsConnect.argtypes = [c_char_p, tPort]
APILibHdfs.hdfsConnect.restype = hdfsFS

######################################################################

"""
int hdfsCreateDirectory(hdfsFS fs, const char* path):

    Make the given file and all non-existent parents into directories.

    :param fs   The configured filesystem handle.
    :param path The path of the directory.
    :return     Returns 0 on success, -1 on error.

"""
APILibHdfs.hdfsCreateDirectory.argtypes = [hdfsFS, c_char_p]

######################################################################

"""
int hdfsDelete(hdfsFS fs, const char* path):

    Delete file.

    :param fs   The configured filesystem handle.
    :param path The path of the file.
    :return     Returns 0 on success, -1 on error.

"""

APILibHdfs.hdfsDelete.argtypes = [hdfsFS, c_char_p]

######################################################################

"""
int hdfsDisconnect(hdfsFS fs):

    Disconnect from the hdfs file system.

    :param fs   The configured filesystem handle.
    :return     Returns 0 on success, -1 on error.
"""
APILibHdfs.hdfsDisconnect.argtypes = [hdfsFS]

######################################################################

"""
int hdfsExists(hdfsFS fs, const char *path):

    Checks if a given path exsits on the filesystem

    :param fs   The configured filesystem handle.
    :param path The path to look for
    :return     Returns 0 on success, -1 on error.

"""

APILibHdfs.hdfsExists.argtypes = [hdfsFS, c_char_p]

######################################################################

"""
tOffset hdfsGetCapacity(hdfsFS fs):

    Return the raw capacity of the filesystem.

    :param fs   The configured filesystem handle.
    :return     Returns the raw-capacity; -1 on error.

"""

APILibHdfs.hdfsGetCapacity.argtypes = [hdfsFS]
APILibHdfs.hdfsGetCapacity.restype = tOffset

######################################################################

"""
tOffset hdfsGetDefaultBlockSize(hdfsFS fs):

    Get the optimum blocksize.

    :param fs   The configured filesystem handle.
    :return     Returns the blocksize; -1 on error.
"""

APILibHdfs.hdfsGetDefaultBlockSize.argtypes = [hdfsFS]
APILibHdfs.hdfsGetDefaultBlockSize.restype = tOffset

######################################################################

"""
hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, const char* path):

    Get information about a path as a (dynamically allocated) single
    hdfsFileInfo struct. hdfsFreeFileInfo should be called when the
    pointer is no longer needed.

    :param fs   The configured filesystem handle.
    :param path The path of the file.
    :return     Returns a dynamically-allocated hdfsFileInfo
                object,  NULL on error.
"""
APILibHdfs.hdfsGetPathInfo.argtypes = [hdfsFS, c_char_p]
APILibHdfs.hdfsGetPathInfo.restype = POINTER(hdfsStats)

######################################################################

"""
hdfsFileInfo *hdfsListDirectory(hdfsFS fs,
                        const char* path, int *numEntries):

    Get list of files/directories for a given directory-path.
    hdfsFreeFileInfo should be called to deallocate memory.

    :param fs           The configured filesystem handle.
    :param path         The path of the directory.
    :param numEntries   Set to the number of files/directories in path.
    :return             Returns a dynamically-allocated array of
                        hdfsFileInfo objects, NULL on error.
"""

APILibHdfs.hdfsListDirectory.argtypes = [hdfsFS, c_char_p, POINTER(c_int)]
APILibHdfs.hdfsListDirectory.restype = POINTER(hdfsStats)

######################################################################

"""
hdfsFile hdfsOpenFile(hdfsFS fs, const char* path, int flags,
                      int bufferSize, short replication, tSize blocksize):

    Open a hdfs file in given mode.

    :param fs           The configured filesystem handle.
    :param path         The full path to the file.
    :param flags        Supported flags are O_RDONLY, O_WRONLY
                        (meaning create or overwrite i.e., implies O_TRUNCAT).

    :param bufferSize   Size of buffer for read/write - pass 0 if you want
                        to use the default configured values.
    :param replication  Block replication - pass 0 if you want to use
                        the default configured values.
    :param blocksize    Size of block - pass 0 if you want to use the
                        default configured values.
    :return             Returns the handle to the open file or NULL on error.


"""

APILibHdfs.hdfsOpenFile.argtypes = [hdfsFS, c_char_p, c_int,
                                    c_int, c_short, tSize]
APILibHdfs.hdfsOpenFile.restype = hdfsFile

######################################################################

"""
tSize hdfsPread(hdfsFS fs, hdfsFile file,
    tOffset position, void* buffer, tSize length):

    Positional read of data from an open file.

    :param fs       The configured filesystem handle.
    :param file     The file handle.
    :param position Position from which to read
    :param buffer   The buffer to copy read bytes into.
    :param length   The length of the buffer.
    :return         Returns the number of bytes actually read,
                    possibly less than than length;-1 on error.

"""

APILibHdfs.hdfsPread.argtypes = [hdfsFS, hdfsFile, tOffset, c_void_p, tSize]
APILibHdfs.hdfsPread.restype = tSize

######################################################################

"""
tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length):

    Read data from an open file.

    :param fs       The configured filesystem handle.
    :param file     The file handle.
    :param buffer   The buffer to copy read bytes into.
    :param length   The length of the buffer.
    :return         Returns the number of bytes actually read,
                    possibly less than than length;-1 on error.

"""
APILibHdfs.hdfsRead.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
APILibHdfs.hdfsRead.restype = tSize

######################################################################

"""

int hdfsRename(hdfsFS fs, const char* oldPath, const char* newPath):

    Rename a file.

    :param fs       The configured filesystem handle.
    :param oldPath  The path of the source file.
    :param newPath  The path of the destination file.
    :return         Returns 0 on success, -1 on error.
"""

APILibHdfs.hdfsRename.argtypes = [hdfsFS, c_char_p, c_char_p]

######################################################################

"""
int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos):

    Seek to given offset in file.
    This works only for files opened in read-only mode.

    :param fs           The configured filesystem handle.
    :param file         The file handle.
    :param desiredPos   Offset into the file to seek into.
    :return             Returns 0 on success, -1 on error.
"""

APILibHdfs.hdfsSeek.argtypes = [hdfsFS, hdfsFile, tOffset]


######################################################################

"""
tOffset hdfsTell(hdfsFS fs, hdfsFile file):

    Get the current offset in the file, in bytes.

    :param fs       The configured filesystem handle.
    :param file     The file handle.
    :return         Returns the current offset in bytes, -1 on error.
"""
APILibHdfs.hdfsTell.argtypes = [hdfsFS, hdfsFile]
APILibHdfs.hdfsTell.restype = tOffset

######################################################################

"""
tSize hdfsWrite(hdfsFS fs, hdfsFile file, const void* buffer,tSize length):

    Write data into an open file.
    :param fs       The configured filesystem handle.
    :param file     The file handle.
    :param buffer   The data.
    :param length   The no. of bytes to write.
    :return         Returns the number of bytes written, -1 on error.
"""

APILibHdfs.hdfsWrite.argtypes = [hdfsFS, hdfsFile, c_void_p, tSize]
APILibHdfs.hdfsWrite.restype = tSize

######################################################################



"""
     * hdfsGetHosts - Get hostnames where a particular block (determined by
     * pos & blocksize) of a file is stored. The last element in the array
     * is NULL. Due to replication, a single block could be present on
     * multiple hosts.
     * @param fs The configured filesystem handle.
     * @param path The path of the file.
     * @param start The start of the block.
     * @param length The length of the block.
     * @return Returns a dynamically-allocated 2-d array of blocks-hosts;
     * NULL on error.
"""
APILibHdfs.hdfsGetHosts.argtypes = [hdfsFS, c_char_p, tOffset, tOffset]
APILibHdfs.hdfsGetHosts.restype = POINTER(POINTER(c_char_p))
