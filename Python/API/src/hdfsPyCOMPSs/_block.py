#!/usr/bin/python
# -*- coding: utf-8 -*-

from hdfs_pycompss._cbase import *


class Block(object):


    def __init__(self, hostname, port, filename, mode='r',
                 buffer_size=0, replication=0, block_size=0):
        """
        """
        self.readline_pos = 0
        self.hostname = hostname
        self.port = port
        self.filename = filename

        if mode == 'w':
            flags = 01  # O_WRONLY
        elif mode == 'w+':
            flags = 1025  # .O_WRONLY | O_APPEND
        else:
            flags = 00  # O_RDONLY

        self.fs = APILibHdfs.hdfsConnect(hostname, port)
        self.fh = APILibHdfs.hdfsOpenFile(self.fs, filename, flags,
                                          buffer_size, replication, block_size)

    def close(self):
        APILibHdfs.hdfsCloseFile(self.fs, self.fh)
        APILibHdfs.hdfsDisconnect(self.fs)

    def stat(self):
        return APILibHdfs.hdfsGetPathInfo(self.fs, self.filename).contents

    def open(self, filename, mode='r', buffer_size=0,
             replication=0, block_size=0):
        """
        Open a hdfs file in given mode.

        :param fs: The configured filesystem handle.
        :param path: The full path to the file.
        :param flags: Currently, the supported flags are 'r' (O_RDONLY) and
        'w' (O_WRONLY) (meaning create or overwrite).
        :param bufferSize: Size of buffer for read/write - pass 0 if you want
        to use the default configured values.
        :param replication: Block replication - pass 0 if you want to use
        the default configured values.
        :param blocksize: Size of block - pass 0 if you want to use the
        default configured values.
        :return: Returns the handle to the open file or NULL on error.
        """
        if mode == 'w':
            flags = 01  # O_WRONLY
        elif mode == 'w+':  # os.O_WRONLY | os.O_APPEND
            flags = 0b10000000001
        else:
            flags = 00  # O_RDONLY

        self.fh = APILibHdfs.hdfsOpenFile(self.fs, filename, flags,
                                          buffer_size, replication,
                                          block_size)
        if not self.fh:
            raise Exception('Failed opening %s' % filename)

    def seek(self, position):
        """
        Seek to given offset in file.
        This works only for files opened in read-only mode.

        :param position   Offset into the file to seek into.
        :return           True if seek was successful, False on error.
        """
        if APILibHdfs.hdfsSeek(self.fs, self.fh, position) == 0:
            return True
        else:
            return False

    def tell(self):
        """
        Get the current offset in the file, in bytes.

        :return Returns the current offset in bytes, None on error.
        """
        ret = APILibHdfs.hdfsTell(self.fs, self.fh)
        if ret != -1:
            return ret
        else:
            return None

    def read(self):
        """
        Read data from an open file.

        :return    Returns the content read; None on error.
        """
        st = self.stat()
        buf = create_string_buffer(st.Size)
        buf_p = cast(buf, c_void_p)
        ret = APILibHdfs.hdfsRead(self.fs, self.fh, buf_p, st.Size)
        if ret == -1 or ret != st.Size:
            return None

        return buf.value[0:ret]

    def readBytes(self):
        """
        Read data from an open file.

        :return    Returns the content read as bytes; None on error.
        """
        st = self.stat()
        buf = create_string_buffer(st.Size)
        buf_p = cast(buf, c_void_p)
        ret = APILibHdfs.hdfsRead(self.fs, self.fh, buf_p, st.Size)
        if ret == -1 or ret != st.Size:
            return None

        return buf.value

    def readBinary(self, length):
        """
        Read data from an open file.

        :return    Returns the content read as bytes; None on error.
        """
        length_readed = 0
        buf = create_string_buffer(length)  # create byte buffer
        buf_p = cast(buf, c_char_p)
        result = ''
        while length_readed < length:
            ret = APILibHdfs.hdfsRead(self.fs, self.fh, buf_p, length)
            if ret == -1:
                return None
            length_readed += ret
            result += buf[0:ret]
        return result

    def pread(self, position, length):
        """
        Positional read of data from an open file.

        :param position: Position from which to read
        :param length: The length of the buffer.
        :return: Returns the number of bytes actually read, possibly less than
        than length; None on error.
        """
        st = self.stat()
        if position >= st.Size:
            return None

        buf = create_string_buffer(length)
        buf_p = cast(buf, c_void_p)
        ret = APILibHdfs.hdfsPread(self.fs, self.fh, position, buf_p, length)
        if ret == -1:
            return None

        return buf.value

    def readline(self, pos, length=1024):
        """
        Read the first line from the position passed by parameter.
        :param pos      Offset into the file to seek into,
                        -1 to use the current.
        :param length   Size of bytes to read at time;
        :return         Returns a line
        """
        if pos >= 0:
            self.readline_pos = pos

        line = ''
        while True:
            data = self.pread(self.readline_pos, length)
            if data is None:
                return line

            newline_pos = data.find('\n')
            if newline_pos == -1:  # not found
                self.readline_pos += len(data)
                line += data

            else:
                self.readline_pos += newline_pos+1
                return line + data[0:newline_pos+1]

    def write(self, buffer):
        """
        Write data into an open file.

        :param buffer   The data.
        :return         Returns False on error, otherwise is True
        """
        sb = create_string_buffer(buffer)
        buffer_p = cast(sb, c_void_p)
        ret = APILibHdfs.hdfsWrite(self.fs, self.fh, buffer_p, len(buffer))

        if ret != -1:
            return True
        else:
            return False
