#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Interface to use the Integration API between HDFS and COMPSs."""
import hdfs3


class HDFS(object):
    """HDFS methods.

    * to retrieve a fragment list:
      * findBlocks
      * findNBlocks
    * to read data:
      * readDataFrame
      * readBlock
      * readBinary
    * to write Data:
      * writeBlock
      * writeDataFrame
      * writeJson
    * Util tools:
      * copyFilesToHDFS
      * exist
      * mergeFiles
      * mkdir
    """

    dfs = None
    host = None
    port = None

    def __init__(self, host='localhost', port=9000):
        """Start connection with HDFS."""
        self.dfs = hdfs3.HDFileSystem(host=host, port=port)
        self.port = port
        self.host = host

    def __del__(self):
        self.dfs.disconnect()

    # ------------------------------------------------
    # Methods to retrieve the fragment list
    def findBlocks(self, filename):
        """Get the HDFS blocks's info of a file."""
        try:
            list_blocks = []
            stats = self.dfs.get_block_locations(filename, start=0, length=0)
            idBlock = 0
            last = False
            for blk in stats:
                idBlock += 1
                if idBlock == len(stats):
                    last = True
                block = {'idBlock': idBlock, 'host': self.host,
                         'port': self.port, 'path': filename,
                         'length': blk['length'], 'start': blk['offset'],
                         'locality': blk['hosts'], 'lastBlock': last}

                list_blocks.append(block)

        except Exception as e:
            print e
        finally:
            return list_blocks

    def findNBlocks(self, filename, numFrag):
        """Get a list of N fragments of a file."""
        list_blocks = []
        try:
            stats = self.dfs.get_block_locations(filename, start=0, length=0)
            length = sum([b['length'] for b in stats])
            blockSize = length/numFrag
            idBlock = 1
            istart = 0
            for i in range(numFrag-1):
                block = {'idBlock': idBlock, 'host': self.host,
                         'port': self.port, 'path': filename,
                         'length': blockSize, 'start': istart,
                         'lastBlock': False}
                istart = istart + blockSize
                list_blocks.append(block)
                idBlock += 1

            blockSize = length - istart
            block = {'idBlock': idBlock, 'host': self.host, 'port': self.port,
                     'path': filename, 'length': blockSize, 'start': istart,
                     'lastBlock': True}
            list_blocks.append(block)

        except Exception as e:
            print e

        finally:
            return list_blocks

    # -------------------------------------------------------------
    #   Methods to Write Data:
    #
    def writeBlock(self, filename, data, append=False, overwrite=True):
        """
        writeBlock.

        Write a fragment of file into a opened file (writing in serial).
        You must use this method in the master COMPSs node.
        """
        if append:
            mode = 'ab'
        else:
            mode = 'wb'
            if not overwrite and self.dfs.exists(filename):
                raise Exception('File {} already exists.'.format(filename))

        with self.dfs.open(filename, mode) as f:
            f.write(data)
            f.close()

    def writeDataFrame(self, filename, data, header=True, append=False,
                       overwrite=False):
        """writeDataFrame.

        :param filename:
        :param data:
        :param header:  True to save the header;
        :param append: True to append;
        :param overwrite: True to overwrite if exists;
        :return: True if it was save with success.
        """
        import StringIO

        if append:
            mode = 'ab'
        else:
            mode = 'wb'
            if not overwrite and self.dfs.exists(filename):
                raise Exception('File {} already exists.'.format(filename))

        with self.dfs.open(filename, mode) as f:
            s = StringIO.StringIO()
            data.to_csv(s, header=header, index=False, sep=',')
            f.write(s.getvalue())
            f.close()

        return True

    def writeJson(self, filename, data, append=False, overwrite=False):
        """writeJson.

        :param settings: A dictionary that contains:
            - path: the output name;
            - header: True to save the header;
            - mode:  Overwrite mode
                * ignore: do nothing;
                * error: raise a error;
                * overwrite;
        :param data: A dataframe.
        """
        import StringIO

        if append:
            mode = 'ab'
        else:
            mode = 'wb'
            if not overwrite and self.dfs.exists(filename):
                raise Exception('File {} already exists.'.format(filename))

        with self.dfs.open(filename, mode) as f:
            s = StringIO.StringIO()
            data.to_json(s, orient='records')
            f.write(s.getvalue())
            f.close()

    def copyFilesToHDFS(self, src, dst=None):
        """Copy local files to HDFS."""
        import os
        if not dst or dst == '':
            dst = src
            if isinstance(dst, list):
                for i, out in enumerate(dst):
                    dst[i] = "/"+os.path.basename(out)
            else:
                dst = "/"+os.path.basename(dst)

        if isinstance(src, list) and isinstance(dst, list):
            for source in src:
                for out in dst:
                    self.dfs.put(source, out)
        else:
            self.dfs.put(src, dst)

    # ------------------------------------
    #  Util tools
    def exist(self, path):
        """Check if file or dir is in HDFS."""
        return self.dfs.exists(path)

    def mkdir(self, path):
        """Create a folder in HDFS."""
        self.dfs.mkdir(path)

    def ls(self, path):
        """List files at path in HDFS."""
        return self.dfs.ls(path)

    def rm(self, path, recursive=False):
        """Use recursive for rm -r, i.e., delete directory and contents."""
        self.dfs.rm(path, recursive)

    def mergeFiles(self, src, dst, wait=True, rm=True):
        """Merge files in HDFS.

        * src: mask of files to be merged;
        * dst: output name;
        * wait: True to wait the end of operation (default);
        * rm: Remove the input files after the merge.
        """
        import os
        import subprocess
        FNULL = open(os.devnull, 'w')

        if rm:
            command = "hdfs dfs -text {} | hdfs dfs -put - {} && "\
                      "hdfs dfs -rm -r {}".format(src, dst, src)
        else:
            command = "hdfs dfs -text {0} | hdfs dfs -put - {1}".\
                      format(src, dst)
        if wait:
            code = subprocess.call(command, shell=True, stdout=FNULL,
                                   stderr=subprocess.STDOUT)
        else:
            code = subprocess.Popen(command, shell=True, stdout=FNULL,
                                    stderr=subprocess.STDOUT)

        return (code == 0)
