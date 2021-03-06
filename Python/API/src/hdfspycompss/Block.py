#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Interface to use the Integration API between HDFS and COMPSs."""

import hdfs3
import pandas as pd
from StringIO import StringIO


class Block(object):
    blk = None
    dfs = None
    opened = False

    def __init__(self, block):
        """Open the BlockObject."""
        if isinstance(block, list):
            if len(block) > 1:
                raise Exception('Please inform only one block.')
            else:
                block = block[0]
        if not all(k in block for k in ('length', 'port', 'path',
                                        'start', 'length')):
            raise Exception('Invalid block object!')
        self.blk = block
        try:
            if all(['host' in block, 'port' in block]):
                host = block['host']
                port = block['port']
            else:
                host = 'localhost'
                port = 9000

            self.dfs = hdfs3.HDFileSystem(host, port=port)
        except Exception as e:
            print e

    def __del__(self):
        del self.blk
        self.dfs.disconnect()

    def readDataFrame(self, settings):
        """Read a fragment as a pandas's DataFrame."""

        format_file = settings.get('format', 'csv')
        separator = settings.get('separator', ',')
        header_op = settings.get('header', True)
        infer = settings.get('infer', True)
        na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
                     '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                     'N/A', 'NA', 'NULL', 'NaN', 'nan']
        na_values = settings.get('na_values', na_values)
        data = ""
        header = ""
        filename = self.blk['path']
        with self.dfs.open(filename) as f:
            if format_file == 'csv':
                # adding header
                if self.blk['start'] > 0 and header_op:
                    header = f.readline(chunksize=2048, lineterminator='\n')

            # adding the phisical content (hdfs block)
            f.seek(self.blk['start'], from_what=0)
            data = f.read(length=self.blk['length'])

            if self.blk['start'] > 0:
                index = data.find("\n")
                if index != -1:
                    data = data[index+1:]

            data = header + data
            # adding the logical content (block --> split)
            if not self.blk['lastBlock']:
                delimiter = False
                line = ""
                while not delimiter:
                    tmp = f.read(length=2048)
                    index = tmp.find("\n")
                    if index > 0:
                        tmp = tmp[:index]
                        delimiter = True
                    line += tmp
                data += line

        # print "[INFO] data-reader: download completed"

        try:

            if format_file == 'csv':
                if header_op:
                    header_op = 'infer'
                else:
                    header_op = None

                mode = settings.get('mode', 'FAILFAST')
                if mode == 'FAILFAST':
                    # Stop processing and raise error
                    mode_err = True
                else:
                    #   elif mode == 'DROPMALFORMED':
                    # Ignore whole corrupted record
                    mode_err = False

                if infer:
                    data = pd.read_csv(StringIO(data), sep=separator,
                                       na_values=na_values,
                                       header=header_op,
                                       error_bad_lines=mode_err)
                else:
                    data = pd.read_csv(StringIO(data), sep=separator,
                                       na_values=na_values,
                                       header=header_op,
                                       error_bad_lines=mode_err,
                                       dtype='str')

                if not header_op:
                    n_cols = len(data.columns)
                    new_columns = ['col_{}'.format(i) for i in range(n_cols)]
                    data.columns = new_columns

            elif format_file == 'json':
                if infer:
                    data = pd.read_json(StringIO(data), orient='records',
                                        lines=True)
                else:
                    data = pd.read_json(StringIO(data), orient='records',
                                        dtype='str', lines=True)

        except Exception as e:
            print e
            raise Exception("The file may has diferent number of columns!")

        return data

    def readBlock(self):
        """Read the fragment as a common file. Return a StringIO file."""
        from cStringIO import StringIO
        try:
            filename = self.blk['path']
            data = self.dfs.read_block(filename, self.blk['start'],
                                       self.blk['length'], delimiter=b'\n')
        except Exception as e:
            print e
        #    raise Exception('Error while trying to read a hdfs block')
        return StringIO(data)

    def readBinary(self, nbytes=-1):
        """Read all file as binary, for instance, to read shapefile."""

        with self.dfs.open(self.blk['path']) as f:
            if nbytes == -1:
                nbytes = f.info()['size']
            data = f.read(nbytes)

        return data
