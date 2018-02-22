#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Interface to use the Integration API between HDFS and COMPSs.

* to retrieve a fragment list:
  * findBlocks
  * findNBlocks
* to read data:
  * readDataFrame
  * readBlock
  * readBinary
* to write Data:
  * writeBlockinSerial
  * writeBlock
  * writeDataFrame
  * writeFiles !
* Util tools:
  * existFile
  * mergeFiles
  * mkdir
"""
# ------------------------------------------------
# Methods to retrieve the fragment list


def findBlocks(hdfs_ops):
    """findBlocks.

    Retrieve the real blocks of a file (as HDFS used to store it).
    """
    from hdfs_pycompss._hdfs import HDFS

    try:
        list_blocks = []
        if all(['host' in hdfs_ops, 'port' in hdfs_ops]):
            host = hdfs_ops['host']
            port = hdfs_ops['port']
        else:
            host = 'default'
            port = 0

        dfs = HDFS(host, port)
        hdfs_path = hdfs_ops['path']

        stats = dfs.stat(hdfs_path)
        blockSize = stats.BlockSize
        length = stats.Size
        name = stats.Name

        remains = length
        i_start = -blockSize
        idBlock = 0

        while remains > 0:
            idBlock += 1
            i_start = i_start + blockSize
            remains -= blockSize
            if remains >= 0:
                locality = dfs.getHosts(hdfs_path, i_start, blockSize)
                block = {'host': host, 'path': name, 'start': i_start,
                         'length': blockSize, 'lastBlock': False,
                         'port': port, 'idBlock': idBlock,
                         'locality': locality}

            else:
                remains = length - i_start
                locality = dfs.getHosts(hdfs_path, i_start, remains)
                block = {'host': host, 'path': name, 'start': i_start,
                         'length': remains, 'lastBlock': True,
                         'port': port, 'idBlock': idBlock,
                         'locality': locality}
                remains = 0

            list_blocks.append(block)

    except Exception as e:
        print e
    finally:
        return list_blocks


def findNBlocks(hdfs_ops, numFrag):
    """Retrieve a list of N fragments."""
    from hdfs_pycompss._hdfs import HDFS

    list_blocks = []

    try:

        if all(['host' in hdfs_ops, 'port' in hdfs_ops]):
            host = hdfs_ops['host']
            port = hdfs_ops['port']
        else:
            host = 'default'
            port = 0

        dfs = HDFS(host, port)
        hdfs_path = hdfs_ops['path']
        stats = dfs.stat(hdfs_path)

        length = stats.Size
        name = stats.Name
        blockSize = length/numFrag

        i_start = -blockSize
        idBlock = 1

        for i in range(numFrag-1):
            i_start = i_start + blockSize
            block = {'host': host, 'path': name, 'start': i_start,
                     'length': blockSize, 'lastBlock': False,
                     'port': port, 'idBlock': idBlock}
            list_blocks.append(block)
            idBlock += 1

        i_start = i_start + blockSize
        remains = length - i_start
        block = {'host': host, 'path': name, 'start': i_start,
                 'length': remains, 'lastBlock': True,
                 'port': port, 'idBlock': idBlock}
        list_blocks.append(block)

    except Exception as e:
        print e

    finally:

        return list_blocks


# -------------------------------------------------------------
#   Methods to Read Data:
#

def readDataFrame(block, settings):
    """Read a fragment as a pandas's DataFrame."""
    from hdfs_pycompss._block import Block

    if not all(k in block for k in ('length', 'port', 'path',
                                    'start', 'length', 'lastBlock')):
        raise Exception('Invalid block object!')

    typeFile = settings.get('format', 'csv')
    sep = settings.get('separator', ',')
    header_op = settings.get('header', True)
    infer = settings.get('infer', True)
    na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
                 '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                 'N/A', 'NA', 'NULL', 'NaN', 'nan']
    na_values = settings.get('na_values', na_values)
    data = ''
    header = ''
    from StringIO import StringIO
    import pandas as pd

    if all(['host' in block, 'port' in block]):
        host = block['host']
        port = block['port']
    else:
        host = 'default'
        port = 0
    hdfs_path = block['path']
    dfs = Block(host, port, hdfs_path, mode='r')

    if typeFile == 'csv':
        # adding header
        if block['start'] > 0 and header_op:
            header = dfs.readline(0)

    # adding the phisical content (hdfs block)
    data = dfs.pread(block['start'], block['length'])
    if block['start'] > 0:
        index = data.find("\n")
        if index != -1:
            data = data[index:]

    data = header + data
    # adding the logical content (block --> split)
    if not block['lastBlock']:
        data = data + dfs.readline(block['start']+block['length'])

    dfs.close()

    try:
        if header_op:
            header_op = 'infer'
        else:
            header_op = None
        mode = settings.get('mode', 'FAILFAST')
        if mode == 'FAILFAST':
            # Stop processing and raise error
            error_bad_lines = True
        elif mode == 'DROPMALFORMED':
            # Ignore whole corrupted record
            error_bad_lines = False
        if typeFile == 'csv':
            if infer:
                data = pd.read_csv(StringIO(data), sep=sep,
                                   na_values=na_values,
                                   parse_dates=True, header=header_op,
                                   error_bad_lines=error_bad_lines)
            else:
                data = pd.read_csv(StringIO(data), sep=sep,
                                   na_values=na_values,
                                   header=header_op,
                                   error_bad_lines=error_bad_lines,
                                   dtype='str')

            if not header_op:
                n_cols = len(data.columns)
                new_columns = ['col_{}'.format(i) for i in range(n_cols)]
                data.columns = new_columns
        elif typeFile == 'JSON':
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


def readBlock(block):
    """Read the fragment as a common file. Return a StringIO file."""
    from hdfs_pycompss._block import Block
    from cStringIO import StringIO

    if not all(k in block for k in ('length', 'port', 'path',
                                    'start', 'length', 'lastBlock')):
        raise Exception('Invalid block object!')

    try:
        data = ''
        if all(['host' in block, 'port' in block]):
            host = block['host']
            port = block['port']
        else:
            host = 'default'
            port = 0
        hdfs_path = block['path']
        dfs = Block(host, port, hdfs_path, mode='r')

        # adding the phisical content (hdfs block)
        data = dfs.pread(block['start'], block['length'])
        if block['start'] > 0:
            index = data.find("\n")
            if index != -1:
                data = data[index+1:]

        # adding the logical content (block --> split)
        if not block['lastBlock']:
            data = data + dfs.readline(block['start']+block['length'])

        # k = data.rfind("\n")
        # data = data[:k]
    except Exception as e:
        print e
        raise Exception('Error while trying to read a hdfs block')
    return StringIO(data)

def readBinary(settings):
    """Read all file as binary. Used for example to read shapefile."""
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0
    hdfs_path = settings['path']

    from hdfs_pycompss._block import Block
    dfs = Block(host, port, hdfs_path, mode='r',
                buffer_size=settings['length'])

    data = dfs.readBinary(settings['length'])
    dfs.close()
    return data


# -------------------------------------------------------------
#   Methods to Write Data:
#

def writeBlockinSerial(settings, data, dfs, hasMore):
    """
    writeBlock.

    Write a fragment of file into a opened file (writing in serial).
    You must use this method in the master COMPSs node.
    """
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0
    hdfs_path = settings['path']

    from hdfs_pycompss._block import Block
    if dfs is None:
        dfs = Block(host, port, hdfs_path, mode='w')

    success = dfs.write(data)

    if not hasMore:
        dfs.close()

    return success, dfs


def writeBlock(settings, data):
    """writeBlock.

    :param settings: A dictionary that contains:
        - host: the namenode HDFS's host;
        - port: the namenode HDFS's port;
        - path: the output name;
        - mode:  Overwrite mode
            * ignore: do nothing;
            * error: raise a error;
            * overwrite;
    :param data: A string.
    """
    from hdfs_pycompss._block import Block
    hdfs_path = settings['path']
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0
    file_exists = existFile(settings)

    if file_exists and settings['mode'] == 'error':
        raise Exception('File already exists.')
    if file_exists and settings['mode'] == 'ignore':
        return True
    elif file_exists:
        # remove automatic
        pass

    dfs = Block(host, port, hdfs_path, mode='w')
    success = dfs.write(data)
    dfs.close()

    return success


def writeDataFrame(settings, data):
    """writeDataFrame.

    :param settings: A dictionary that contains:
        - host: the namenode HDFS's host;
        - port: the namenode HDFS's port;
        - path: the output name;
        - header: True to save the header;
        - mode:  Overwrite mode
            * ignore: do nothing;
            * error: raise a error;
            * overwrite;
    :param data: A dataframe.
    """
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0

    hdfs_path = settings['path']
    header_op = settings.get('header', True)

    from hdfs_pycompss._block import Block
    import pandas as pd
    import StringIO

    file_exists = existFile(settings)

    if file_exists and settings['mode'] == 'error':
        raise Exception('File already exists.')
    if file_exists and settings['mode'] == 'ignore':
        return True
    elif file_exists:
        # remove automatic
        pass
    dfs = Block(host, port, hdfs_path, mode='w')

    s = StringIO.StringIO()
    data.to_csv(s, header=header_op, index=False, sep=',')
    success = dfs.write(s.getvalue())
    dfs.close()

    return success


def writeFiles(settings):
    """Copy local files to HDFS.

        * dst: if dst i"""
    wait = settings.get('wait', False)
    src = settings['path']
    dst = settings.get('dst', '')
    if dst == "":
        dst = src

    import os
    import subprocess
    FNULL = open(os.devnull, 'w')

    if isinstance(src, list) and isinstance(dst, list):
            for source in src:
                for out in dst:
                    command = "cat {0} | hdfs dfs -put - {1}".\
                               format(source, out)
                    if wait:
                        subprocess.call(command, shell=True, stdout=FNULL,
                                        stderr=subprocess.STDOUT)
                    else:
                        subprocess.Popen(command, shell=True, stdout=FNULL,
                                         stderr=subprocess.STDOUT)
    else:
        command = "cat {0} | hdfs dfs -put - {1}".\
                   format(src, dst)
        if wait:
            subprocess.call(command, shell=True, stdout=FNULL,
                            stderr=subprocess.STDOUT)
        else:
            subprocess.Popen(command, shell=True, stdout=FNULL,
                             stderr=subprocess.STDOUT)

# ------------------------------------
#  Util tools


def existFile(hdfs_ops):
    """Check if file is in HDFS."""
    from hdfs_pycompss._hdfs import HDFS
    if all(['host' in hdfs_ops, 'port' in hdfs_ops]):
        host = hdfs_ops['host']
        port = hdfs_ops['port']
    else:
        host = 'default'
        port = 0

    dfs = HDFS(host, port)
    hdfs_path = hdfs_ops['path']

    return dfs.exists(hdfs_path)


def mkdir(hdfs_ops):
    """Create a folder in HDFS."""
    from hdfs_pycompss._hdfs import HDFS
    if all(['host' in hdfs_ops, 'port' in hdfs_ops]):
        host = hdfs_ops['host']
        port = hdfs_ops['port']
    else:
        host = 'default'
        port = 0
    hdfs_path = hdfs_ops['path']
    dfs = HDFS(host, port)
    success = dfs.mkdir(hdfs_path)
    return success


def mergeFiles(settings):
    """Merge files in HDFS.

    :param settings a python dictionary where,
        * path: mask of files to be merged;
        * dst: output name;
        * wait: True to wait the end of operation (default);
        * rm: Remove the input files after the merge.
    """
    src = settings['path']
    dst = settings['dst']
    wait = settings.get('wait', False)
    rm = settings.get('rm', True)

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
