#!/usr/bin/python
# -*- coding: utf-8 -*-


def mergeFiles(settings):
    """Hadoop fs -getmerge <source path> <destination path>."""
    src = settings['source']
    dst = settings['destination']
    import subprocess
    proc = subprocess.Popen(['hadoop', 'fs', '-getmerge', src, dst],
                            stdout=subprocess.PIPE)


def findBlocks(hdfs_ops):
    """findBlocks."""

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


def readDataFrame(block, csv_options):
    """Read a fragment as a pandas's DataFrame."""
    from hdfs_pycompss._block import Block

    if not all(k in block for k in ('length', 'port', 'path',
                                    'start', 'length', 'lastBlock')):
        raise Exception('Invalid block object!')

    sep = csv_options.get('separator', ',')
    header_op = csv_options.get('header', True)
    infer = csv_options.get('infer', True)
    na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
                 '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                 'N/A', 'NA', 'NULL', 'NaN', 'nan']
    na_values = csv_options.get('na_values', na_values)

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
    # end = block['start']+block['length']

    dfs = Block(host, port, hdfs_path, mode='r')

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

        mode = csv_options.get('mode', 'FAILFAST')
        if mode == 'FAILFAST':
            # Stop processing and raise error
            error_bad_lines = True
        elif mode == 'DROPMALFORMED':
            # Ignore whole corrupted record
            error_bad_lines = False

        if infer:
            data = pd.read_csv(StringIO(data), sep=sep, na_values=na_values,
                               parse_dates=True, header=header_op,
                               error_bad_lines=error_bad_lines)

        else:
            data = pd.read_csv(StringIO(data), sep=sep, na_values=na_values,
                               header=header_op,
                               error_bad_lines=error_bad_lines, dtype='str')

        if not header_op:
            n_cols = len(data.columns)
            new_columns = ['col_{}'.format(i) for i in range(n_cols)]
            data.columns = new_columns

    except Exception as e:
        print e
        raise Exception("The file may has diferent number of columns!")

    return data


def readBlock(block):

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
        end = block['start']+block['length']

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

        data = data[:-2]  # remove the last "\n"
    except Exception as e:
        print e
        raise Exception('Error while trying to read a hdfs block')
    return StringIO(data)


def readJsonDataFrame(block, csv_options):

    from hdfs_pycompss._block import Block

    if not all(k in block for k in ('length', 'port', 'path',
                                    'start', 'length', 'lastBlock')):
        raise Exception('Invalid block object!')

    infer = csv_options.get('infer', True)
    na_values = ['', '#N/A', '#N/A N/A', '#NA', '-1.#IND',
                 '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN',
                 'N/A', 'NA', 'NULL', 'NaN', 'nan']
    na_values = csv_options.get('na_values', na_values)

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
    #end = block['start']+block['length']

    dfs = Block(host, port, hdfs_path, mode='r')

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

        if infer:
            data = pd.read_json(StringIO(data), orient='records', lines=True)
        else:
            data = pd.read_json(StringIO(data), orient='records',
                                dtype='str', lines=True)

    except Exception as e:
        print e
        raise Exception("The Json is wrong format!")

    return data


def writeBlock(settings, data, dfs, hasMore):
    """Write a fragment of file into a opened file."""
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


def writeSplittedBlock(settings, data):
    """writeSplittedBlock.

    :param settings: A dictionary that contains:
        - host: the namenode HDFS's host;
        - port: the namenode HDFS's port;
        - path: the output name;
        - mode:  Overwrite mode
            * ignore: do nothing;
            * error: raise a error;
            * overwrite;

    :para data: A list of strings.
    """
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0

    hdfs_path = settings['path']

    from hdfs_pycompss._block import Block
    from pycompss.api.api import compss_wait_on

    file_exists = ExistFile(settings)

    if file_exists and settings['mode'] == 'error':
        raise Exception('File already exists.')
    if file_exists and settings['mode'] == 'ignore':
        return True
    elif file_exists:
        # remove automatic
        pass

    dfs = Block(host, port, hdfs_path, mode='w')

    success = True
    for i in range(len(data)):
        part = data[i]
        part = compss_wait_on(part)
        success = True and dfs.write(part)

    dfs.close()

    return success


def writeSplittedDataFrame(settings, data):
    """writeSplittedDataFrame.

    :param settings: A dictionary that contains:
        - host: the namenode HDFS's host;
        - port: the namenode HDFS's port;
        - path: the output name;
        - header: True to save the header;
        - mode:  Overwrite mode
            * ignore: do nothing;
            * error: raise a error;
            * overwrite;

    :para data: A list of dataframe (ie., one splitted dataframe)
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
    from pycompss.api.api import compss_wait_on
    import pandas as pd
    import StringIO

    file_exists = ExistFile(settings)

    if file_exists and settings['mode'] == 'error':
        raise Exception('File already exists.')
    if file_exists and settings['mode'] == 'ignore':
        return True
    elif file_exists:
        # remove automatic
        pass
    dfs = Block(host, port, hdfs_path, mode='w')

    success = True
    for i in range(len(data)):
        part = data[i]
        part = compss_wait_on(part)
        s = StringIO.StringIO()
        part.to_csv(s, header=header_op, index=False, sep=',')
        success = True and dfs.write(s.getvalue())
        header_op = False

    dfs.close()

    return success


def readAllBytes(settings):
    if all(['host' in settings, 'port' in settings]):
        host = settings['host']
        port = settings['port']
    else:
        host = 'default'
        port = 0

    hdfs_path = settings['path']
    from hdfs_pycompss._block import Block
    dfs = Block(host, port, hdfs_path, mode='r')

    data = dfs.read()
    dfs.close()
    return data


def ExistFile(hdfs_ops):
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


def readBinary(settings):
    """Read all file as binary. Used for example to read shapefile"""
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
