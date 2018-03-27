# Integration: PyCOMPSs and HDFS

The abstraction that is provided by this version is exactly the same as that provided by the version in java. Please read the Java version before continuing.


# How to install the HDFSPyCOMPSs module

In order to install, you can add the `hdfspycompss` folder in your own project folder or you could move it to a Python's libraries folder (such as: `/usr/lib/python2.7/`)

If you have a installed Hadoop in your machine, you dont need any other configuration. But if you will use your application on a machine that does not belong to a HDFS cluster, you have two options: 

 1. To the first option, you only need a variable that points to the `hdfs-site.xml` of the HDFS cluster that you want to connect to. In order to do this, you can copy the `hdfs-site.xml` file to where your application will run (and put, for example, in the same folder as your project) and add this line at your .bashrc:

```bash
export LIBHDFS3_CONF=/path/hdfs-site.xml
```

2. The second option, dont need to creave any other environment, you can simply inform the namenode address and port at runtime.


# Dependencies

 - [Libhdfs3](https://github.com/ContinuumIO/libhdfs3-downstream/tree/master/libhdfs3):
    There are two ways to install libhdfs3 that works, the first one is using conda `conda install hdfs3 -c conda-forge` the other is building from source (instructions in the link above).

    While using conda may not be a viable option for many users and compiling from source can be very tricky, I've created an installer with the library already precompiled.

    ```bash
    $ sudo apt-get install cmake libxml2 libxml2-dev uuid-dev libgsasl7-dev libkrb5-dev
    $ sudo dpkg -i libhdfs3.deb
    $ sudo ldconfig
    ```

 - [Hdfs3](https://hdfs3.readthedocs.io/en/latest/index.html): To install it, use the command `pip install hdfs3`, or if you use conda, the previous command `conda install hdfs3 -c conda-forge` also download hdfs3.


# Example of how to use the API (without StorageAPI)

```python
def wordcount(blk, word):
    from hdfspycompss.Block import Block
    data = Block(blk).readBlock()
    ...

def main():
    import hdfspycompss.HDFS import HDFS
    dfs = HDFS(host='localhost', port=9000)
    # or: dfs = HDFS()
    HDFS_BLOCKS = dfs.findBlocks('/input.data')

    nFrag = len(HDFS_BLOCKS)
    result = [{} for f in range(nFrag)]
    for f, blk in enumerate(HDFS_BLOCKS):
        result[f] = wordcount(blk, 'word')
    ...
```





