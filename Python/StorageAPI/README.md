

# How to use

 1. Create the configHDFS.txt file, the first line should contain the address:port of namenode, the files to be read by HDFS must be on the next lines, one per line.
 

		cat <<EOF >> configHDFS.txt
		localhost:9000
		/JavaIntegration.txt
		EOF
 
 2. Run the COMPSs application, remember to add the classpath flag with the [StorageAPIpyhdfs.jar](./StorageHDFS/target/StorageAPIpyhdfs.jar) and add the storage_conf flag with the configHDFS.txt file.

	```bash 
	runcompss --lang=python -d \
	          --storage_conf=$PWD/configHDFS.txt \
	          --classpath=$PWD/StorageHDFS/target/StorageAPIpyhdfs.jar \
	          $PWD/grep-hdfs.py
	```

 3. Inside the application, you can use `HDFS_BLOCKS = ListBlocks.HDFS_BLOCKS` to retrieve HDFS blocks from all files or use `HDFS_BLOCKS = ListBlocks.findBlocks(<filename>)`  to retrieve only HDFS blocks of a unique file.
