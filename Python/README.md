# Integration: PyCOMPSs and HDFS

The abstraction that is provided by this version is exactly the same as that provided by the version in java. Please read the Java version before continuing.



# Configuring the API manually

After install the HADOOP in each machine, make sure you add the *HADOOP\_CONF* and the others Hadoop variables in your environment. To this, add this line at your .bashrc (change the hadoop_home if necessary): 

```bash

#Set hadoop-related enviroment variables
export HADOOP_PREFIX=/opt/hadoop
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONFDIR=$HADOOP_HOME/etc/hadoop
export HADOOP_CONF=/opt/hadoop/etc/hadoop
export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_PREFIX}/lib/native
export HADOOP_OPTS="-Djava.library.path=${HADOOP_PREFIX}/lib/native"
export HADOOP_COMMON_LIB_NATIVE_DIR=/opt/hadoop/lib/native
export HADOOP_OPTS="-Djava.library.path=/opt/hadoop/lib"

export CLASSPATH=$CLASSPATH:/usr/local/lib/hdfs-pycompss/libhdfs/hdfs-connector.jar
export LIBHDFS_OPTS=-Xmx1024m
```

After that, create a folder in `/usr/local/lib/hdfs-pycompss/` and move the folder `./API/src/hdfsPyCOMPSs/libhdfs` to there.

Then, run the command `hadoop classpath --jar hdfs-connector.jar`, it will work in any directory as long as the *$HADOOP_HOME/bin*  folder is in the *PATH* of your environment. And then, put the "hdfs-connector.jar" in `/usr/local/lib/hdfs-pycompss/libhdfs/` folder. This file is responsible to link all jars required to the classpath during execution.


## Known issues

The most common problem is the CLASSPATH is not set properly when calling a program that uses libhdfs. Make sure you set it to all the Hadoop jars needed to run Hadoop itself as well as the right configuration directory containing hdfs-site.xml. It is not valid to use wildcard syntax for specifying multiple jars. It may be useful to run `hadoop classpath --jar <path>` to generate the correct classpath for your deployment. 


