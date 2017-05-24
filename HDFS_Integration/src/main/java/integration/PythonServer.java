package integration;

import py4j.GatewayServer;

/**
 * Created by lucasmsp on 08/04/17.
 */
public class PythonServer {

    public static void main(String[] args) {

        String defaultFS = System.getenv("MASTER_HADOOP_URL"); // local of the HDFS's master node
        GatewayServer gatewayServer;

        if (args[1].equals("hdfs")){
            HDFS dfs = new HDFS(defaultFS);
            gatewayServer = new GatewayServer(dfs);
        }else {
            int port  = Integer.parseInt(args[2]);
            Block blk = new Block();
            gatewayServer = new GatewayServer(blk,port);
        }
        gatewayServer.start();
        System.out.println("PythonHDFS Server Started");
    }

}
