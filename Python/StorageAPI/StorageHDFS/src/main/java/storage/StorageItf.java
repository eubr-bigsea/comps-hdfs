package storage;


//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;


import java.util.ArrayList;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StorageItf {

    // Logger According to Loggers.STORAGE
    //private static final Logger LOGGER = LogManager.getLogger("es.bsc.compss.Storage");

    /**
     * This function returns a order list of the hosts that have the higher number of local tokens.
     *
     * @param objectID the block identifier
     * @return
     * @throws storage.StorageException
     */
    public static List<String> getLocations(String objectID) throws storage.StorageException {
        /*
        True:0:880:localhost:9000:hdfs://localhost:9000/JavaIntegration.txt:<lucasmsp-S550CB>
         */

        List<String> locations = new ArrayList<String>();
        Pattern pattern = Pattern.compile("<.*>");
        Matcher pxMatcher = pattern.matcher(objectID);

        if(pxMatcher.find()) {
            String tmp = pxMatcher.group().replace("<","").replace(">","");
            String[] parts = tmp.split(",");

            for (int i = 0; i < parts.length; i++) {
                locations.add(parts[i]);
            }
        }

        return  locations;
    }

    public static void newReplica(String objectID, String node) throws StorageException {
    }

    public static String newVersion(String objectID, String node) throws StorageException {
        return objectID;
    }

    public static void consolidateVersion(String objectID) throws StorageException {
    }

    public static void delete(String objectID) throws StorageException {
    }

    public static void finish() throws StorageException {
    }

    public static Object getByID(String objectID) throws StorageException {
        return null;
    }

    public static void init(String storageConf) throws storage.StorageException {

    }


//    public static void main(String[] args) throws StorageException {
//
//        StorageItf client = new StorageItf();
//        String b = "{'length': 67108864, 'locality': ['worker-8'], " +
//                "'idBlock': i, 'lastBlock': False, 'start': 6643777536" +
//                "'host': 'default', 'path': '/files_256_r1', 'port': 0}";
//
//        getLocations(b);
//
//        System.out.println("Application ended");
//    }
}
