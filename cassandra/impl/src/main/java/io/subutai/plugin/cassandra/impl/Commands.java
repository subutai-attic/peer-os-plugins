package io.subutai.plugin.cassandra.impl;


public class Commands
{
    public static final String STATUS_COMMAND = "service cassandra status";
    public static final String START_COMMAND = "service cassandra start";
    public static final String STOP_COMMAND = "service cassandra stop";
    public static final String RESTART_COMMAND = "service cassandra restart";
    public static final String REMOVE_FOLDER = "rm -rf /var/lib/cassandra/";
    public static final String CREATE_FOLDER = "mkdir /var/lib/cassandra/";
    public static final String CHOWN = "chown cassandra:cassandra /var/lib/cassandra/";

    public static final String SCRIPT = "bash /etc/cassandra/cassandra-conf.sh %s";
    public static final String PERMISSION_PARAM = "chmod +x /etc/cassandra/cassandra-conf.sh";
    public static final String CLUSTER_NAME_PARAM = "cluster_name Test Cluster";
    public static final String DATA_DIR_DIR = "data_dir /var/lib/cassandra/data";
    public static final String DATA_FILE_DIRECTORIES_PROPERTY = "data_file_directories";
    public static final String COMMITLOG_DIRECTORY_PROPERTY = "commitlog_directory";
    public static final String SAVED_CACHES_DIRECTORY_PROPERTY = "saved_caches_directory";
    public static final String COMMIT_LOG_DIR = "commitlog_dir /var/lib/cassandra/commitlog";
    public static final String SAVED_CHACHE_DIR = "saved_cache_dir /var/lib/cassandra/saved_caches";


    private final static String SEEDS_COMMAND =
            "sed -i \"s/- seeds:.*/- seeds: \\\"%s\\\"/g\" /etc/cassandra/cassandra.yaml";

    private final static String RPC_ADDRESS_COMMAND =
            "sed -i \"s/rpc_address:.*/rpc_address: %s/g\" /etc/cassandra/cassandra.yaml";

    private final static String LISTEN_ADDRESS_COMMAND =
            "sed -i \"s/listen_address:.*/listen_address: %s/g\" /etc/cassandra/cassandra.yaml";

    private final static String CLUSTER_NAME_COMMAND =
            "sed -i \"s/cluster_name:.*/cluster_name: '%s'/g\" /etc/cassandra/cassandra.yaml";

    private final static String ENDPOINT_SNITCH_COMMAND =
            "sed -i \"s/endpoint_snitch:.*/endpoint_snitch: GossipingPropertyFileSnitch/g\" /etc/cassandra/cassandra"
                    + ".yaml";

    private final static String AUTO_BOOTSTRAP_COMMAND =
            "echo 'auto_bootstrap: false' >> /etc/cassandra/cassandra.yaml";


    //    private final static String REPLACE_PROPERTY_COMMAND = "sed -i \"s|%1$s:.*|%1$s: %2$s|g\"
    // /etc/cassandra/cassandra.yaml";


    //    private final static String RESTART_COMMAND = "service cassandra restart";

    //    private final static String STOP_COMMAND = "service cassandra stop";

    //    private static final String REMOVE_FOLDER = "rm -rf /var/lib/cassandra/";
    //
    //    private static final String CREATE_FOLDER = "mkdir /var/lib/cassandra/";
    //
    //    private static final String CHOWN = "chown cassandra:cassandra /var/lib/cassandra/";

    private static final String NODETOOL_STATUS = "nodetool status";

    private static final String HS_ERRORS = "cat /var/lib/cassandra/hs_err*.log";

    private static final String AVAILABLE_RAM =
            "cat /sys/fs/cgroup/memory$(cat /proc/self/cgroup | grep memory | cut " + "-d: -f3)/memory.limit_in_bytes";

    private static final String HEAP_SIZE = "echo \"-Xms%dM\n-Xmx%dM\" >> /etc/cassandra/jvm.options";


    static String getReplacePropertyCommand( String value )
    {
        return String.format( SCRIPT, value );
    }


    static String getSeedsCommand( String seeds )
    {
        return String.format( SEEDS_COMMAND, seeds );
    }


    static String getRpcAddressCommand( String ipAddress )
    {
        return String.format( RPC_ADDRESS_COMMAND, ipAddress );
    }


    static String getListenAddressCommand( String ipAddress )
    {
        return String.format( LISTEN_ADDRESS_COMMAND, ipAddress );
    }


    static String getClusterNameCommand( String clusterName )
    {
        return String.format( CLUSTER_NAME_COMMAND, clusterName );
    }


    static String getEndpointSnitchCommand()
    {
        return ENDPOINT_SNITCH_COMMAND;
    }


    static String getAutoBootstrapCommand()
    {
        return AUTO_BOOTSTRAP_COMMAND;
    }


    static String getRestartCommand()
    {
        return RESTART_COMMAND;
    }


    static String getStopCommand()
    {
        return STOP_COMMAND;
    }


    static String getRemoveFolderCommand()
    {
        return REMOVE_FOLDER;
    }


    static String getCreateFolderCommand()
    {
        return CREATE_FOLDER;
    }


    static String getChownFolderCommand()
    {
        return CHOWN;
    }


    public static String getNodetoolStatus()
    {
        return NODETOOL_STATUS;
    }


    public static String getHsErrors()
    {
        return HS_ERRORS;
    }


    public static String getAvailableRam()
    {
        return AVAILABLE_RAM;
    }


    public static String getHeapSize( long heapSizeInGb )
    {
        return String.format( HEAP_SIZE, heapSizeInGb, heapSizeInGb );
    }
}

