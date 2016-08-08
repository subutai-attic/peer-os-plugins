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
    public static final String COMMIT_LOG_DIR = "commitlog_dir /var/lib/cassandra/commitlog";
    public static final String SAVED_CHACHE_DIR = "saved_cache_dir /var/lib/cassandra/saved_caches";
}

