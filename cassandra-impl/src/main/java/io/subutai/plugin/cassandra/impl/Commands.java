package io.subutai.plugin.cassandra.impl;


import io.subutai.plugin.cassandra.api.CassandraClusterConfig;


public class Commands
{
    public static String statusCommand = "service cassandra status";
    public static String startCommand = "service cassandra start";
    public static String stopCommand = "service cassandra stop";
    public static String uninstallCommand =
            String.format( "apt-get --force-yes --assume-yes purge %s", CassandraClusterConfig.PACKAGE_NAME );
}

