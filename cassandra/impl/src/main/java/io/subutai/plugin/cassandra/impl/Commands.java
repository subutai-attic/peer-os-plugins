package io.subutai.plugin.cassandra.impl;


public class Commands
{
    public static String statusCommand = "service cassandra status";
    public static String startCommand = "service cassandra start";
    public static String stopCommand = "service cassandra stop";
    public static String restartCommand = "service cassandra restart";
    public static String removeFolder = "rm -rf /var/lib/cassandra/";
    public static String createFolder = "mkdir /var/lib/cassandra/";
    public static String chown = "chown cassandra:cassandra /var/lib/cassandra/";
}

