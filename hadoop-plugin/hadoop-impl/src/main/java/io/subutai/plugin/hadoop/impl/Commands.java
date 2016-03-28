package io.subutai.plugin.hadoop.impl;


import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class Commands
{
    HadoopClusterConfig config;


    public Commands( HadoopClusterConfig config )
    {
        this.config = config;
    }

    public static String getStatusAll() { return "service hadoop-all status"; }

    public static String getStatusNameNodeCommand()
    {
        return "service hadoop-dfs status";
    }


    public static String getStartNameNodeCommand()
    {
        return "service hadoop-dfs start";
    }


    public static String getStopNameNodeCommand()
    {
        return "service hadoop-dfs stop";
    }


    public static String getStartJobTrackerCommand()
    {
        return "service hadoop-mapred start";
    }


    public static String getStopJobTrackerCommand()
    {
        return "service hadoop-mapred stop";
    }


    public static String getStatusJobTrackerCommand()
    {
        return "service hadoop-mapred status";
    }


    public static String getStatusDataNodeCommand()
    {
        return "service hadoop-dfs status";
    }


    public static String getClearMastersCommand()
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh masters clear";
    }


    public static String getClearSlavesCommand()
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves clear";
    }


    public static String getRefreshJobTrackerCommand()
    {
        return "/opt/hadoop*/bin/hadoop mradmin -refreshNodes";
    }


    public static String getStartDataNodeCommand()
    {
        return "/opt/hadoop*/bin/hadoop-daemon.sh start datanode";
    }


    public static String getStopDataNodeCommand()
    {
        return "/opt/hadoop*/bin/hadoop-daemon.sh stop datanode";
    }


    public static String getStartTaskTrackerCommand()
    {
        return "/opt/hadoop*/bin/hadoop-daemon.sh start tasktracker";
    }


    public static String getStopTaskTrackerCommand()
    {
        return "/opt/hadoop*/bin/hadoop-daemon.sh stop tasktracker";
    }


    public static String getStatusTaskTrackerCommand()
    {
        return "service hadoop-mapred status";
    }


    public static String getFormatNameNodeCommand()
    {
        return "/opt/hadoop*/bin/hadoop namenode -format";
    }


    public static String getReportHadoopCommand()
    {
        return "/opt/hadoop*/bin/hadoop dfsadmin -report";
    }


    public static String getRefreshNameNodeCommand()
    {
        return "/opt/hadoop*/bin/hadoop dfsadmin -refreshNodes";
    }


    public static String getSetDataNodeCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves " + hostname;
    }


    public static String getExcludeDataNodeCommand( String ip )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh dfs.exclude clear " + ip;
    }


    public static String getSetTaskTrackerCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves " + hostname;
    }


    public static String getExcludeTaskTrackerCommand( String ip )
    {

        return "/opt/hadoop*/bin/hadoop-master-slave.sh mapred.exclude clear " + ip;
    }


    public static String getRemoveTaskTrackerCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves clear " + hostname;
    }


    public static String getIncludeTaskTrackerCommand( String ip )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh mapred.exclude " + ip;
    }


    public static String getRemoveDataNodeCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves clear " + hostname;
    }


    public static String getIncludeDataNodeCommand( String ip )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh dfs.exclude " + ip;
    }


    public static String getConfigureJobTrackerCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves " + hostname;
    }


    public static String getConfigureSecondaryNameNodeCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh masters " + hostname;
    }


    public static String getConfigureSlaveNodes( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves " + hostname;
    }


    public static  String getConfigureTaskTrackersCommand( String hostname )
    {
        return "/opt/hadoop*/bin/hadoop-master-slave.sh slaves " + hostname;
    }


    public String getSetMastersCommand( String namenode, String jobtracker )
    {
        return "/opt/hadoop*/bin/hadoop-configure.sh " +
                namenode + ":" + HadoopClusterConfig.NAME_NODE_PORT + " " +
                jobtracker + ":" + HadoopClusterConfig.JOB_TRACKER_PORT + " " +
                config.getReplicationFactor();
    }

    public static String getSetMastersCommand( String namenode, String jobtracker, int replicationFactor )
    {
        return "/opt/hadoop*/bin/hadoop-configure.sh " +
                namenode + ":" + HadoopClusterConfig.NAME_NODE_PORT + " " +
                jobtracker + ":" + HadoopClusterConfig.JOB_TRACKER_PORT + " " +
                replicationFactor;
    }

    public static String getClearDataDirectory(){
        return " rm -rf /var/lib/hadoop-root/";
    }

}