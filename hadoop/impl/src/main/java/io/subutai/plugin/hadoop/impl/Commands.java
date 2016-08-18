package io.subutai.plugin.hadoop.impl;


import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class Commands
{
    HadoopClusterConfig config;


    public Commands( HadoopClusterConfig config )
    {
        this.config = config;
    }


    public static String getNodeStatusCommand()
    {
        return "source /etc/profile ; jps";
    }


    public static String getStartYarnCommand()
    {
        return "source /etc/profile ; start-yarn.sh";
    }


    public static String getStartDfsCommand()
    {
        return "source /etc/profile ; start-dfs.sh";
    }


    public static String getStopYarnCommand()
    {
        return "source /etc/profile ; stop-yarn.sh";
    }


    public static String getStopDfsCommand()
    {
        return "source /etc/profile ; stop-dfs.sh";
    }


    public static String getCreateNamenodeDirectoryCommand()
    {
        return "mkdir -pv /opt/hadoop/data/namenode";
    }


    public static String getUpdateHdfsMaster()
    {
        return "rm /opt/hadoop/etc/hadoop/hdfs-site.xml ; cp /opt/hadoop/etc/hadoop/hdfs-site.xml.master.template "
                + "/opt/hadoop/etc/hadoop/hdfs-site.xml";
    }


    public static String getUpdateCore()
    {
        return "rm /opt/hadoop/etc/hadoop/core-site.xml ; cp /opt/hadoop/etc/hadoop/core-site.xml.template "
                + "/opt/hadoop/etc/hadoop/core-site.xml";
    }


    public static String getUpdateYarn()
    {
        return "rm /opt/hadoop/etc/hadoop/yarn-site.xml ; cp /opt/hadoop/etc/hadoop/yarn-site.xml.template "
                + "/opt/hadoop/etc/hadoop/yarn-site.xml";
    }


    public static String getCreateMapred()
    {
        return "cp /opt/hadoop/etc/hadoop/mapred-site.xml.template /opt/hadoop/etc/hadoop/mapred-site.xml";
    }


    public static String getSetNamenodeIp( String ip )
    {
        return String.format( "bash /opt/hadoop/etc/hadoop/hadoop-conf.sh namenode.ip.hdfs %s", ip );
    }


    public static String getSetReplication( final String replicationFactor )
    {
        return String.format( "bash /opt/hadoop/etc/hadoop/hadoop-conf.sh repl %s", replicationFactor );
    }


    public static String getSetSlavesCommand( final String slaveIPs )
    {
        return String.format( "echo -e \"%s\" > /opt/hadoop/etc/hadoop/slaves", slaveIPs );
    }


    public static String getCreateDatanodeDirectoryCommand()
    {
        return "mkdir -pv /opt/hadoop/data/datanode";
    }


    public static String getUpdateHdfsSlave()
    {
        return "rm /opt/hadoop/etc/hadoop/hdfs-site.xml ; cp /opt/hadoop/etc/hadoop/hdfs-site.xml.slave.template "
                + "/opt/hadoop/etc/hadoop/hdfs-site.xml";
    }


    public static String getSetNamenodeIpCore( final String ip )
    {
        return String.format( "bash /opt/hadoop/etc/hadoop/hadoop-conf.sh namenode.ip.core %s", ip );
    }


    public static String getFormatHdfs()
    {
        return "source /etc/profile ; hdfs namenode -format";
    }


    public static String getCleanHdfs()
    {
        return "rm /opt/hadoop/etc/hadoop/hdfs-site.xml ; cp /opt/hadoop/etc/hadoop/hdfs-site.xml.clean "
                + "/opt/hadoop/etc/hadoop/hdfs-site.xml";
    }


    public static String getCleanCore()
    {
        return "rm /opt/hadoop/etc/hadoop/core-site.xml ; cp /opt/hadoop/etc/hadoop/core-site.xml.clean "
                + "/opt/hadoop/etc/hadoop/core-site.xml";
    }


    public static String getCleanYarn()
    {
        return "rm /opt/hadoop/etc/hadoop/yarn-site.xml ; cp /opt/hadoop/etc/hadoop/yarn-site.xml.clean "
                + "/opt/hadoop/etc/hadoop/yarn-site.xml";
    }


    public static String getCleanMapred()
    {
        return "rm /opt/hadoop/etc/hadoop/mapred-site.xml";
    }


    public static String getExcludeCommand( final String slaveIP )
    {
        return String.format( "echo \"%s\" >> /opt/hadoop/etc/hadoop/dfs.exclude", slaveIP );
    }


    public static String getIncludeCommand( final String slaveIP )
    {
        return String.format( "sed -i -e \"/%s/d\" /opt/hadoop/etc/hadoop/dfs.exclude", slaveIP );
    }


    public static String getUncommentExcludeSettingsCommand()
    {
        return "bash /opt/hadoop/etc/hadoop/hadoop-conf.sh uncomment 51,54";
    }


    public static String getCommentExcludeSettingsCommand()
    {
        return "bash /opt/hadoop/etc/hadoop/hadoop-conf.sh comment 51,54";
    }


    public static String getRefreshNodesCommand()
    {
        return "source /etc/profile ; hdfs dfsadmin -refreshNodes";
    }


    public static String getDeleteNamenodeFolder()
    {
        return "rm -rf /opt/hadoop/data/namenode ; rm -rf /opt/hadoop/data";
    }


    public static String getDeleteDataNodeFolder()
    {
        return "rm -rf /opt/hadoop/data/datanode ; rm -rf /opt/hadoop/data";
    }
}