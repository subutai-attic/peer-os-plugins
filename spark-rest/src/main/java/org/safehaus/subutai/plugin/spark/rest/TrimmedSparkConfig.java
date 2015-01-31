package org.safehaus.subutai.plugin.spark.rest;


import java.util.Set;
import java.util.UUID;


public class TrimmedSparkConfig
{

    private String clusterName;
    private String hadoopClusterName;

    private UUID masterHostId;
    private Set<UUID> slaveHostsIds;


    public String getClusterName()
    {
        return clusterName;
    }


    public UUID getMasterHostId()
    {
        return masterHostId;
    }


    public Set<UUID> getSlaveHostsIds()
    {
        return slaveHostsIds;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }
}
