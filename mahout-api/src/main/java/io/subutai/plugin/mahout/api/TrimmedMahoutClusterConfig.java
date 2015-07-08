package io.subutai.plugin.mahout.api;


import java.util.List;
import java.util.UUID;


public class TrimmedMahoutClusterConfig
{

    String clusterName;
    String hadoopClusterName;
    List<UUID> nodes;


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public List<UUID> getNodes()
    {
        return nodes;
    }
}
