package org.safehaus.subutai.plugin.pig.rest;


import java.util.Set;


public class TrimmedConfig
{
    private String clusterName;
    private Set<String> nodes;
    private String hadoopClusterName;


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }
}
