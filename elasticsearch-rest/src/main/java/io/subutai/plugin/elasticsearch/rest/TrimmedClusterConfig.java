package io.subutai.plugin.elasticsearch.rest;


import java.util.Set;


public class TrimmedClusterConfig
{
    private String clusterName;
    private String environmentId;
    private Set<String> nodes;


    public String getClusterName()
    {
        return clusterName;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }
}
