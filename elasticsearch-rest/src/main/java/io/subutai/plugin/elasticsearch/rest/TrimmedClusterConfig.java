package io.subutai.plugin.elasticsearch.rest;


import java.util.Set;
import java.util.UUID;


public class TrimmedClusterConfig
{
    private String clusterName;
    private UUID environmentId;
    private Set<UUID> nodes;


    public String getClusterName()
    {
        return clusterName;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public Set<UUID> getNodes()
    {
        return nodes;
    }
}
