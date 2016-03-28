package io.subutai.plugin.shark.rest.pojo;


import java.util.Set;


public class SharkPojo
{
    private String clusterName;
    private String sparkClusterName;
    private String environmentId;
    private Set<ContainerPojo> nodes;


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getSparkClusterName()
    {
        return sparkClusterName;
    }


    public void setSparkClusterName( final String sparkClusterName )
    {
        this.sparkClusterName = sparkClusterName;
    }


    public Set<ContainerPojo> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<ContainerPojo> nodes )
    {
        this.nodes = nodes;
    }
}
