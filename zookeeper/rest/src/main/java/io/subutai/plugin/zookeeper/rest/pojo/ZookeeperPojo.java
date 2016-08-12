package io.subutai.plugin.zookeeper.rest.pojo;


import java.util.Set;


public class ZookeeperPojo
{
    private String clusterName;
    private boolean autoScaling;
    private Set<ContainerPojo> nodes;

    private String environmentDataSource;
    private String environmentId;


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public Set<ContainerPojo> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<ContainerPojo> nodes )
    {
        this.nodes = nodes;
    }


    public String getEnvironmentDataSource()
    {
        return environmentDataSource;
    }


    public void setEnvironmentDataSource( String environmentDataSource )
    {
        this.environmentDataSource = environmentDataSource;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }
}
