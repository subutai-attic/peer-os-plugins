package io.subutai.plugin.zookeeper.rest.pojo;


import java.util.Set;


public class ZookeeperPojo
{
    private String clusterName;
    private boolean autoScaling;
    private Set<ContainerPojo> nodes;


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
}
