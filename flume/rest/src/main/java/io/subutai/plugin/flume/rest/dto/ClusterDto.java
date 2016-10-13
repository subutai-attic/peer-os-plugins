package io.subutai.plugin.flume.rest.dto;


import java.util.ArrayList;
import java.util.List;


public class ClusterDto
{
    private String clusterName;
    private List<ContainerDto> containers;
    private String environmentId;


    public ClusterDto( final String clusterName )
    {
        this.clusterName = clusterName;
        this.containers = new ArrayList<>(  );
    }

    public void addContainerDto( ContainerDto containerDto )
    {
        containers.add( containerDto );
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public List<ContainerDto> getContainers()
    {
        return containers;
    }


    public void setContainers( final List<ContainerDto> containers )
    {
        this.containers = containers;
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
