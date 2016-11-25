package io.subutai.plugin.galera.rest.dto;


import java.util.ArrayList;
import java.util.List;


public class ClusterDto
{
    private String name;
    private List<ContainerDto> containers;
    private String environmentId;


    public ClusterDto( final String name )
    {
        this.name = name;
        this.containers = new ArrayList<>(  );
    }

    public void addContainerDto( ContainerDto containerDto )
    {
        containers.add( containerDto );
    }


    public String getName()
    {
        return name;
    }


    public void setName( final String name )
    {
        this.name = name;
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
