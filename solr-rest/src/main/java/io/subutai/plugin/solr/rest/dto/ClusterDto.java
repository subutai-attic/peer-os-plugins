package io.subutai.plugin.solr.rest.dto;


import java.util.ArrayList;
import java.util.List;


public class ClusterDto
{
    private String name;
    private List<ContainerDto> containerDtos;


    public ClusterDto( final String name )
    {
        this.name = name;
        this.containerDtos = new ArrayList<>(  );
    }

    public void addContainerInfo( ContainerDto containerDto )
    {
        containerDtos.add( containerDto );
    }


    public String getName()
    {
        return name;
    }


    public void setName( final String name )
    {
        this.name = name;
    }


    public List<ContainerDto> getContainerDtos()
    {
        return containerDtos;
    }


    public void setContainerDtos( final List<ContainerDto> containerDtos )
    {
        this.containerDtos = containerDtos;
    }
}
