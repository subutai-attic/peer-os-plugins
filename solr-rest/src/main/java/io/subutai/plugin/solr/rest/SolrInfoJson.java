package io.subutai.plugin.solr.rest;


import java.util.ArrayList;
import java.util.List;


public class SolrInfoJson
{
    private String name;
    private List<ContainerInfo> containerInfos;


    public SolrInfoJson( final String name )
    {
        this.name = name;
        this.containerInfos = new ArrayList<>(  );
    }

    public void addContainerInfo( ContainerInfo containerInfo )
    {
        containerInfos.add( containerInfo );
    }


    public String getName()
    {
        return name;
    }


    public void setName( final String name )
    {
        this.name = name;
    }


    public List<ContainerInfo> getContainerInfos()
    {
        return containerInfos;
    }


    public void setContainerInfos( final List<ContainerInfo> containerInfos )
    {
        this.containerInfos = containerInfos;
    }
}
