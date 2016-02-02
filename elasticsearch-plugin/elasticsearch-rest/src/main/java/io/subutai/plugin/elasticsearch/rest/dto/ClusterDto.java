package io.subutai.plugin.elasticsearch.rest.dto;


import java.util.ArrayList;
import java.util.List;

import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


public class ClusterDto
{
    private Boolean autoScaling;
    private String clusterName;
    private List<ContainerDto> containers;

    public ClusterDto(  )
    {
        containers = new ArrayList<>(  );
    }

    public ClusterDto( ElasticsearchClusterConfiguration elasticsearchClusterConfiguration )
    {
        this();
        this.autoScaling = elasticsearchClusterConfiguration.isAutoScaling();
        this.clusterName = elasticsearchClusterConfiguration.getClusterName();
    }


    public Boolean getAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final Boolean autoScaling )
    {
        this.autoScaling = autoScaling;
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

    public void addContainerDto( ContainerDto containerDto )
    {
        containers.add( containerDto );
    }
}
