package io.subutai.plugin.mongodb.rest.pojo;


import java.util.Set;


public class MongoPojo
{
    private String clusterName;
    private String environmentId;
    private Set<ContainerPojo> configHosts;
    private Set<ContainerPojo> routerHosts;
    private Set<ContainerPojo> dataHosts;


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


    public Set<ContainerPojo> getConfigHosts()
    {
        return configHosts;
    }


    public void setConfigHosts( final Set<ContainerPojo> configHosts )
    {
        this.configHosts = configHosts;
    }


    public Set<ContainerPojo> getRouterHosts()
    {
        return routerHosts;
    }


    public void setRouterHosts( final Set<ContainerPojo> routerHosts )
    {
        this.routerHosts = routerHosts;
    }


    public Set<ContainerPojo> getDataHosts()
    {
        return dataHosts;
    }


    public void setDataHosts( final Set<ContainerPojo> dataHosts )
    {
        this.dataHosts = dataHosts;
    }
}
