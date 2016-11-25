package io.subutai.plugin.galera.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.core.plugincommon.api.ConfigBase;


public class GaleraClusterConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Galera";
    public static final String PRODUCT_NAME = "Galera";
    public static final String TEMPLATE_NAME = "Galera";
    private String clusterName = "";
    private int numberOfNodes = 1;
    private Set<String> nodes = new HashSet<>();
    private String environmentId;
    private String initiator;


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public int getNumberOfNodes()
    {
        return nodes.size();
    }


    public void setNumberOfNodes( final int numberOfNodes )
    {
        this.numberOfNodes = numberOfNodes;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<String> nodes )
    {
        this.nodes = nodes;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    @Override
    public String getClusterName()
    {
        return clusterName;
    }


    @Override
    public String getProductName()
    {
        return PRODUCT_KEY;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public void setInitiator( final String initiator )
    {
        this.initiator = initiator;
    }
}