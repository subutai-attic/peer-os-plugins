package io.subutai.plugin.shark.api;


import java.util.Set;

import com.google.common.collect.Sets;

import io.subutai.plugin.common.api.ConfigBase;


public class SharkClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Shark";

    private String clusterName = "";
    private String sparkClusterName = "";
    private Set<String> nodeIds = Sets.newHashSet();
    private String environmentId;
    private boolean autoScaling;


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
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


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
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


    public Set<String> getNodeIds()
    {
        return nodeIds;
    }


    public String getSparkClusterName()
    {
        return sparkClusterName;
    }


    public void setSparkClusterName( String sparkClusterName )
    {
        this.sparkClusterName = sparkClusterName;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", nodeIds=" + nodeIds + '}';
    }
}

