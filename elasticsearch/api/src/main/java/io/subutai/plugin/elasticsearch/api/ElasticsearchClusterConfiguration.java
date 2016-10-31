package io.subutai.plugin.elasticsearch.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


public class ElasticsearchClusterConfiguration implements ConfigBase
{
    public static final String PRODUCT_KEY = "Elasticsearch";
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PRODUCT_KEY.toLowerCase();
    public static final String TEMPLATE_NAME = "elasticsearch";

    private String clusterName = "";
    private String environmentId;
    private Set<String> nodes = new HashSet<>();
    private boolean autoScaling;


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
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


    public Set<String> getNodes()
    {
        return nodes;
    }


    public void setNodes( Set<String> nodes )
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
}
