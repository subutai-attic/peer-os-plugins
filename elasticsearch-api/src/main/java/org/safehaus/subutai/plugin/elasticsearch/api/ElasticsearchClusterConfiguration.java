package org.safehaus.subutai.plugin.elasticsearch.api;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.common.api.ConfigBase;


public class ElasticsearchClusterConfiguration implements ConfigBase
{
    public static final String PRODUCT_KEY = "Elasticsearch";
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PRODUCT_KEY.toLowerCase();
    public static final String TEMPLATE_NAME = "elasticsearch";

    private String clusterName = "";
    private UUID environmentId;
    private Set<UUID> nodes = new HashSet<>();
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


    public Set<UUID> getNodes()
    {
        return nodes;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }
}
