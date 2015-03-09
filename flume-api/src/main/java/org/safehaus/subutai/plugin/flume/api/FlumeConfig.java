package org.safehaus.subutai.plugin.flume.api;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.common.api.ConfigBase;

import com.google.common.collect.Sets;


public class FlumeConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Flume";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();

    private String clusterName = "";
    private String hadoopClusterName;
    private Set<UUID> nodes = Sets.newHashSet();
    private UUID environmentId;


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


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public Set<UUID> getNodes()
    {
        return nodes;
    }


    public void setNodes( Set<UUID> nodes ){
        this.nodes = nodes;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }


    @Override
    public String toString()
    {
        int c = nodes != null ? nodes.size() : 0;
        return "Config{" + "clusterName=" + clusterName + ", hadoopClusterName=" + hadoopClusterName + ", nodes=" + c
                + '}';
    }
}
