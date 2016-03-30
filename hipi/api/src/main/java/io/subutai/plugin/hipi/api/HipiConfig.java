package io.subutai.plugin.hipi.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


public class HipiConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Hipi";
    public static final String PRODUCT_PACKAGE = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();

    private String clusterName;
    private String environmentId;
    private Set<String> nodes = new HashSet<>();
    private String hadoopClusterName;


    public String getClusterName()
    {
        return clusterName;
    }


    public HipiConfig setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
        return this;
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


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", nodes=" + nodes + '}';
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
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
