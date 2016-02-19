package io.subutai.plugin.ceph.api;


import io.subutai.core.plugincommon.api.ConfigBase;


public class CephClusterConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Ceph";
    private String clusterName;
    private String environmentId;
    private String radosGW;


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


    public String getRadosGW()
    {
        return radosGW;
    }


    public void setRadosGW( final String radosGW )
    {
        this.radosGW = radosGW;
    }
}
