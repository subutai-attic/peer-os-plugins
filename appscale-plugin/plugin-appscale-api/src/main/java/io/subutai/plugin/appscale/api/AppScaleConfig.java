package io.subutai.plugin.appscale.api;


import java.util.ArrayList;
import java.util.List;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


/**
 *
 * @author caveman
 */
public class AppScaleConfig implements ConfigBase
{
    public static final String PRODUCT_NAME = "AppScale";
    public static final String PRODUCT_KEY = "AppScale";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_NAME ).toLowerCase ();

    // private static final String TEMPLATE_NAME = "Appscale";
    private static final String TEMPLATE_NAME = "appscale"; // we will be using master template
    private String clusterName = ""; // this will be login point => management AKA master

    // if any means of other clusters.
    private String zookeeperName;
    private String cassandraName;
    private String openJreName;


    private String domainName = "intra.lan";
    private List<String> nodes;
    private String environmentId;
    private String environmentName;
    private String containerType;
    private String tracker;
    private List<AppScaleConfig> clusters;
    private List<String> clusternames;


    public AppScaleConfig ()
    {
        nodes = new ArrayList<> ();
    }


    @Override
    public String getClusterName ()
    {
        return this.clusterName;
    }


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    @Override
    public String getProductName ()
    {
        return AppScaleConfig.PRODUCT_NAME;
    }


    @Override
    public String getProductKey ()
    {
        return AppScaleConfig.PRODUCT_KEY;
    }


    public List<String> getNodes ()
    {
        return nodes;
    }


    public void setNodes ( List<String> nodes )
    {
        this.nodes = nodes;
    }


    public String getEnvironmentId ()
    {
        return environmentId;
    }


    public void setEnvironmentId ( String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getContainerType ()
    {
        return containerType;
    }


    public void setContainerType ( String containerType )
    {
        this.containerType = containerType;
    }


    public String getTracker ()
    {
        return tracker;
    }


    public void setTracker ( String tracker )
    {
        this.tracker = tracker;
    }


    public String getZookeeperName ()
    {
        return zookeeperName;
    }


    public void setZookeeperName ( String zookeeperName )
    {
        this.zookeeperName = zookeeperName;
    }


    public String getCassandraName ()
    {
        return cassandraName;
    }


    public void setCassandraName ( String cassandraName )
    {
        this.cassandraName = cassandraName;
    }


    public String getOpenJreName ()
    {
        return openJreName;
    }


    public void setOpenJreName ( String openJreName )
    {
        this.openJreName = openJreName;
    }


    public List<AppScaleConfig> getClusters ()
    {
        return clusters;
    }


    public void setClusters ( List<AppScaleConfig> clusters )
    {
        this.clusters = clusters;
    }


    public List<String> getClusternames ()
    {
        return clusternames;
    }


    public void setClusternames ( List<String> clusternames )
    {
        this.clusternames = clusternames;
    }


    public String getEnvironmentName ()
    {
        return environmentName;
    }


    public void setEnvironmentName ( String environmentName )
    {
        this.environmentName = environmentName;
    }


    public static String getPRODUCT_NAME ()
    {
        return PRODUCT_NAME;
    }


    public static String getPRODUCT_KEY ()
    {
        return PRODUCT_KEY;
    }


    public static String getPACKAGE_NAME ()
    {
        return PACKAGE_NAME;
    }


    public static String getTEMPLATE_NAME ()
    {
        return TEMPLATE_NAME;
    }


    public String getDomainName ()
    {
        return domainName;
    }


    public void setDomainName ( String domainName )
    {
        this.domainName = domainName;
    }

    
}

