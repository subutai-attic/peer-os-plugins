package io.subutai.plugin.appscale.api;


import java.util.ArrayList;
import java.util.List;

import io.subutai.common.settings.Common;
import io.subutai.plugin.common.api.ConfigBase;


/**
 *
 * @author caveman
 */
public class AppScaleConfig implements ConfigBase
{
    public static final String PRODUCT_NAME = "Appscale";
    public static final String PRODUCT_KEY = "Appscale";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_NAME ).toLowerCase();

    private static final String TEMPLATE_NAME = "Appscale";
    private String clusterName = "";
    private final String domainName = Common.DEFAULT_DOMAIN_NAME;
    private List<String> nodes;
    private String environmentId;
    private String containerType;
    private String tracker;


    public AppScaleConfig()
    {
        nodes = new ArrayList<>();
    }


    @Override
    public String getClusterName()
    {
        return this.clusterName;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    @Override
    public String getProductName()
    {
        return AppScaleConfig.PRODUCT_NAME;
    }


    @Override
    public String getProductKey()
    {
        return AppScaleConfig.PRODUCT_KEY;
    }


    public List<String> getNodes()
    {
        return nodes;
    }


    public void setNodes( List<String> nodes )
    {
        this.nodes = nodes;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getContainerType()
    {
        return containerType;
    }


    public void setContainerType( String containerType )
    {
        this.containerType = containerType;
    }


    public String getTracker()
    {
        return tracker;
    }


    public void setTracker( String tracker )
    {
        this.tracker = tracker;
    }


    public static String getPRODUCT_NAME()
    {
        return PRODUCT_NAME;
    }


    public static String getPRODUCT_KEY()
    {
        return PRODUCT_KEY;
    }


    public static String getPACKAGE_NAME()
    {
        return PACKAGE_NAME;
    }


    public static String getTEMPLATE_NAME()
    {
        return TEMPLATE_NAME;
    }


    public String getDomainName()
    {
        return domainName;
    }


}

