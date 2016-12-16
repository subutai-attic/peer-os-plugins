package io.subutai.plugin.appscale.api;


import java.util.ArrayList;
import java.util.List;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


public class AppScaleConfig implements ConfigBase
{
    public static final String PRODUCT_NAME = "AppScale";
    public static final String PRODUCT_KEY = "AppScale";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_NAME ).toLowerCase();

    private static final String TEMPLATE_NAME = "appscale";
    private String clusterName = "";

    private String controllerNode;
    private List<String> zookeeperNodes;
    private List<String> cassandraNodes;
    private List<String> appengineNodes;


    private List<String> nodes;
    private String environmentId;
    private String vlan;
    private String peerId;
    private String domain;

    private String login;
    private String password;


    public AppScaleConfig()
    {
        nodes = new ArrayList<>();
    }


    public String getClusterName()
    {
        return this.clusterName;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getProductName()
    {
        return AppScaleConfig.PRODUCT_NAME;
    }


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


    public String getLogin()
    {
        return login;
    }


    public void setLogin( final String login )
    {
        this.login = login;
    }


    public String getPassword()
    {
        return password;
    }


    public void setPassword( final String password )
    {
        this.password = password;
    }


    public static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }


    public List<String> getZookeeperNodes()
    {
        return zookeeperNodes;
    }


    public void setZookeeperNodes( final List<String> zookeeperNodes )
    {
        this.zookeeperNodes = zookeeperNodes;
    }


    public List<String> getCassandraNodes()
    {
        return cassandraNodes;
    }


    public void setCassandraNodes( final List<String> cassandraNodes )
    {
        this.cassandraNodes = cassandraNodes;
    }


    public List<String> getAppengineNodes()
    {
        return appengineNodes;
    }


    public void setAppengineNodes( final List<String> appengineNodes )
    {
        this.appengineNodes = appengineNodes;
    }


    public String getDomain()
    {
        return domain;
    }


    public void setDomain( final String domain )
    {
        this.domain = domain;
    }


    public static String getPackageName()
    {
        return PACKAGE_NAME;
    }


    public String getControllerNode()
    {
        return controllerNode;
    }


    public void setControllerNode( final String controllerNode )
    {
        this.controllerNode = controllerNode;
    }


    public String getVlan()
    {
        return vlan;
    }


    public void setVlan( final String vlan )
    {
        this.vlan = vlan;
    }


    public String getPeerId()
    {
        return peerId;
    }


    public void setPeerId( final String peerId )
    {
        this.peerId = peerId;
    }
}

