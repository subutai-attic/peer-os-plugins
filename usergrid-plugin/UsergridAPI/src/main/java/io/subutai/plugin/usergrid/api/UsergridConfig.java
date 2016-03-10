/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.api;


import java.util.List;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class UsergridConfig implements ConfigBase
{

    public static final String PRODUCT_NAME = "Usergrid";
    public static final String PRODUCT_KEY = "Usergrid";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_NAME ).toLowerCase ();
    private static final String TEMPLATE_NAME = "tomcat";
    private String clusterName;

    private String tomcatName;
    private List<String> cassandraName;
    private List<String> elasticSName;
    private List<String> clusters;
    private String domainName = "intra.lan";
    private List<String> nodes;
    private String environmentId;
    private String tracker;
    private String userDomain;


    public String getClusterName ()
    {
        return this.clusterName;
    }


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getProductName ()
    {
        return PRODUCT_NAME;
    }


    public String getProductKey ()
    {
        return PRODUCT_KEY;
    }


    public String getTomcatName ()
    {
        return tomcatName;
    }


    public void setTomcatName ( String tomcatName )
    {
        this.tomcatName = tomcatName;
    }


    public List<String> getCassandraName ()
    {
        return cassandraName;
    }


    public void setCassandraName ( List<String> cassandraName )
    {
        this.cassandraName = cassandraName;
    }


    public List<String> getElasticSName ()
    {
        return elasticSName;
    }


    public void setElasticSName ( List<String> elasticSName )
    {
        this.elasticSName = elasticSName;
    }


    public List<String> getClusters ()
    {
        return clusters;
    }


    public void setClusters ( List<String> clusters )
    {
        this.clusters = clusters;
    }


    public String getDomainName ()
    {
        return domainName;
    }


    public void setDomainName ( String domainName )
    {
        this.domainName = domainName;
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


    public String getTracker ()
    {
        return tracker;
    }


    public void setTracker ( String tracker )
    {
        this.tracker = tracker;
    }


    public String getUserDomain ()
    {
        return userDomain;
    }


    public void setUserDomain ( String userDomain )
    {
        this.userDomain = userDomain;
    }


}

