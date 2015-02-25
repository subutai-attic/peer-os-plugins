/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.rest;


import java.util.Set;

import org.safehaus.subutai.common.settings.Common;

import com.google.common.collect.Sets;


/**
 * @author dilshat
 */
public class TrimmedMongodbConfig
{

    private String environmentId = "";
    private String clusterName = "";
    private String replicaSetName = "repl";
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private int cfgSrvPort = 27019;
    private int routerPort = 27018;
    private int dataNodePort = 27017;
    private Set<String> configNodes = Sets.newHashSet();
    private Set<String> dataNodes = Sets.newHashSet();
    private Set<String> routerNodes = Sets.newHashSet();


    public String getClusterName()
    {
        return clusterName;
    }


    public String getReplicaSetName()
    {
        return replicaSetName;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public int getCfgSrvPort()
    {
        return cfgSrvPort;
    }


    public int getRouterPort()
    {
        return routerPort;
    }


    public int getDataNodePort()
    {
        return dataNodePort;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public void setReplicaSetName( final String replicaSetName )
    {
        this.replicaSetName = replicaSetName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public void setCfgSrvPort( final int cfgSrvPort )
    {
        this.cfgSrvPort = cfgSrvPort;
    }


    public void setRouterPort( final int routerPort )
    {
        this.routerPort = routerPort;
    }


    public void setDataNodePort( final int dataNodePort )
    {
        this.dataNodePort = dataNodePort;
    }


    public Set<String> getConfigNodes()
    {
        return configNodes;
    }


    public void setConfigNodes( final Set<String> configNodes )
    {
        this.configNodes = configNodes;
    }


    public Set<String> getDataNodes()
    {
        return dataNodes;
    }


    public void setDataNodes( final Set<String> dataNodes )
    {
        this.dataNodes = dataNodes;
    }


    public Set<String> getRouterNodes()
    {
        return routerNodes;
    }


    public void setRouterNodes( final Set<String> routerNodes )
    {
        this.routerNodes = routerNodes;
    }
}
