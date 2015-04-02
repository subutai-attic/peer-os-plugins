/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mongodb.api;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ConfigBase;

import com.google.common.collect.Sets;


/**
 * Holds a single mongo cluster configuration settings
 */
public class MongoClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "MongoDB";
    public static final String PRODUCT_NAME = "mongo";
    public static final String TEMPLATE_NAME = "mongo";
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PRODUCT_NAME.toLowerCase();

    private String clusterName = "";
    private String replicaSetName = "repl";
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private int numberOfConfigServers = 1;
    private int numberOfRouters = 1;
    private int numberOfDataNodes = 1;
    private int cfgSrvPort = 27019;
    private int routerPort = 27018;
    private int dataNodePort = 27017;
    private UUID primaryNode;

    private Set<UUID> configHosts;
    private Set<UUID> routerHosts;
    private Set<UUID> dataHosts;
    private UUID environmentId;
    private boolean autoScaling;



    public Set<UUID> getAllNodes()
    {
        Set<UUID> allNodes = Sets.newHashSet();
        if ( !CollectionUtil.isCollectionEmpty( configHosts ) )
        {
            allNodes.addAll( configHosts );
        }
        if ( !CollectionUtil.isCollectionEmpty( routerHosts ) )
        {
            allNodes.addAll( routerHosts );
        }
        if ( !CollectionUtil.isCollectionEmpty( dataHosts ) )
        {
            allNodes.addAll( dataHosts );
        }

        return allNodes;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getReplicaSetName()
    {
        return replicaSetName;
    }


    public void setReplicaSetName( final String replicaSetName )
    {
        this.replicaSetName = replicaSetName;
    }


    public int getNumberOfConfigServers()
    {
        return numberOfConfigServers;
    }


    public void setNumberOfConfigServers( final int numberOfConfigServers )
    {
        this.numberOfConfigServers = numberOfConfigServers;
    }


    public int getNumberOfRouters()
    {
        return numberOfRouters;
    }


    public void setNumberOfRouters( final int numberOfRouters )
    {
        this.numberOfRouters = numberOfRouters;
    }


    public int getNumberOfDataNodes()
    {
        return numberOfDataNodes;
    }


    public void setNumberOfDataNodes( final int numberOfDataNodes )
    {
        this.numberOfDataNodes = numberOfDataNodes;
    }


    public int getCfgSrvPort()
    {
        return cfgSrvPort;
    }


    public void setCfgSrvPort( int cfgSrvPort )
    {
        cfgSrvPort = cfgSrvPort;
    }


    public int getRouterPort()
    {
        return routerPort;
    }


    public void setRouterPort( int routerPort )
    {
        routerPort = routerPort;
    }


    public int getDataNodePort()
    {
        return dataNodePort;
    }


    public void setDataNodePort( int dataNodePort )
    {
        dataNodePort = dataNodePort;
    }


    public UUID getPrimaryNode()
    {
        return primaryNode;
    }


    public void setPrimaryNode( final UUID primaryNode )
    {
        this.primaryNode = primaryNode;
    }


    public Set<UUID> getConfigHosts()
    {
        return configHosts;
    }


    public void setConfigHosts( final Set<UUID> configHosts )
    {
        this.configHosts = configHosts;
    }


    public Set<UUID> getRouterHosts()
    {
        return routerHosts;
    }


    public void setRouterHosts( final Set<UUID> routerHosts )
    {
        this.routerHosts = routerHosts;
    }


    public Set<UUID> getDataHosts()
    {
        return dataHosts;
    }


    public void setDataHosts( final Set<UUID> dataHosts )
    {
        this.dataHosts = dataHosts;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }


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


    @Override
    public String getProductName()
    {
        return PRODUCT_NAME;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public List<NodeType> getNodeRoles( UUID uuid ){
        List<NodeType> roles = new ArrayList<>();
        if ( routerHosts.contains( uuid ) ){
            roles.add( NodeType.ROUTER_NODE );
        }
        if ( dataHosts.contains( uuid ) ){
            roles.add( NodeType.DATA_NODE );
        }
        if ( configHosts.contains( uuid ) ){
            roles.add( NodeType.CONFIG_NODE );
        }
        return roles;
    }

    @Override
    public String toString() {
        return "MongoClusterConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", replicaSetName='" + replicaSetName + '\'' +
                ", domainName='" + domainName + '\'' +
                ", numberOfConfigServers=" + numberOfConfigServers +
                ", numberOfRouters=" + numberOfRouters +
                ", numberOfDataNodes=" + numberOfDataNodes +
                ", cfgSrvPort=" + cfgSrvPort +
                ", routerPort=" + routerPort +
                ", dataNodePort=" + dataNodePort +
                ", primaryNode=" + primaryNode +
                ", configHosts=" + configHosts +
                ", routerHosts=" + routerHosts +
                ", dataHosts=" + dataHosts +
                ", environmentId=" + environmentId +
                ", autoScaling=" + autoScaling +
                '}';
    }
}
