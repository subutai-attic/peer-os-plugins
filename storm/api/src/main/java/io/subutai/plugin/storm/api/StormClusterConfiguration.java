package io.subutai.plugin.storm.api;


import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


public class StormClusterConfiguration implements ConfigBase
{

    public static final String PRODUCT_NAME = "Storm";
    public static final String PRODUCT_KEY = "Storm";
    public static final String TEMPLATE_NAME = "storm102";

    private String clusterName;
    private int supervisorsCount;
    private boolean externalZookeeper;
    private String zookeeperClusterName;
    private String nimbus; // master node
    private Set<String> supervisors = new HashSet<>(); // worker nodes
    private String environmentId;
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private boolean isAutoScaling;


    public boolean isAutoScaling()
    {
        return isAutoScaling;
    }


    public void setAutoScaling( final boolean isAutoScaling )
    {
        this.isAutoScaling = isAutoScaling;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
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
        return PRODUCT_NAME;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getNimbus()
    {
        return nimbus;
    }


    public void setNimbus( String nimbus )
    {
        this.nimbus = nimbus;
    }


    public int getSupervisorsCount()
    {
        return supervisors.size();
    }


    public void setSupervisorsCount( int supervisorsCount )
    {
        this.supervisorsCount = supervisorsCount;
    }


    public Set<String> getSupervisors()
    {
        return supervisors;
    }


    public void setSupervisors( Set<String> supervisors )
    {
        this.supervisors = supervisors;
    }


    public boolean isExternalZookeeper()
    {
        return externalZookeeper;
    }


    public void setExternalZookeeper( boolean externalZookeeper )
    {
        this.externalZookeeper = externalZookeeper;
    }


    public String getZookeeperClusterName()
    {
        return zookeeperClusterName;
    }


    public void setZookeeperClusterName( String zookeeperClusterName )
    {
        this.zookeeperClusterName = zookeeperClusterName;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allAgents = new HashSet<>();
        if ( nimbus != null )
        {
            allAgents.add( nimbus );
        }
        if ( supervisors != null )
        {
            allAgents.addAll( supervisors );
        }
        return allAgents;
    }


    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode( this.clusterName );
        return hash;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof StormClusterConfiguration )
        {
            StormClusterConfiguration other = ( StormClusterConfiguration ) obj;
            return clusterName.equals( other.clusterName );
        }
        return false;
    }
}
