package io.subutai.plugin.spark.api;


import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.Lists;

import io.subutai.common.settings.Common;
import io.subutai.common.util.CollectionUtil;
import io.subutai.plugin.common.api.ConfigBase;


public class SparkClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Spark";
    public static final String PRODUCT_PACKAGE = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();

    private String clusterName = "";
    private String hadoopClusterName = "";
    private String masterNodeId;
    private Set<String> slaveIds = new HashSet<>();
    private String environmentId;
    private boolean autoScaling;


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getMasterNodeId()
    {
        return masterNodeId;
    }


    public Set<String> getSlaveIds()
    {
        return slaveIds;
    }


    public void setSlavesId( Set<String> slaves )
    {
        slaveIds = slaves;
    }


    public void setMasterNodeId( final String masterNodeId )
    {
        this.masterNodeId = masterNodeId;
    }


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


    public List<String> getAllNodesIds()
    {
        List<String> allNodesIds = Lists.newArrayList();
        if ( !CollectionUtil.isCollectionEmpty( slaveIds ) )
        {
            allNodesIds.addAll( slaveIds );
        }
        if ( masterNodeId != null )
        {
            allNodesIds.add( masterNodeId );
        }

        return allNodesIds;
    }


    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode( this.clusterName );
        return hash;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof SparkClusterConfig )
        {
            SparkClusterConfig other = ( SparkClusterConfig ) obj;
            return clusterName.equals( other.clusterName );
        }
        return false;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", masterNode=" + masterNodeId + ", slaves=" + slaveIds + '}';
    }
}

