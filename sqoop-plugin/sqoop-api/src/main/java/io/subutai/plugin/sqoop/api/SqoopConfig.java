package io.subutai.plugin.sqoop.api;


import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.subutai.core.plugincommon.api.ConfigBase;


public class SqoopConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Sqoop";
    public static final String TEMPLATE_NAME = "hadoopsqoop";

    private String clusterName = "";
    private String environmentId;
    private int nodesCount;
    private Set<String> nodes = new HashSet<>();
    private String hadoopClusterName = "";
    private Set<String> hadoopNodes = new HashSet<>();


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


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( String environmenId )
    {
        this.environmentId = environmenId;
    }


    public int getNodesCount()
    {
        return nodesCount;
    }


    public void setNodesCount( int nodesCount )
    {
        this.nodesCount = nodesCount;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }


    public void setNodes( Set<String> nodeIds )
    {
        this.nodes = nodeIds;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public Set<String> getHadoopNodes()
    {
        return hadoopNodes;
    }


    public void setHadoopNodes( Set<String> hadoopNodes )
    {
        this.hadoopNodes = hadoopNodes;
    }


    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 23 * hash + Objects.hashCode( this.clusterName );
        return hash;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof SqoopConfig )
        {
            SqoopConfig other = ( SqoopConfig ) obj;
            return Objects.equals( this.clusterName, other.clusterName );
        }
        return false;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", nodes=" + ( nodes != null ? nodes.size() : 0 ) + '}';
    }
}

