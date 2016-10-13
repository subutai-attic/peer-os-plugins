package io.subutai.plugin.hive.api;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.subutai.core.plugincommon.api.ConfigBase;


public class HiveConfig implements ConfigBase, Serializable
{

    public static final String PRODUCT_KEY = "Hive";
    private String clusterName = "";
    private String hadoopClusterName = "";
    private String server;
    private Set<String> clients = new HashSet<>();
    private String environmentId;
    private String namenode;


    public HiveConfig()
    {
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


    public String getServer()
    {
        return server;
    }


    public void setServer( String server )
    {
        this.server = server;
    }


    public Set<String> getClients()
    {
        return clients;
    }


    public void setClients( Set<String> clients )
    {
        this.clients = clients;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = new HashSet<>();
        if ( clients != null )
        {
            allNodes.addAll( clients );
        }
        if ( server != null )
        {
            allNodes.add( server );
        }
        return allNodes;
    }


    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode( this.clusterName );
        return hash;
    }


    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof HiveConfig )
        {
            HiveConfig o = ( HiveConfig ) obj;
            return clusterName != null && clusterName.equals( o.clusterName );
        }
        return false;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", server=" + server + ", clients=" + ( clients != null ?
                                                                                                  clients.size() : 0 )
                + '}';
    }


    public String getNamenode()
    {
        return namenode;
    }


    public void setNamenode( final String namenode )
    {
        this.namenode = namenode;
    }
}
