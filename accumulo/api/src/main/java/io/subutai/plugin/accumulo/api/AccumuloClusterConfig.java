package io.subutai.plugin.accumulo.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.core.plugincommon.api.ConfigBase;


public class AccumuloClusterConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Accumulo";
    private String clusterName = "";
    private String hadoopClusterName = "";
    private String master;
    private Set<String> slaves = new HashSet<>();
    private String environmentId;
    private String password;


    @Override
    public String getClusterName()
    {
        return clusterName;
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


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( final String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public String getMaster()
    {
        return master;
    }


    public void setMaster( final String master )
    {
        this.master = master;
    }


    public Set<String> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( final Set<String> slaves )
    {
        this.slaves = slaves;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = new HashSet<>();
        if ( slaves != null )
        {
            allNodes.addAll( slaves );
        }
        if ( master != null )
        {
            allNodes.add( master );
        }
        return allNodes;
    }


    public String getPassword()
    {
        return password;
    }


    public void setPassword( final String password )
    {
        this.password = password;
    }
}
