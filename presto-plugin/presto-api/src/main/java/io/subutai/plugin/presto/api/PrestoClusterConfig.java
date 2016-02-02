package io.subutai.plugin.presto.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.plugin.common.api.ConfigBase;


public class PrestoClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Presto";
    public static final String PRODUCT_PACKAGE = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();

    private String clusterName = "";
    // over-Hadoop params
    private String hadoopClusterName = "";
    private Set<String> workers = new HashSet<>();
    private String coordinatorNode;
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


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public Set<String> getWorkers()
    {
        return workers;
    }


    public void setWorkers( Set<String> workers )
    {
        this.workers = workers;
    }


    public String getCoordinatorNode()
    {
        return coordinatorNode;
    }


    public void setCoordinatorNode( String coordinatorNode )
    {
        this.coordinatorNode = coordinatorNode;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = new HashSet<>();
        if ( workers != null )
        {
            allNodes.addAll( workers );
        }
        if ( coordinatorNode != null )
        {
            allNodes.add( coordinatorNode );
        }
        return allNodes;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", coordinatorNode=" + coordinatorNode + ", workers="
                + workers + '}';
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }
}
