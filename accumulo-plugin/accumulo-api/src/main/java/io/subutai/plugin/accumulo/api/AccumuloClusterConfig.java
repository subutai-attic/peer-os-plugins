/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.accumulo.api;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.common.api.NodeType;


public class AccumuloClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Accumulo";
    public static final String PRODUCT_NAME = "accumulo";
    public static final String PRODUCT_PACKAGE = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();
    private String clusterName = "";
    private String instanceName = "";
    private String password = "";
    private String masterNode;
    private String gcNode;
    private String monitor;
    private Set<String> tracers;
    private Set<String> slaves;
    private int numberOfTracers = 1;
    private int numberOfSlaves = 3;
    private SetupType setupType;
    private String hadoopClusterName;
    private String zookeeperClusterName;
    private String environmentId;
    private boolean autoScaling;


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( final String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public String getZookeeperClusterName()
    {
        return zookeeperClusterName;
    }


    public void setZookeeperClusterName( final String zookeeperClusterName )
    {
        this.zookeeperClusterName = zookeeperClusterName;
    }


    public SetupType getSetupType()
    {
        return setupType;
    }


    public void setSetupType( final SetupType setupType )
    {
        this.setupType = setupType;
    }


    public int getNumberOfTracers()
    {
        return numberOfTracers;
    }


    public int getNumberOfSlaves()
    {
        return numberOfSlaves;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = new HashSet<>();

        if ( masterNode != null )
        {
            allNodes.add( masterNode );
        }
        if ( gcNode != null )
        {
            allNodes.add( gcNode );
        }
        if ( monitor != null )
        {
            allNodes.add( monitor );
        }
        if ( tracers != null )
        {
            allNodes.addAll( tracers );
        }
        if ( slaves != null )
        {
            allNodes.addAll( slaves );
        }

        return allNodes;
    }


    public boolean removeNode( String nodeId )
    {
        if ( masterNode != null && Objects.equals( masterNode, nodeId ) )
        {
            return false;
        }
        if ( gcNode != null && Objects.equals( gcNode, nodeId ) )
        {
            return false;
        }
        if ( monitor != null && Objects.equals( monitor, nodeId ) )
        {
            return false;
        }
        if ( tracers != null && tracers.contains( nodeId ) )
        {
            tracers.remove( nodeId );
            return true;
        }
        if ( slaves != null && slaves.contains( nodeId ) )
        {
            slaves.remove( nodeId );
            return true;
        }
        return true;
    }


    public List<NodeType> getMasterNodeRoles( String id )
    {
        List<NodeType> roles = new ArrayList<>();
        if ( masterNode.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_MASTER );
        }
        if ( monitor.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_MONITOR );
        }
        if ( gcNode.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_GC );
        }
        return roles;
    }


    public List<NodeType> getNodeRoles( final String id )
    {
        List<NodeType> roles = new ArrayList<>();

        if ( masterNode.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_MASTER );
        }
        if ( monitor.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_MONITOR );
        }
        if ( gcNode.equals( id ) )
        {
            roles.add( NodeType.ACCUMULO_GC );
        }
        if ( tracers.contains( id ) )
        {
            roles.add( NodeType.ACCUMULO_TRACER );
        }
        if ( slaves.contains( id ) )
        {
            roles.add( NodeType.ACCUMULO_TABLET_SERVER );
        }
        return roles;
    }


    public String getMasterNode()
    {
        return masterNode;
    }


    public void setMasterNode( String masterNode )
    {
        this.masterNode = masterNode;
    }


    public String getGcNode()
    {
        return gcNode;
    }


    public void setGcNode( String gcNode )
    {
        this.gcNode = gcNode;
    }


    public String getMonitor()
    {
        return monitor;
    }


    public void setMonitor( String monitor )
    {
        this.monitor = monitor;
    }


    public Set<String> getTracers()
    {
        return tracers;
    }


    public void setTracers( Set<String> tracers )
    {
        this.tracers = tracers;
    }


    public Set<String> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( Set<String> slaves )
    {
        this.slaves = slaves;
    }


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
        return PRODUCT_NAME;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public String getInstanceName()
    {
        return instanceName;
    }


    public void setInstanceName( String instanceName )
    {
        this.instanceName = instanceName;
    }


    public String getPassword()
    {
        return password;
    }


    public void setPassword( String password )
    {
        this.password = password;
    }


    @Override
    public String toString()
    {
        return "AccumuloClusterConfig{" +
                "clusterName='" + clusterName + '\'' +
                ", instanceName='" + instanceName + '\'' +
                ", password='" + password + '\'' +
                ", masterNode=" + masterNode +
                ", gcNode=" + gcNode +
                ", monitor=" + monitor +
                ", tracers=" + tracers +
                ", slaves=" + slaves +
                ", numberOfTracers=" + numberOfTracers +
                ", numberOfSlaves=" + numberOfSlaves +
                ", setupType=" + setupType +
                ", hadoopClusterName='" + hadoopClusterName + '\'' +
                ", zookeeperClusterName='" + zookeeperClusterName + '\'' +
                '}';
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }
}
