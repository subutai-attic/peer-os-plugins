package io.subutai.plugin.hadoop.api;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.core.plugincommon.api.NodeType;


public class HadoopClusterConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Hadoop";
    public static final String PRODUCT_NAME = PRODUCT_KEY.toLowerCase();
    public static final String TEMPLATE_NAME = "hadoop27";

    private String clusterName, domainName;
    private String nameNode;
    private Set<String> slaves;
    private Set<String> excludedSlaves;
    private String replicationFactor;
    private String environmentId;
    private boolean autoScaling;


    public HadoopClusterConfig()
    {
        domainName = Common.DEFAULT_DOMAIN_NAME;
        autoScaling = false;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public static List<NodeType> getNodeRoles( HadoopClusterConfig clusterConfig,
                                               final EnvironmentContainerHost containerHost )
    {
        List<NodeType> nodeRoles = new ArrayList<>();

        if ( clusterConfig.isNameNode( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.NAMENODE );
        }

        return nodeRoles;
    }


    public boolean isNameNode( String uuid )
    {
        return getNameNode().equals( uuid );
    }


    public String getNameNode()
    {
        return nameNode;
    }


    public void setNameNode( String nameNode )
    {
        this.nameNode = nameNode;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getTemplateName()
    {
        return TEMPLATE_NAME;
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


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( String domainName )
    {
        this.domainName = domainName;
    }


    public String getReplicationFactor()
    {
        return replicationFactor;
    }


    public void setReplicationFactor( String replicationFactor )
    {
        this.replicationFactor = replicationFactor;
    }


    public List<String> getAllNodes()
    {
        List<String> allNodes = Lists.newArrayList();

        allNodes.add( getNameNode() );
        allNodes.addAll( getSlaves() );

        return allNodes;
    }


    @Override
    public int hashCode()
    {
        return clusterName != null ? clusterName.hashCode() : 0;
    }


    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        HadoopClusterConfig hadoopClusterConfig = ( HadoopClusterConfig ) o;

        if ( clusterName != null ? !clusterName.equals( hadoopClusterConfig.clusterName ) :
             hadoopClusterConfig.clusterName != null )
        {
            return false;
        }

        return true;
    }


    @Override
    public String toString()
    {
        return "Config{" +
                "clusterName='" + clusterName + '\'' +
                ", domainName='" + domainName + '\'' +
                ", nameNode=" + nameNode +
                ", replicationFactor=" + replicationFactor +
                '}';
    }


    public Set<String> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( final Set<String> slaves )
    {
        this.slaves = slaves;
    }


    public Set<String> getExcludedSlaves()
    {
        return excludedSlaves;
    }


    public void setExcludedSlaves( final Set<String> excludedSlaves )
    {
        this.excludedSlaves = excludedSlaves;
    }
}
