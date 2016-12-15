package io.subutai.plugin.hbase.api;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.util.UUIDUtil;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.core.plugincommon.api.NodeType;


public class HBaseConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "HBase";
    public static final String PRODUCT_NAME = "HBase";
    private String id;
    private String clusterName = "";
    private String hbaseMaster;
    private Set<String> regionServers = Sets.newHashSet();
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private Set<String> hadoopNodes = new HashSet<>();
    private String environmentId;
    private String vlan;
    private String peerId;
    private String hadoopClusterName;
    private String hadoopNameNode;
    private boolean autoScaling;


    public HBaseConfig()
    {
        this.id = UUIDUtil.generateTimeBasedUUID().toString();
        autoScaling = false;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( final String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public String getHadoopNameNode()
    {
        return hadoopNameNode;
    }


    public void setHadoopNameNode( final String hadoopNameNode )
    {
        this.hadoopNameNode = hadoopNameNode;
    }


    public Set<String> getHadoopNodes()
    {
        return hadoopNodes;
    }


    public void setHadoopNodes( final Set<String> hadoopNodes )
    {
        this.hadoopNodes = hadoopNodes;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public String getId()
    {
        return id;
    }


    public void setId( String id )
    {
        this.id = id;
    }


    public void reset()
    {
        this.hbaseMaster = null;
        this.regionServers = null;
        this.domainName = "";
        this.clusterName = "";
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
        return PRODUCT_KEY;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public Set<String> getAllNodes()
    {
        final Set<String> allNodes = new HashSet<>();
        allNodes.add( getHbaseMaster() );
        allNodes.addAll( getRegionServers() );
        return allNodes;
    }


    public String getHbaseMaster()
    {
        return hbaseMaster;
    }


    public void setHbaseMaster( String hbaseMaster )
    {
        this.hbaseMaster = hbaseMaster;
    }


    public Set<String> getRegionServers()
    {
        return regionServers;
    }


    public void setRegionServers( Set<String> regionServers )
    {
        this.regionServers = regionServers;
    }


    public List<NodeType> getNodeRoles( final ContainerHost containerHost )
    {
        List<NodeType> nodeRoles = new ArrayList<>();

        if ( hbaseMaster.equals( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.HMASTER );
        }
        if ( regionServers.contains( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.HREGIONSERVER );
        }
        return nodeRoles;
    }


    public String getVlan()
    {
        return vlan;
    }


    public void setVlan( final String vlan )
    {
        this.vlan = vlan;
    }


    public String getPeerId()
    {
        return peerId;
    }


    public void setPeerId( final String peerId )
    {
        this.peerId = peerId;
    }
}
