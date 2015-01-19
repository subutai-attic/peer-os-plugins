package org.safehaus.subutai.plugin.hbase.api;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.util.UUIDUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.common.api.NodeType;

import com.google.common.collect.Sets;


public class HBaseConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "HBase";
    public static final String TEMPLATE_NAME = "HBase";
    private UUID uuid;
    private String clusterName = "";
    private UUID hbaseMaster;
    private Set<UUID> regionServers = Sets.newHashSet();
    private Set<UUID> quorumPeers = Sets.newHashSet();
    private Set<UUID> backupMasters = Sets.newHashSet();
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private Set<UUID> hadoopNodes;
    private SetupType setupType;
    private UUID environmentId;
    private String hadoopClusterName;
    private UUID hadoopNameNode;
    private boolean autoScaling;


    public HBaseConfig()
    {
        this.uuid = UUID.fromString( UUIDUtil.generateTimeBasedUUID().toString() );
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


    public UUID getHadoopNameNode()
    {
        return hadoopNameNode;
    }


    public void setHadoopNameNode( final UUID hadoopNameNode )
    {
        this.hadoopNameNode = hadoopNameNode;
    }


    public Set<UUID> getHadoopNodes()
    {
        return hadoopNodes;
    }


    public void setHadoopNodes( final Set<UUID> hadoopNodes )
    {
        this.hadoopNodes = hadoopNodes;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }


    public SetupType getSetupType()
    {
        return setupType;
    }


    public void setSetupType( final SetupType setupType )
    {
        this.setupType = setupType;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public UUID getUuid()
    {
        return uuid;
    }


    public void setUuid( UUID uuid )
    {
        this.uuid = uuid;
    }


    public void reset()
    {
        this.hbaseMaster = null;
        this.regionServers = null;
        this.quorumPeers = null;
        this.backupMasters = null;
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


    public Set<UUID> getAllNodes()
    {
        final Set<UUID> allNodes = new HashSet<>();
        allNodes.add( getHbaseMaster() );
        allNodes.addAll( getRegionServers() );
        allNodes.addAll( getQuorumPeers() );
        allNodes.addAll( getBackupMasters() );
        return allNodes;
    }

    public List<NodeType> getNodeRoles( HBaseConfig clusterConfig, final ContainerHost containerHost )
    {
        List<NodeType> nodeRoles = new ArrayList<>();

        if ( hbaseMaster.equals ( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.HMASTER );
        }
        if ( regionServers.contains( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.HREGIONSERVER );
        }
        if ( quorumPeers.contains( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.HQUORUMPEER );
        }
        return nodeRoles;
    }

    public UUID getHbaseMaster()
    {
        return hbaseMaster;
    }


    public void setHbaseMaster( UUID hbaseMaster )
    {
        this.hbaseMaster = hbaseMaster;
    }


    public Set<UUID> getRegionServers()
    {
        return regionServers;
    }


    public void setRegionServers( Set<UUID> regionServers )
    {
        this.regionServers = regionServers;
    }


    public Set<UUID> getQuorumPeers()
    {
        return quorumPeers;
    }


    public void setQuorumPeers( Set<UUID> quorumPeers )
    {
        this.quorumPeers = quorumPeers;
    }


    public Set<UUID> getBackupMasters()
    {
        return backupMasters;
    }


    public void setBackupMasters( Set<UUID> backupMasters )
    {
        this.backupMasters = backupMasters;
    }
}
