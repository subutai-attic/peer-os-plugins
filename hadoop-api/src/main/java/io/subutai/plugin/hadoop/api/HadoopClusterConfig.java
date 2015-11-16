package io.subutai.plugin.hadoop.api;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Topology;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.common.api.NodeType;


public class HadoopClusterConfig implements ConfigBase
{
    public static final String PRODUCT_KEY = "Hadoop";
    public static final int DEFAULT_HADOOP_MASTER_NODES_QUANTITY = 3;
    public static final String PRODUCT_NAME = PRODUCT_KEY.toLowerCase();
    public static final String TEMPLATE_NAME = PRODUCT_NAME;
    public static final int NAME_NODE_PORT = 8020, JOB_TRACKER_PORT = 9000;

    private String clusterName, domainName;
    private String nameNode, jobTracker, secondaryNameNode;
    private List<String> dataNodes, taskTrackers;
    private Integer replicationFactor = 1, countOfSlaveNodes = 1;
    private Set<String> blockedAgents;
    private String environmentId;
    private boolean autoScaling;
//    private Topology topology;


    public HadoopClusterConfig()
    {
        domainName = Common.DEFAULT_DOMAIN_NAME;
        dataNodes = new ArrayList<>();
        taskTrackers = new ArrayList<>();
        blockedAgents = new HashSet<>();
//        topology = new Topology();
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
        if ( clusterConfig.isSecondaryNameNode( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.SECONDARY_NAMENODE );
        }
        if ( clusterConfig.isJobTracker( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.JOBTRACKER );
        }
        if ( clusterConfig.isDataNode( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.DATANODE );
        }
        if ( clusterConfig.isTaskTracker( containerHost.getId() ) )
        {
            nodeRoles.add( NodeType.TASKTRACKER );
        }

        return nodeRoles;
    }


    public boolean isDataNode( String uuid )
    {
        return getAllDataNodeAgent().contains( uuid );
    }


    public Set<String> getAllDataNodeAgent()
    {
        Set<String> allAgents = new HashSet<>();
        for ( String id : getDataNodes() )
        {
            allAgents.add( id );
        }
        return allAgents;
    }


    public List<String> getDataNodes()
    {
        return dataNodes;
    }


    public void setDataNodes( List<String> dataNodes )
    {
        this.dataNodes = dataNodes;
    }


    public boolean isTaskTracker( String uuid )
    {
        return getAllTaskTrackerNodeAgents().contains( uuid );
    }


    public Set<String> getAllTaskTrackerNodeAgents()
    {
        Set<String> allAgents = new HashSet<>();
        for ( String id : getTaskTrackers() )
        {
            allAgents.add( id );
        }
        return allAgents;
    }


    public List<String> getTaskTrackers()
    {
        return taskTrackers;
    }


    public void setTaskTrackers( List<String> taskTrackers )
    {
        this.taskTrackers = taskTrackers;
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


    public boolean isJobTracker( String uuid )
    {
        return getJobTracker().equals( uuid );
    }


    public String getJobTracker()
    {
        return jobTracker;
    }


    public void setJobTracker( String jobTracker )
    {
        this.jobTracker = jobTracker;
    }


    public boolean isSecondaryNameNode( String id )
    {
        return getSecondaryNameNode().equals( id );
    }


    public String getSecondaryNameNode()
    {
        return secondaryNameNode;
    }


    public void setSecondaryNameNode( String secondaryNameNode )
    {
        this.secondaryNameNode = secondaryNameNode;
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


    public List<String> getAllNodes()
    {
        Set<String> allAgents = new HashSet<>();
        if ( dataNodes != null )
        {
            allAgents.addAll( dataNodes );
        }
        if ( taskTrackers != null )
        {
            allAgents.addAll( taskTrackers );
        }

        if ( nameNode != null )
        {
            allAgents.add( nameNode );
        }
        if ( jobTracker != null )
        {
            allAgents.add( jobTracker );
        }
        if ( secondaryNameNode != null )
        {
            allAgents.add( secondaryNameNode );
        }

        return new ArrayList<>( allAgents );
    }


    public Set<String> getAllMasterNodesAgents()
    {
        Set<String> allAgents = new HashSet<>();
        for ( String id : getAllMasterNodes() )
        {
            allAgents.add( id );
        }
        return allAgents;
    }


    public Set<String> getAllMasterNodes()
    {
        Preconditions.checkNotNull( nameNode, "NameNode is null" );
        Preconditions.checkNotNull( jobTracker, "JobTracker is null" );
        Preconditions.checkNotNull( secondaryNameNode, "SecondaryNameNode is null" );
        Set<String> allMastersNodes = new HashSet<>();
        allMastersNodes.add( nameNode );
        allMastersNodes.add( jobTracker );
        allMastersNodes.add( secondaryNameNode );
        return allMastersNodes;
    }


    public Set<String> getAllSlaveNodesAgents()
    {
        Set<String> allAgents = new HashSet<>();
        for ( String uuid : getAllSlaveNodes() )
        {
            allAgents.add( uuid );
        }
        return allAgents;
    }


    public List<String> getAllSlaveNodes()
    {
        Set<String> allAgents = new HashSet<>();
        if ( dataNodes != null )
        {
            allAgents.addAll( dataNodes );
        }
        if ( taskTrackers != null )
        {
            allAgents.addAll( taskTrackers );
        }

        return new ArrayList<>( allAgents );
    }


    public void removeNode( String agent )
    {
        if ( dataNodes.contains( agent ) )
        {
            dataNodes.remove( agent );
        }
        if ( taskTrackers.contains( agent ) )
        {
            taskTrackers.remove( agent );
        }
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


    public Integer getReplicationFactor()
    {
        return replicationFactor;
    }


    public void setReplicationFactor( Integer replicationFactor )
    {
        this.replicationFactor = replicationFactor;
    }


    public Integer getCountOfSlaveNodes()
    {
        return countOfSlaveNodes;
    }


    public void setCountOfSlaveNodes( Integer countOfSlaveNodes )
    {
        this.countOfSlaveNodes = countOfSlaveNodes;
    }


    public Set<String> getBlockedAgentUUIDs()
    {
        Set<String> blockedAgents = new HashSet<>();
        for ( String id : getBlockedAgents() )
        {
            blockedAgents.add( id );
        }
        return blockedAgents;
    }


    public Set<String> getBlockedAgents()
    {
        return blockedAgents;
    }


    public void setBlockedAgents( Set<String> blockedAgents )
    {
        this.blockedAgents = blockedAgents;
    }

//
//    public Topology getTopology()
//    {
//        return topology;
//    }
//
//
//    public void setTopology( final Topology topology )
//    {
//        this.topology = topology;
//    }
//

    public boolean isMasterNode( EnvironmentContainerHost containerHost )
    {
        return containerHost.getId().equals( getNameNode() ) ||
                containerHost.getId().equals( getJobTracker() ) ||
                containerHost.getId().equals( getSecondaryNameNode() );
    }


    public boolean isSlaveNode( EnvironmentContainerHost containerHost )
    {
        return dataNodes.contains( containerHost.getId() ) || taskTrackers.contains( containerHost.getId() );
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
                ", jobTracker=" + jobTracker +
                ", secondaryNameNode=" + secondaryNameNode +
                ", dataNodes=" + dataNodes +
                ", taskTrackers=" + taskTrackers +
                ", replicationFactor=" + replicationFactor +
                ", countOfSlaveNodes=" + countOfSlaveNodes +
                '}';
    }
}
