package io.subutai.plugin.hadoop.rest.pojo;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.subutai.common.settings.Common;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class HadoopPojo implements Serializable
{
    private String clusterName, domainName;
    private ContainerPojo nameNode;
    private ContainerPojo jobTracker;
    private ContainerPojo secondaryNameNode;
    private Set<ContainerPojo> dataNodes, taskTrackers;
    private Integer replicationFactor = 1, countOfSlaveNodes = 1;
    private Set<ContainerPojo> blockedAgents;
    private String environmentId;

    private String environmentDataSource;

    private boolean autoScaling;


    public HadoopPojo()
    {
        domainName = Common.DEFAULT_DOMAIN_NAME;
        dataNodes = new HashSet<>();
        taskTrackers = new HashSet<>();
        blockedAgents = new HashSet<>();
        autoScaling = false;
    }


    public HadoopPojo( HadoopClusterConfig config )
    {
        domainName = Common.DEFAULT_DOMAIN_NAME;
        dataNodes = new HashSet<>();
        taskTrackers = new HashSet<>();
        blockedAgents = new HashSet<>();
        autoScaling = false;


        clusterName = config.getClusterName();
        domainName = config.getDomainName();
        replicationFactor = config.getReplicationFactor();
        environmentId = config.getEnvironmentId();

        nameNode = new ContainerPojo( config.getNameNode(), "", "" );
        secondaryNameNode = new ContainerPojo( config.getSecondaryNameNode(), "", "" );
        jobTracker = new ContainerPojo( config.getJobTracker(), "", "" );

        for ( String uuid : config.getAllDataNodeAgent() )
        {
            dataNodes.add( new ContainerPojo( uuid, "", "" ) );
        }

        for ( String uuid : config.getAllTaskTrackerNodeAgents() )
        {
            taskTrackers.add( new ContainerPojo( uuid, "", "" ) );
        }

        for ( String uuid : config.getBlockedAgentUUIDs() )
        {
            blockedAgents.add( new ContainerPojo( uuid, "", "" ) );
        }

        countOfSlaveNodes = config.getCountOfSlaveNodes();
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public boolean isDataNode( String uuid )
    {
        return getAllDataNodeAgent().contains( uuid );
    }


    public Set<ContainerPojo> getAllDataNodeAgent()
    {
        Set<ContainerPojo> allAgents = new HashSet<>();
        for ( ContainerPojo id : getDataNodes() )
        {
            allAgents.add( id );
        }
        return allAgents;
    }


    public Set<ContainerPojo> getDataNodes()
    {
        return dataNodes;
    }


    public void setDataNodes( Set<ContainerPojo> dataNodes )
    {
        this.dataNodes = dataNodes;
    }


    public boolean isTaskTracker( String uuid )
    {
        return getAllTaskTrackerNodeAgents().contains( uuid );
    }


    public Set<ContainerPojo> getAllTaskTrackerNodeAgents()
    {
        Set<ContainerPojo> allAgents = new HashSet<>();
        for ( ContainerPojo id : getTaskTrackers() )
        {
            allAgents.add( id );
        }
        return allAgents;
    }


    public Set<ContainerPojo> getTaskTrackers()
    {
        return taskTrackers;
    }


    public void setTaskTrackers( Set<ContainerPojo> taskTrackers )
    {
        this.taskTrackers = taskTrackers;
    }


    public boolean isNameNode( String uuid )
    {
        return getNameNode().equals( uuid );
    }


    public ContainerPojo getNameNode()
    {
        return nameNode;
    }


    public void setNameNode( ContainerPojo nameNode )
    {
        this.nameNode = nameNode;
    }


    public boolean isJobTracker( String uuid )
    {
        return getJobTracker().equals( uuid );
    }


    public ContainerPojo getJobTracker()
    {
        return jobTracker;
    }


    public void setJobTracker( ContainerPojo jobTracker )
    {
        this.jobTracker = jobTracker;
    }


    public boolean isSecondaryNameNode( String id )
    {
        return getSecondaryNameNode().equals( id );
    }


    public ContainerPojo getSecondaryNameNode()
    {
        return secondaryNameNode;
    }


    public void setSecondaryNameNode( ContainerPojo secondaryNameNode )
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


    public String getEnvironmentDataSource()
    {
        return environmentDataSource;
    }


    public void setEnvironmentDataSource( String environmentDataSource )
    {
        this.environmentDataSource = environmentDataSource;
    }


    public Set<ContainerPojo> getAllNodes()
    {
        Set<ContainerPojo> allAgents = new HashSet<>();
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

        return new HashSet<>( allAgents );
    }


    public Set<ContainerPojo> getAllMasterNodes()
    {
        Preconditions.checkNotNull( nameNode, "NameNode is null" );
        Preconditions.checkNotNull( jobTracker, "JobTracker is null" );
        Preconditions.checkNotNull( secondaryNameNode, "SecondaryNameNode is null" );

        Set<ContainerPojo> allMastersNodes = new HashSet<>();
        allMastersNodes.add( nameNode );
        allMastersNodes.add( jobTracker );
        allMastersNodes.add( secondaryNameNode );
        return allMastersNodes;
    }


    public Set<ContainerPojo> getAllSlaveNodes()
    {
        Set<ContainerPojo> allAgents = new HashSet<>();
        if ( dataNodes != null )
        {
            allAgents.addAll( dataNodes );
        }
        if ( taskTrackers != null )
        {
            allAgents.addAll( taskTrackers );
        }

        return new HashSet<>( allAgents );
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


    public Set<ContainerPojo> getBlockedAgentUUIDs()
    {
        Set<ContainerPojo> blockedAgents = new HashSet<>();
        for ( ContainerPojo id : getBlockedAgents() )
        {
            blockedAgents.add( id );
        }
        return blockedAgents;
    }


    public Set<ContainerPojo> getBlockedAgents()
    {
        return blockedAgents;
    }


    public void setBlockedAgents( Set<ContainerPojo> blockedAgents )
    {
        this.blockedAgents = blockedAgents;
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

        HadoopPojo hadoopClusterConfig = ( HadoopPojo ) o;

        return !( clusterName != null ? !clusterName.equals( hadoopClusterConfig.clusterName ) :
                  hadoopClusterConfig.clusterName != null );
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
