package org.safehaus.subutai.plugin.hadoop.impl;


import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationInterface;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );
    private TrackerOperation po;
    private HadoopImpl hadoopManager;


    public ClusterConfiguration( final TrackerOperation operation, final HadoopImpl cassandraManager )
    {
        this.po = operation;
        this.hadoopManager = cassandraManager;
    }


    public void configureCluster( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {


        HadoopClusterConfig config = ( HadoopClusterConfig ) configBase;
        Commands commands = new Commands( config );

        ContainerHost namenode;
        try
        {
            namenode = environment.getContainerHostById( config.getNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting container host for name node.", e );
            po.addLogFailed( "Error getting container host for name node." );
            throw new ClusterConfigurationException( e );
        }
        ContainerHost jobtracker;
        try
        {
            jobtracker = environment.getContainerHostById( config.getJobTracker() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting container host for job tracker.", e );
            po.addLogFailed( "Error getting container host for name node." );
            throw new ClusterConfigurationException( e );
        }
        ContainerHost secondaryNameNode;
        try
        {
            secondaryNameNode = environment.getContainerHostById( config.getSecondaryNameNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Error getting secondary container host", e );
            po.addLogFailed( "Error getting secondary container host" );
            throw new ClusterConfigurationException( e );
        }
        po.addLog( String.format( "Configuring cluster: %s", configBase.getClusterName() ) );

        // Clear configuration files
        for ( ContainerHost containerHost : environment.getContainerHosts() )
        {
            executeCommandOnContainer( containerHost, Commands.getClearMastersCommand() );
            executeCommandOnContainer( containerHost, Commands.getClearSlavesCommand() );
        }

        // Configure NameNode
        for ( ContainerHost containerHost : environment.getContainerHosts() )
        {
            executeCommandOnContainer( containerHost,
                    commands.getSetMastersCommand( namenode.getHostname(), jobtracker.getHostname() ) );
        }

        // Configure JobTracker
        executeCommandOnContainer( jobtracker, Commands.getConfigureJobTrackerCommand( jobtracker.getHostname() ) );


        // Configure Secondary NameNode
        executeCommandOnContainer( namenode,
                Commands.getConfigureSecondaryNameNodeCommand( secondaryNameNode.getHostname() ) );


        // Configure DataNodes
        for ( UUID uuid : config.getDataNodes() )
        {
            try
            {
                executeCommandOnContainer( namenode, Commands.getConfigureDataNodesCommand(
                        environment.getContainerHostById( uuid ).getHostname() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error executing command", e );
            }
        }

        // Configure TaskTrackers
        for ( UUID uuid : config.getTaskTrackers() )
        {
            try
            {
                executeCommandOnContainer( jobtracker, Commands.getConfigureTaskTrackersCommand(
                        environment.getContainerHostById( uuid ).getHostname() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error configuring task tracker.", e );
            }
        }

        // Format NameNode
        executeCommandOnContainer( namenode, Commands.getFormatNameNodeCommand() );


        // Start Hadoop cluster
//        executeCommandOnContainer( namenode, Commands.getStartNameNodeCommand() );
//        executeCommandOnContainer( jobtracker, Commands.getStartJobTrackerCommand() );


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hadoopManager.getPluginDAO()
                     .saveInfo( HadoopClusterConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "Hadoop cluster data saved into database" );
    }


    private void executeCommandOnContainer( ContainerHost containerHost, String command )
    {
        try
        {
            containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }
}
