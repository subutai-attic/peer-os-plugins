package io.subutai.plugin.hadoop.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.common.api.ConfigBase;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


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

        EnvironmentContainerHost namenode;
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
        EnvironmentContainerHost jobtracker;
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
        EnvironmentContainerHost secondaryNameNode;
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


        // Configure NameNode & JobTracker and replication factor on all cluser nodes
        for ( String id : config.getAllNodes() )
        {
            try
            {
                EnvironmentContainerHost containerHost = environment.getContainerHostById( id );
                if ( containerHost.getId().equals( namenode.getId() ) || containerHost.getId()
                                                                                      .equals( jobtracker.getId() ) )
                {
                    executeCommandOnContainer( containerHost, Commands.getClearMastersCommand() );
                    executeCommandOnContainer( containerHost, Commands.getClearSlavesCommand() );
                }
                executeCommandOnContainer( containerHost,
                        commands.getSetMastersCommand( namenode.getHostname(), jobtracker.getHostname() ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }

        // Configure Secondary NameNode
        executeCommandOnContainer( namenode,
                Commands.getConfigureSecondaryNameNodeCommand( secondaryNameNode.getHostname() ) );


        // Configure DataNodes & TaskTrackers
        for ( String id : config.getDataNodes() )
        {
            try
            {
                executeCommandOnContainer( namenode,
                        Commands.getConfigureSlaveNodes( environment.getContainerHostById( id ).getHostname() ) );

                if ( !namenode.getId().equals( jobtracker.getId() ) )
                {
                    executeCommandOnContainer( jobtracker,
                            Commands.getConfigureSlaveNodes( environment.getContainerHostById( id ).getHostname() ) );
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Error executing command", e );
            }
        }

        // Format NameNode
        executeCommandOnContainer( namenode, Commands.getFormatNameNodeCommand() );


        // Start Hadoop cluster
        // executeCommandOnContainer( namenode, Commands.getStartNameNodeCommand() );
        // executeCommandOnContainer( jobtracker, Commands.getStartJobTrackerCommand() );


        po.addLog( "Configuration is finished !" );

        config.setEnvironmentId( environment.getId() );
        hadoopManager.getPluginDAO()
                     .saveInfo( HadoopClusterConfig.PRODUCT_KEY, configBase.getClusterName(), configBase );
        po.addLogDone( "Hadoop cluster data saved into database" );

        //subscribe to alerts
        try
        {
            hadoopManager.subscribeToAlerts( environment );
        }
        catch ( MonitorException e )
        {
            throw new ClusterConfigurationException( e );
        }
    }


    private void executeCommandOnContainer( EnvironmentContainerHost containerHost, String command )
    {
        try
        {
            containerHost.execute( new RequestBuilder( command ).withTimeout( 10 ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error while executing \"" + command + "\"." );
            e.printStackTrace();
        }
    }
}
