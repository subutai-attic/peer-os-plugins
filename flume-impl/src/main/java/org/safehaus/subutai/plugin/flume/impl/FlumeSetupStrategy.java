package org.safehaus.subutai.plugin.flume.impl;


import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.flume.api.FlumeConfig;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


class FlumeSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( FlumeSetupStrategy.class.getName() );
    final FlumeImpl manager;
    final FlumeConfig config;
    final TrackerOperation po;

    private Environment environment;


    public FlumeSetupStrategy( FlumeImpl manager, FlumeConfig config, TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.po = po;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();
        return config;
    }


    private void configure() throws ClusterSetupException
    {
        po.addLog( "Updating db..." );
        //save to db
        config.setEnvironmentId( environment.getId() );
        manager.getPluginDao().saveInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster info saved to DB\nInstalling Flume..." );
        //install pig,
        String s = Commands.make( CommandType.INSTALL );
        try
        {
            for ( ContainerHost node : environment.getContainerHostsByIds( config.getNodes() ) )
            {
                try
                {
                    CommandResult result = node.execute( new RequestBuilder( s ).withTimeout( 600 ) );
                    processResult( node, result );
                }
                catch ( CommandException e )
                {
                    throw new ClusterSetupException(
                            String.format( "Error while installing Flume on container %s; %s", node.getHostname(),
                                    e.getMessage() ) );
                }
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            po.addLogFailed( "Container host not found" );
        }
    }


    private void check() throws ClusterSetupException
    {
        String m = "Invalid configuration: ";

        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            throw new ClusterSetupException( m + "Cluster name not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    m + String.format( "Cluster '%s' already exists", config.getClusterName() ) );
        }


        if ( CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( m + "Target nodes not specified" );
        }
        HadoopClusterConfig hc = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }
        if ( !hc.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }

        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + hc.getEnvironmentId().toString(), e );
            return;
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        po.addLog( "Checking prerequisites..." );

        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.make( CommandType.STATUS ) );
        for ( UUID uuid : config.getNodes() )
        {
            ContainerHost node = null;
            try
            {
                node = environment.getContainerHostById( uuid );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                po.addLogFailed( "Container host not found" );
            }
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    po.addLog(
                            String.format( "Node %s already has Flume installed. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
            }
        }
        if ( config.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
        }
    }


    public void processResult( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {

        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
