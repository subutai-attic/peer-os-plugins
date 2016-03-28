package io.subutai.plugin.flume.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


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
        //install flume,
        String s = Commands.make( CommandType.INSTALL );
        try
        {
            for ( ContainerHost node : environment.getContainerHostsByIds( config.getNodes() ) )
            {
                try
                {
                    CommandResult result = node.execute( new RequestBuilder( s ).withTimeout( 600 ) );
                    checkInstalled( node, result );
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

        po.addLog( "Updating db..." );
        //save to db
        config.setEnvironmentId( environment.getId() );
        manager.getPluginDao().saveInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster info saved to DB\nInstalling Flume..." );
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
            environment = manager.getEnvironmentManager().loadEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + hc.getEnvironmentId(), e );
            return;
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        po.addLog( "Checking prerequisites..." );

        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.make( CommandType.STATUS ) );
        for ( String nodeId : config.getNodes() )
        {
            ContainerHost node ;
            try
            {
                node = environment.getContainerHostById( nodeId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                po.addLogFailed( "Container host not found" );
                return;
            }
            try
            {
                node.execute( Commands.getAptUpdate() );
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


    public void checkInstalled( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( new RequestBuilder( Commands.make( CommandType.STATUS ) ) );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( FlumeConfig.PACKAGE_NAME ) ) )
        {
            po.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
