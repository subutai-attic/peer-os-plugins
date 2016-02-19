package io.subutai.plugin.pig.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.ConfigBase;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.pig.api.PigConfig;


class PigSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( PigSetupStrategy.class.getName() );
    final PigImpl manager;
    final PigConfig config;
    final TrackerOperation trackerOperation;
    private Environment environment;
    CommandUtil commandUtil = new CommandUtil();


    public PigSetupStrategy( PigImpl manager, PigConfig config, TrackerOperation trackerOperation )
    {
        this.manager = manager;
        this.config = config;
        this.trackerOperation = trackerOperation;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();
        return config;
    }


    private void check() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) || Strings.isNullOrEmpty( config.getHadoopClusterName() )
                || CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Malformed configuration\nInstallation aborted" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists\nInstallation aborted",
                            config.getClusterName() ) );
        }
        //check hadoop cluster
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
            throw new ClusterSetupException( "Could not find Hadoop cluster environment" );
        }

        //      check nodes are connected
        Set<EnvironmentContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            trackerOperation.addLogFailed( "Container hosts not found" );
            return;
        }
        for ( EnvironmentContainerHost host : nodes )
        {
            if ( !host.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Container %s is not connected", host.getHostname() ) );
            }
        }

        trackerOperation.addLog( "Checking prerequisites..." );

        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.checkCommand );
        for ( String nodeId : config.getNodes() )
        {
            EnvironmentContainerHost node;
            try
            {
                node = environment.getContainerHostById( nodeId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                trackerOperation.addLogFailed( "Container host not found" );
                return;
            }
            try
            {
                CommandResult result = commandUtil.execute( checkInstalledCommand, node );
                if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Pig installed. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
                else if ( !result.getStdOut()
                                 .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s has no Hadoop installation. Omitting this node from installation",
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


    private void configure() throws ClusterSetupException
    {
        //install pig
        try
        {
            for ( EnvironmentContainerHost node : environment.getContainerHostsByIds( config.getNodes() ) )
            {
                try
                {
                    CommandResult result = commandUtil
                            .execute( new RequestBuilder( Commands.installCommand ).withTimeout( 1000 ), node );
                    checkInstalled( node, result );
                }
                catch ( CommandException e )
                {
                    throw new ClusterSetupException(
                            String.format( "Error while installing Pig on container %s; %s", node.getHostname(),
                                    e.getMessage() ) );
                }
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            trackerOperation.addLogFailed( "Container hosts not found" );
        }

        trackerOperation.addLog( "Updating db..." );
        //save to db
        config.setEnvironmentId( environment.getId() );
        try
        {
            manager.saveConfig( config );
            trackerOperation.addLog( "Cluster info saved to DB\nInstalling Pig..." );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( e );
        }
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = commandUtil.execute( new RequestBuilder( Commands.checkCommand ), host );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( PigConfig.PRODUCT_PACKAGE ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
