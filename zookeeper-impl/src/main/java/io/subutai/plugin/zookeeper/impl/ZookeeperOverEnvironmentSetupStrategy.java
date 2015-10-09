package io.subutai.plugin.zookeeper.impl;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


public class ZookeeperOverEnvironmentSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperOverEnvironmentSetupStrategy.class );
    private final ZookeeperClusterConfig zookeeperClusterConfig;
    private final ZookeeperImpl manager;
    private final TrackerOperation po;
    private Environment environment;


    public ZookeeperOverEnvironmentSetupStrategy( final Environment environment,
                                                  final ZookeeperClusterConfig zookeeperClusterConfig,
                                                  final TrackerOperation po, final ZookeeperImpl zookeeperManager )
    {
        this.zookeeperClusterConfig = zookeeperClusterConfig;
        this.manager = zookeeperManager;
        this.po = po;
        this.environment = environment;
    }


    @Override
    public ZookeeperClusterConfig setup() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( zookeeperClusterConfig.getClusterName() ) ||
                zookeeperClusterConfig.getNodes() == null || zookeeperClusterConfig.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( manager.getCluster( zookeeperClusterConfig.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", zookeeperClusterConfig.getClusterName() ) );
        }

        if ( zookeeperClusterConfig.getSetupType() == SetupType.OVER_ENVIRONMENT )
        {
            try
            {
                environment =
                        manager.getEnvironmentManager().loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Error getting environment for id: " + zookeeperClusterConfig.getEnvironmentId(), e );
            }
        }
        Set<EnvironmentContainerHost> zookeeperNodes;
        try
        {
            zookeeperNodes = environment.getContainerHostsByIds( zookeeperClusterConfig.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error( "Some container hosts not found.", e );
            throw new ClusterSetupException( e.getMessage() );
        }

        //check if node agent is connected
        for ( ContainerHost node : zookeeperNodes )
        {
            try
            {
                if ( environment.getContainerHostByHostname( node.getHostname() ) == null )
                {
                    throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterSetupException( String.format( "Node %s not found", node.getHostname() ) );
            }
        }

        po.addLog( "Checking prerequisites..." );


        //check installed subutai packages
        String checkInstalledCommand = Commands.getStatusCommand();
        List<CommandResult> commandResultList = runCommandOnContainers( checkInstalledCommand, zookeeperNodes );
        if ( getFailedCommandResults( commandResultList ).size() != 0 )
        {
            throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
        }

        Iterator<EnvironmentContainerHost> iterator = zookeeperNodes.iterator();
        int nodeIndex = 0;
        while ( iterator.hasNext() )
        {
            ContainerHost host = iterator.next();
            CommandResult result = commandResultList.get( nodeIndex++ );

            if ( result.getStdOut().contains( "Zookeeper Server is running" ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s already has Zookeeper installed", host.getHostname() ) );
            }
        }

        po.addLog( String.format( "Installing Zookeeper..." ) );

        //install
        try
        {
            String installCommand = Commands.getStatusCommand();
            for ( final ContainerHost zookeeperNode : zookeeperNodes )
            {
                try
                {
                    CommandResult result =
                            zookeeperNode.execute( new RequestBuilder( installCommand ).withTimeout( 30 ) );
                    if ( result.getStdErr().contains( "unrecognized service" ) )
                    {
                        zookeeperNode.execute( new RequestBuilder( Commands.getInstallCommand() ).withTimeout( 120 ) );
                    }
                    po.addLog( String.format( "Installed zookeeper package on %s", zookeeperNode.getHostname() ) );
                }
                catch ( CommandException e )
                {
                    po.addLogFailed( "Couldn't install zookeeper on container host: " + zookeeperNode.getHostname() );
                    LOGGER.error( "Couldn't install zookeeper on container host: " + zookeeperNode.getHostname(), e );
                    throw new ClusterSetupException( e );
                }
            }


            po.addLog( "Installation succeeded\nConfiguring cluster..." );

            new ClusterConfiguration( manager, po ).configureCluster( zookeeperClusterConfig, environment );

            po.addLog( "Saving cluster information to database..." );

            zookeeperClusterConfig.setEnvironmentId( environment.getId() );

            manager.getPluginDAO()
                   .saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, zookeeperClusterConfig.getClusterName(),
                           zookeeperClusterConfig );
            for ( final ContainerHost zookeeperNode : zookeeperNodes )
            {
                manager.subscribeToAlerts( zookeeperNode );
            }
            po.addLog( "Cluster information saved to database" );
        }
        catch ( MonitorException | ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }

        return zookeeperClusterConfig;
    }


    private List<CommandResult> runCommandOnContainers( String command,
                                                        final Set<EnvironmentContainerHost> zookeeperNodes )
    {
        List<CommandResult> commandResults = new ArrayList<>();
        for ( ContainerHost containerHost : zookeeperNodes )
        {
            try
            {
                commandResults.add( containerHost.execute( new RequestBuilder( command ).withTimeout( 1800 ) ) );
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }
        return commandResults;
    }


    public List<CommandResult> getFailedCommandResults( final List<CommandResult> commandResultList )
    {
        List<CommandResult> failedCommands = new ArrayList<>();
        for ( CommandResult commandResult : commandResultList )
        {
            if ( !commandResult.hasSucceeded() )
            {
                failedCommands.add( commandResult );
            }
        }
        return failedCommands;
    }
}
