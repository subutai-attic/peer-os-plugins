package io.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.core.environment.api.exception.EnvironmentDestructionException;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.ClusterConfiguration;
import io.subutai.plugin.zookeeper.impl.Commands;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import io.subutai.plugin.zookeeper.impl.ZookeeperOverHadoopSetupStrategy;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ZookeeperClusterOperationHandler
        extends AbstractPluginOperationHandler<ZookeeperImpl, ZookeeperClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ZookeeperClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private ZookeeperClusterConfig zookeeperClusterConfig;
    private CommandUtil commandUtil;


    public ZookeeperClusterOperationHandler( final ZookeeperImpl manager, final ZookeeperClusterConfig config,
                                             final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.zookeeperClusterConfig = config;
        trackerOperation = manager.getTracker().createTrackerOperation( config.getProductKey(),
                String.format( "Running %s operation on %s...", operationType, clusterName ) );
        this.commandUtil = new CommandUtil();
    }


    public ZookeeperClusterOperationHandler( final ZookeeperImpl manager,
                                             final ZookeeperClusterConfig zookeeperClusterConfig, final String hostName,
                                             final ClusterOperationType operationType )
    {
        super( manager, zookeeperClusterConfig );
        this.operationType = operationType;
        this.zookeeperClusterConfig = zookeeperClusterConfig;
        trackerOperation = manager.getTracker().createTrackerOperation( zookeeperClusterConfig.getProductKey(),
                String.format( "Running %s operation on %s...", operationType, clusterName ) );
        this.commandUtil = new CommandUtil();
    }


    public void run()
    {
        Preconditions.checkNotNull( zookeeperClusterConfig, "Configuration is null !!!" );
        runOperationOnContainers( operationType );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        Environment environment;
        List<CommandResult> commandResultList = new ArrayList<>();
        try
        {
            switch ( clusterOperationType )
            {
                case INSTALL:
                    setupCluster();
                    break;
                case UNINSTALL:
                    destroyCluster();
                    break;
                case START_ALL:
                    environment = manager.getEnvironmentManager()
                                         .loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) )
                        {
                            commandResultList.add( executeCommand( containerHost, Commands.getStartCommand() ) );
                            commandResultList
                                    .add( executeCommand( containerHost, Commands.getStartZkServerCommand() ) );
                        }
                    }
                    break;
                case STOP_ALL:
                    environment = manager.getEnvironmentManager()
                                         .loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) )
                        {
                            commandResultList.add( executeCommand( containerHost, Commands.getStopCommand() ) );
                            commandResultList.add( executeCommand( containerHost, Commands.getStopZkServerCommand() ) );
                        }
                    }
                    break;
                case STATUS_ALL:
                    environment = manager.getEnvironmentManager()
                                         .loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) )
                        {
                            commandResultList
                                    .add( executeCommand( containerHost, Commands.getStatusZkServerCommand() ) );
                        }
                    }
                    break;
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Environment with id: %s not found", zookeeperClusterConfig.getEnvironmentId() ) );
        }
        logResults( trackerOperation, commandResultList );
    }


    @Override
    public void setupCluster()
    {
        try
        {
            if ( Strings.isNullOrEmpty( zookeeperClusterConfig.getClusterName() ) )
            {
                trackerOperation.addLogFailed( "Malformed configuration" );
                return;
            }

            if ( manager.getCluster( clusterName ) != null )
            {
                trackerOperation.addLogFailed( String.format( "Cluster with name '%s' already exists", clusterName ) );
                return;
            }

            try
            {
                Environment env;
                try
                {
                    env = manager.getEnvironmentManager().loadEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                }
                catch ( EnvironmentNotFoundException e )
                {
                    throw new ClusterException( String.format( "Could not find environment of Hadoop cluster by id %s",
                            zookeeperClusterConfig.getEnvironmentId() ) );
                }

                ClusterSetupStrategy clusterSetupStrategy =
                        manager.getClusterSetupStrategy( env, zookeeperClusterConfig, trackerOperation );
                clusterSetupStrategy.setup();

                trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
            }
            catch ( ClusterSetupException e )
            {
                trackerOperation.addLogFailed(
                        String.format( "Failed to setup %s cluster %s : %s", zookeeperClusterConfig.getProductKey(),
                                clusterName, e.getMessage() ) );
                LOG.error( String.format( "Failed to setup %s cluster %s : %s", zookeeperClusterConfig.getProductKey(),
                        clusterName, e.getMessage() ), e );
            }
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in setupCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to setup cluster : %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        ZookeeperClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            new ClusterConfiguration( manager, trackerOperation )
                    .deleteConfiguration( zookeeperClusterConfig, environment );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Error during deleting cluster configuration, %s", e.getMessage() ) );
            LOG.error( String.format( "Error during deleting cluster configuration, %s", e.getMessage() ) );
        }
        catch ( ClusterConfigurationException e )
        {
            trackerOperation.addLogFailed( String.format( "Error during reconfiguration after removing cluster: %s",
                    config.getClusterName() ) );
            LOG.error(
                    String.format( "Error during reconfiguration after removing cluster: %s", config.getClusterName() ),
                    e );
        }

        manager.getPluginDAO().deleteInfo( config.getProductKey(), config.getClusterName() );
        trackerOperation.addLogDone( "Cluster destroyed" );
    }
}
