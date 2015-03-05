package org.safehaus.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.core.env.api.exception.EnvironmentDestructionException;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.zookeeper.api.SetupType;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperOverHadoopSetupStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


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
    private String hostName;
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
        this.hostName = hostName;
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
                                         .findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) ){
                            commandResultList.add( executeCommand( containerHost, Commands.getStartCommand() ) );
                        }
                    }
                    break;
                case STOP_ALL:
                    environment = manager.getEnvironmentManager()
                                         .findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) ){
                            commandResultList.add( executeCommand( containerHost, Commands.getStopCommand() ) );
                        }
                    }
                    break;
                case STATUS_ALL:
                    environment = manager.getEnvironmentManager()
                                         .findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
                    for ( ContainerHost containerHost : environment.getContainerHosts() )
                    {
                        if ( config.getNodes().contains( containerHost.getId() ) ){
                            commandResultList.add( executeCommand( containerHost, Commands.getStatusCommand() ) );
                        }
                    }
                    break;
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Environment with id: %s not found",
                    zookeeperClusterConfig.getEnvironmentId().toString() ) );
        }
        logResults( trackerOperation, commandResultList );
    }


    @Override
    public void setupCluster()
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
            Environment env = null;

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

        // stop all nodes before removing zookeeper
        manager.stopAllNodes( clusterName );

        try
        {
            if ( config.getSetupType() == SetupType.OVER_HADOOP || config.getSetupType() == SetupType.OVER_ENVIRONMENT )
            {
                trackerOperation.addLog( "Uninstalling zookeeper from nodes" );
                Environment zookeeperEnvironment =
                        manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );

                Set<Host> hostSet = ZookeeperOverHadoopSetupStrategy.getHosts( config.getNodes(), zookeeperEnvironment );

                try
                {
                    Map<Host, CommandResult> resultMap = commandUtil
                            .executeParallel( new RequestBuilder( Commands.getUninstallCommand() ), hostSet );
                    if ( ZookeeperOverHadoopSetupStrategy.isAllSuccessful( resultMap, hostSet ) ){
                        trackerOperation.addLog( "Zookeeper is uninstalled from all containers successfully" );
                    }
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                trackerOperation.addLog( "Destroying environment..." );
                manager.getEnvironmentManager().destroyEnvironment( config.getEnvironmentId(), true, true );
            }

            manager.getPluginDAO().deleteInfo( config.getProductKey(), config.getClusterName() );
            manager.unsubscribeFromAlerts(
                    manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() ) );
            trackerOperation.addLogDone( "Cluster destroyed" );
        }
        catch ( MonitorException |
                EnvironmentDestructionException | EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Error running command, %s", e.getMessage() ) );
            LOG.error( e.getMessage(), e );
        }
    }
}
