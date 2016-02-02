package io.subutai.plugin.hbase.impl.handler;


import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.hbase.api.HBaseConfig;
import io.subutai.plugin.hbase.impl.Commands;
import io.subutai.plugin.hbase.impl.HBaseImpl;
import io.subutai.plugin.hbase.impl.HBaseSetupStrategy;


public class ClusterOperationHandler extends AbstractOperationHandler<HBaseImpl, HBaseConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private Environment environment;
    private HBaseConfig config;
    private CommandUtil commandUtil;


    public ClusterOperationHandler( final HBaseImpl manager, final HBaseConfig hbaseConfig,
                                    final ClusterOperationType operationType )
    {
        this( manager, hbaseConfig );
        Preconditions.checkNotNull( hbaseConfig );
        this.operationType = operationType;
        this.commandUtil = new CommandUtil();
        try
        {
            this.environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
            return;
        }
        this.trackerOperation = manager.getTracker().createTrackerOperation( HBaseConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on cluster %s", operationType.name(), clusterName ) );
    }


    public ClusterOperationHandler( final HBaseImpl manager, final HBaseConfig baseClusterConfig )
    {
        super( manager, baseClusterConfig );
        Preconditions.checkNotNull( baseClusterConfig );
        this.config = baseClusterConfig;
    }


    @Override
    public void run()
    {
        runOperationOnContainers( operationType );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
                startCluster();
                break;
            case STOP_ALL:
                stopCluster();
                break;
        }
    }


    private void stopCluster()
    {
        try
        {
            EnvironmentContainerHost hmaster = environment.getContainerHostById( config.getHbaseMaster() );
            CommandResult result = hmaster.execute( Commands.getStopCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }
            else
            {
                trackerOperation.addLogFailed( result.getStdErr() );
            }
            trackerOperation.addLogDone( "Stop cluster command executed" );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( e.getMessage() );
            LOG.error( e.getMessage(), e );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }
    }


    private void startCluster()
    {
        try
        {
            EnvironmentContainerHost hmaster = environment.getContainerHostById( config.getHbaseMaster() );
            // start hadoop before starting hbase cluster
            manager.getHadoopManager()
                   .startNameNode( manager.getHadoopManager().getCluster( config.getHadoopClusterName() ) );
            CommandResult result = hmaster.execute( Commands.getStartCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }
            else
            {
                trackerOperation.addLogFailed( result.getStdErr() );
            }
            trackerOperation.addLogDone( "Start cluster command executed" );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( e.getMessage() );
            LOG.error( e.getMessage(), e );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }
    }


    @Override
    public void setupCluster()
    {
        try
        {
            //setup HBase cluster
            trackerOperation.addLog( "Installing cluster..." );
            HBaseSetupStrategy strategy =
                    new HBaseSetupStrategy( manager, manager.getHadoopManager(), config, environment,
                            trackerOperation );

            strategy.setup();
            trackerOperation.addLogDone( "Installing cluster completed" );
        }
        catch ( ClusterSetupException e )
        {
            LOG.error( "Error in setupCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to setup cluster : %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        try
        {
            Set<EnvironmentContainerHost> hbaseNodes = environment.getContainerHostsByIds( config.getAllNodes() );

            if ( hbaseNodes.size() < config.getAllNodes().size() )
            {
                throw new ClusterException( "Found fewer HBase nodes in environment than exist" );
            }

            for ( EnvironmentContainerHost node : hbaseNodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            // stop hbase cluster before removing hbase debian package
            manager.stopCluster( clusterName );

            try
            {
                Set<Host> hostSet = HBaseSetupStrategy.getHosts( config, environment );
                Map<Host, CommandResult> resultMap = commandUtil.executeParallel( Commands.getUninstallCommand(),
                        HBaseSetupStrategy.getHosts( config, environment ) );
                if ( isAllSuccessful( resultMap, hostSet ) )
                {
                    trackerOperation.addLog( "HBase package is removed from all nodes succesfully" );
                }
            }
            catch ( CommandException e )
            {
                LOG.error( "Error while uninstalling HBase from nodes", e );
                e.printStackTrace();
            }

            if ( !manager.getPluginDAO().deleteInfo( HBaseConfig.PRODUCT_KEY, clusterName ) )
            {
                throw new ClusterException( "Could not remove cluster info" );
            }

            trackerOperation.addLogDone( "HBase uninstalled successfully" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to destroy cluster : %s", e.getMessage() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
        }
    }


    public static boolean isAllSuccessful( Map<Host, CommandResult> resultMap, Set<Host> hosts )
    {
        boolean allSuccess = true;
        for ( Host host : hosts )
        {
            if ( !resultMap.get( host ).hasSucceeded() )
            {
                allSuccess = false;
            }
        }
        return allSuccess;
    }
}
