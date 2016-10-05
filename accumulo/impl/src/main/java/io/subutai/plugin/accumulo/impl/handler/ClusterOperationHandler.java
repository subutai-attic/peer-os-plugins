package io.subutai.plugin.accumulo.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.ClusterConfiguration;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class ClusterOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private Environment environment;
    private EnvironmentContainerHost master;


    public ClusterOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on cluster %s", operationType.name(), clusterName ) );
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
        }
    }


    @Override
    public void setupCluster()
    {
        try
        {
            HadoopClusterConfig hadoopConfig = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );

            if ( hadoopConfig == null )
            {
                throw new ClusterException(
                        String.format( "Could not find Hadoop cluster %s", config.getHadoopClusterName() ) );
            }

            Environment env;
            try
            {
                env = manager.getEnvironmentManager().loadEnvironment( hadoopConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException( String.format( "Could not find environment of Hadoop cluster by id %s",
                        hadoopConfig.getEnvironmentId() ) );
            }


            ClusterSetupStrategy s = manager.getClusterSetupStrategy( trackerOperation, config, env );
            try
            {
                trackerOperation.addLog( "Setting up cluster..." );
                s.setup();
                trackerOperation.addLogDone( "Cluster setup completed" );
            }
            catch ( ClusterSetupException e )
            {
                throw new ClusterException( "Failed to setup cluster: " + e.getMessage() );
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
        try
        {
            checkPrerequisites();

            Set<EnvironmentContainerHost> allNodes = environment.getContainerHostsByIds( config.getAllNodes() );
            EnvironmentContainerHost namenode = environment.getContainerHostById( config.getMaster() );

            trackerOperation.addLog( "Uninstalling Accumulo..." );

            namenode.execute( Commands.getDeleteHdfsFolderCommand() );

            for ( EnvironmentContainerHost node : allNodes )
            {
                node.execute( Commands.getStopZkServerCommand() );
                node.execute( Commands.getStopAllCommand() );
                node.execute( Commands.getUninstallZkCommand() );
                CommandResult result = node.execute( Commands.getUninstallAccumuloCommand() );
                if ( !result.hasSucceeded() )
                {
                    throw new ClusterException(
                            String.format( "Could not uninstall Accumulo from node %s : %s", node.getHostname(),
                                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
                }
            }

            if ( !manager.getPluginDAO().deleteInfo( AccumuloClusterConfig.PRODUCT_KEY, clusterName ) )
            {
                throw new ClusterException( "Could not remove cluster info" );
            }

            trackerOperation.addLogDone( "Cluster uninstalled successfully" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to uninstall cluster: %s", e.getMessage() ) );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    @Override
    public void run()
    {
        runOperationOnContainers( operationType );
    }


    public void checkPrerequisites() throws ClusterException
    {
        if ( manager.getCluster( clusterName ) == null )
        {
            throw new ClusterException( String.format( "Cluster with name %s does not exist", clusterName ) );
        }


        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            throw new ClusterException( String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
        }

        try
        {
            master = environment.getContainerHostById( config.getMaster() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterException(
                    String.format( "Master node not found in environment by id %s", config.getMaster() ) );
        }

        if ( !master.isConnected() )
        {
            throw new ClusterException( "Master node is not connected" );
        }
    }
}
