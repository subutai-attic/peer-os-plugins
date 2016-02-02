package io.subutai.plugin.shark.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.shark.impl.SharkImpl;
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class ClusterOperationHandler extends AbstractOperationHandler<SharkImpl, SharkClusterConfig>
        implements ClusterOperationHandlerInterface
{

    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private Environment environment;


    public ClusterOperationHandler( final SharkImpl manager, final SharkClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        Preconditions.checkNotNull( operationType );
        this.operationType = operationType;
        trackerOperation = manager.getTracker().createTrackerOperation( SharkClusterConfig.PRODUCT_KEY,
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
            case CUSTOM:
                actualizeMasterIP();
                break;
        }
    }


    private void actualizeMasterIP()
    {
        try
        {
            SparkClusterConfig sparkConfig = manager.getSparkManager().getCluster( config.getSparkClusterName() );
            if ( sparkConfig == null )
            {
                throw new ClusterException( String.format( "Spark cluster %s not found", config.getClusterName() ) );
            }
            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
            }


            Set<EnvironmentContainerHost> sharkNodes;
            try
            {
                sharkNodes = environment.getContainerHostsByIds( config.getNodeIds() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( "Failed to obtain Shark environment containers", e );
            }

            if ( sharkNodes.size() < config.getNodeIds().size() )
            {
                throw new ClusterException( "Found fewer Shark nodes in environment than exist" );
            }

            EnvironmentContainerHost sparkMaster;
            try
            {
                sparkMaster = environment.getContainerHostById( sparkConfig.getMasterNodeId() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( "Spark master not found in environment" );
            }


            for ( EnvironmentContainerHost node : sharkNodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }


            RequestBuilder actualizeMasterIpCommand = manager.getCommands().getSetMasterIPCommand( sparkMaster );


            trackerOperation.addLog( "Setting master IP..." );

            for ( EnvironmentContainerHost sharkNode : sharkNodes )
            {
                executeCommand( sharkNode, actualizeMasterIpCommand );
            }

            trackerOperation.addLogDone( "Master IP updated on all nodes" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in actualizeMasterIP", e );
            trackerOperation.addLogFailed( String.format( "Failed to actualize master IP : %s", e.getMessage() ) );
        }
    }


    @Override
    public void setupCluster()
    {

        try
        {

            SparkClusterConfig sparkConfig = manager.getSparkManager().getCluster( config.getSparkClusterName() );
            if ( sparkConfig == null )
            {
                throw new ClusterSetupException(
                        String.format( "Spark cluster %s not found", config.getClusterName() ) );
            }
            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( sparkConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterSetupException( String.format( "Could not find environment of Spark cluster by id %s",
                        sparkConfig.getEnvironmentId() ) );
            }


            //setup Shark cluster
            ClusterSetupStrategy s = manager.getClusterSetupStrategy( trackerOperation, config, environment );

            trackerOperation.addLog( "Setting up cluster..." );
            s.setup();
            trackerOperation.addLogDone( "Cluster setup completed" );
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
            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
            }

            Set<EnvironmentContainerHost> sharkNodes;
            try
            {
                sharkNodes = environment.getContainerHostsByIds( config.getNodeIds() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( "Failed to obtain Shark environment containers", e );
            }

            if ( sharkNodes.size() < config.getNodeIds().size() )
            {
                throw new ClusterException( "Found fewer Shark nodes in environment than exist" );
            }


            for ( EnvironmentContainerHost node : sharkNodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            RequestBuilder uninstallCommand = manager.getCommands().getUninstallCommand();


            trackerOperation.addLog( "Uninstalling Shark..." );

            for ( EnvironmentContainerHost sharkNode : sharkNodes )
            {
                executeCommand( sharkNode, uninstallCommand );
            }

            if ( !manager.getPluginDao().deleteInfo( SharkClusterConfig.PRODUCT_KEY, clusterName ) )
            {
                throw new ClusterException( "Could not remove cluster info" );
            }


            trackerOperation.addLogDone( "Shark uninstalled successfully" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to destroy cluster : %s", e.getMessage() ) );
        }
    }


    @Override
    public void run()
    {
        runOperationOnContainers( operationType );
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command ) throws ClusterException
    {

        CommandResult result;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            throw new ClusterException( e );
        }
        if ( !result.hasSucceeded() )
        {
            throw new ClusterException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
        return result;
    }
}
