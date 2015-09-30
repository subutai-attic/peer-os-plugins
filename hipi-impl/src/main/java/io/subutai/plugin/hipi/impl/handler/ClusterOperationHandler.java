package io.subutai.plugin.hipi.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.impl.CommandFactory;
import io.subutai.plugin.hipi.impl.HipiImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<HipiImpl, HipiConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    CommandUtil commandUtil = new CommandUtil();
    private ClusterOperationType operationType;


    public ClusterOperationHandler( final HipiImpl manager, final HipiConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );

        this.operationType = operationType;
        trackerOperation = manager.getTracker().createTrackerOperation( HipiConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on cluster %s", operationType.name(), clusterName ) );
    }


    @Override
    public void run()
    {
        runOperationOnContainers( this.operationType );
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
            case STOP_ALL:
            case STATUS_ALL:
                LOG.warn( "Unsupported operation type: " + operationType );
                break;
        }
    }


    @Override
    public void setupCluster()
    {
        try
        {

            ClusterSetupStrategy s = manager.getClusterSetupStrategy( config, trackerOperation );

            trackerOperation.addLog( "Installing Hipi nodes..." );
            s.setup();
            trackerOperation.addLogDone( "Installing successfully completed" );
        }
        catch ( ClusterSetupException e )
        {
            String msg = "Installation failed\n" + e.getMessage();
            LOG.error( msg, e );
            trackerOperation.addLogFailed( msg );
        }
    }


    @Override
    public void destroyCluster()
    {
        try
        {
            if ( manager.getCluster( clusterName ) == null )
            {
                throw new ClusterException( "Hipi installation not found: " + clusterName );
            }
            Environment env;
            try
            {
                env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException( "Environment not found: " + config.getEnvironmentId() );
            }


            Set<ContainerHost> nodes;
            try
            {
                nodes = env.getContainerHostsByIds( config.getNodes() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( String.format( "Environment containers not found: %s", e ) );
            }
            for ( ContainerHost node : nodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            trackerOperation.addLog( "Uninstalling HIPI..." );


            RequestBuilder rb = new RequestBuilder( CommandFactory.build( NodeOperationType.UNINSTALL ) );
            for ( ContainerHost node : nodes )
            {
                try
                {
                    commandUtil.execute( rb, node );

                    trackerOperation.addLog( "HIPI uninstalled from " + node.getHostname() );
                }
                catch ( CommandException e )
                {
                    //ignore failed uninstallation and log it
                    trackerOperation.addLog(
                            String.format( "Failed to uninstall HIPI on node %s: %s", node.getHostname(), e ) );
                }
            }


            manager.deleteConfig( config );

            trackerOperation.addLogDone( "HIPI installation successfully removed" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to uninstall cluster: %s", e.getMessage() ) );
        }
    }
}
