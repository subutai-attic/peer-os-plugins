package org.safehaus.subutai.plugin.hipi.impl.handler;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;
import org.safehaus.subutai.plugin.hipi.impl.CommandFactory;
import org.safehaus.subutai.plugin.hipi.impl.HipiImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ClusterOperationHandler extends AbstractOperationHandler<HipiImpl, HipiConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Log LOG = LogFactory.getLog( ClusterOperationHandler.class );
    private ClusterOperationType operationType;
    CommandUtil commandUtil = new CommandUtil();


    public ClusterOperationHandler( final HipiImpl manager, final HipiConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );

        this.operationType = operationType;
        trackerOperation = manager.getTracker().createTrackerOperation( HipiConfig.PRODUCT_KEY,
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
            Environment env = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
            if ( env == null )
            {
                throw new ClusterException( "Environment not found: " + config.getEnvironmentId() );
            }

            Set<ContainerHost> nodes = env.getContainerHostsByIds( config.getNodes() );
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


            boolean deleted = manager.getPluginDao().deleteInfo( HipiConfig.PRODUCT_KEY, config.getClusterName() );
            if ( !deleted )
            {
                throw new ClusterException( "Failed to delete installation info" );
            }
            trackerOperation.addLogDone( "HIPI installation successfully removed" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to uninstall cluster: %s", e.getMessage() ) );
        }
    }


    @Override
    public void run()
    {
        runOperationOnContainers( this.operationType );
    }
}
