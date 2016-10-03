package io.subutai.plugin.accumulo.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.OperationType;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;


public class NodeOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );

    private String hostId;
    private OperationType operationType;
    private NodeType nodeType;
    private Environment environment;
    private EnvironmentContainerHost node;


    public NodeOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config, final String hostId,
                                 final OperationType operationType, final NodeType nodeType )
    {
        super( manager, config );
        this.hostId = hostId;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on node %s", operationType.name(), hostId ) );
    }


    @Override
    public void run()
    {
        try
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
                throw new ClusterException(
                        String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
            }

            try
            {
                node = environment.getContainerHostById( hostId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Node not found in environment by id %s", node.getHostname() ) );
            }


            if ( !node.isConnected() )
            {
                throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
            }


            switch ( operationType )
            {
                case START:
                    startNode();
                    break;
                case STOP:
                    stopNode();
                    break;
                case STATUS:
                    checkNode();
                    break;
                case INSTALL:
                    addNode();
                    break;
                case UNINSTALL:
                    removeNode();
                    break;
            }

            trackerOperation.addLogDone( "" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in NodeOperationHandler", e );
            trackerOperation
                    .addLogFailed( String.format( "Operation %s failed: %s", operationType.name(), e.getMessage() ) );
        }
    }


    private void removeNode()
    {
    }


    private void addNode()
    {
    }


    private void checkNode()
    {
    }


    private void stopNode()
    {
    }


    private void startNode()
    {
    }
}
