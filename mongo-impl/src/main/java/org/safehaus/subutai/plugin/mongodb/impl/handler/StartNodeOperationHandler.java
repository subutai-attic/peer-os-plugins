package org.safehaus.subutai.plugin.mongodb.impl.handler;


import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoNode;
import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;


/**
 * Handles start mongo node operation
 */
public class StartNodeOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
{
    private final String lxcHostname;


    public StartNodeOperationHandler( MongoImpl manager, String clusterName, String lxcHostname )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.lxcHostname = lxcHostname;
        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
                String.format( "Starting node %s in %s", lxcHostname, clusterName ) );
    }


    @Override
    public void run()
    {
        MongoClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }


        trackerOperation.addLog( "Starting node..." );

        MongoNode node = config.findNode( lxcHostname );

        try
        {
            node.start( config );
            trackerOperation.addLogDone( String.format( "Node on %s started", lxcHostname ) );
        }
        catch ( Exception e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to start node %s, %s", lxcHostname, e ) );
        }
    }
}
