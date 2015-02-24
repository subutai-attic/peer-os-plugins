package org.safehaus.subutai.plugin.hadoop.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class RestServiceImpl implements RestService
{

    private static final String OPERATION_ID = "OPERATION_ID";
    private Hadoop hadoopManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    @Override
    public Response listClusters()
    {
        List<HadoopClusterConfig> hadoopClusterConfigList = hadoopManager.getClusters();
        ArrayList<String> clusterNames = new ArrayList<>();

        for ( HadoopClusterConfig hadoopClusterConfig : hadoopClusterConfigList )
        {
            clusterNames.add( hadoopClusterConfig.getClusterName() );
        }


        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( String clusterName )
    {
        String cluster = JsonUtil.GSON.toJson( hadoopManager.getCluster( clusterName ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( String clusterName, int numberOfSlaveNodes, int numberOfReplicas )
    {
        HadoopClusterConfig hadoopClusterConfig = new HadoopClusterConfig();
        hadoopClusterConfig.setClusterName( clusterName );
        hadoopClusterConfig.setCountOfSlaveNodes( numberOfSlaveNodes );
        hadoopClusterConfig.setReplicationFactor( numberOfReplicas );

        UUID uuid = hadoopManager.installCluster( hadoopClusterConfig );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( String clusterName )
    {
        UUID uuid = hadoopManager.uninstallCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );    }


    @Override
    public Response startNameNode( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.startNameNode( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNameNode( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.stopNameNode( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );    }


    @Override
    public Response statusNameNode( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.statusNameNode( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response statusSecondaryNameNode( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.statusSecondaryNameNode( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startJobTracker( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.startJobTracker( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopJobTracker( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.stopJobTracker( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response statusJobTracker( String clusterName )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.statusJobTracker( hadoopClusterConfig );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( String clusterName )
    {
        UUID uuid = hadoopManager.addNode( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response statusDataNode( String clusterName, String hostname )
    {
        UUID uuid = hadoopManager.statusDataNode( hadoopManager.getCluster( clusterName ), hostname );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response statusTaskTracker( String clusterName, String hostname )
    {
        HadoopClusterConfig hadoopClusterConfig = hadoopManager.getCluster( clusterName );
        UUID uuid = hadoopManager.statusTaskTracker( hadoopClusterConfig, hostname );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }

    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public Tracker getTracker(){
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    private Response createResponse( UUID uuid, OperationState state ){
        TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
        if ( state == OperationState.FAILED ){
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED ){
            return Response.status( Response.Status.OK ).entity( po.getLog() ).build();
        }
        else {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    private OperationState waitUntilOperationFinish( UUID uuid ){
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    state = po.getState();
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 60 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }
}
