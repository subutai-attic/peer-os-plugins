package io.subutai.plugin.mysql.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;


public class RestServiceImpl implements RestService
{

    private MySQLC mysql;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final MySQLC mysql )
    {
        this.mysql = mysql;
    }


    @Override
    public Response listClusters()
    {
        List<MySQLClusterConfig> configs = mysql.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( MySQLClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        MySQLClusterConfig config = mysql.getCluster( clusterName );
        if ( config == null )
        {
            return clusterNotFound( clusterName );
        }
        return Response.status( Response.Status.OK ).entity( JsonUtil.toJson( config ) ).build();
    }


    @Override
    public Response configureCluster( final String config )
    {
        MySQLClusterConfig mySQLClusterConfig = JsonUtil.fromJson( config, MySQLClusterConfig.class );
        UUID uuid = mysql.installCluster( mySQLClusterConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private Response createResponse( final UUID uuid, final OperationState state )
    {

        TrackerOperationView po = tracker.getTrackerOperation( MySQLClusterConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( po.getLog() ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {

        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        UUID uuid = mysql.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        NodeType type = getType( nodeType );

        UUID uuid = mysql.startNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        NodeType type = getType( nodeType );

        UUID uuid = mysql.stopNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        UUID uuid = mysql.startCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        UUID uuid = mysql.stopCluster( clusterName );
        return createResponse( uuid, waitUntilOperationFinish( uuid ) );
    }


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        NodeType type = getType( nodeType );

        UUID uuid = mysql.destroyNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname, final String nodeType )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        NodeType type = getType( nodeType );

        UUID uuid = mysql.checkNode( clusterName, lxcHostname, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String nodeType )
    {
        if ( mysql.getCluster( clusterName ) == null )
        {
            return clusterNotFound( clusterName );
        }
        NodeType type = getType( nodeType );

        UUID uuid = mysql.addNode( clusterName, type );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MySQLClusterConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 200 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response clusterNotFound( final String clusterName )
    {
        return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + " not found" ).build();
    }


    private NodeType getType( String type )
    {
        NodeType nodeType = null;

        if ( type.contains( "master_node" ) )
        {
            nodeType = NodeType.MASTER_NODE;
        }
        else if ( type.contains( "data" ) )
        {
            nodeType = NodeType.DATANODE;
        }
        else if ( type.contains( "client" ) )
        {
            nodeType = NodeType.CLIENT;
        }

        return nodeType;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
