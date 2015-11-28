package io.subutai.plugin.hive.rest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.hive.api.HiveConfig;
import io.subutai.plugin.hive.rest.pojo.HivePojo;
import io.subutai.plugin.hive.rest.pojo.NodePojo;


public class RestServiceImpl implements RestService
{
    private Hive hiveManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Hive hiveManager, final Tracker tracker, final EnvironmentManager environmentManager )
    {
        Preconditions.checkNotNull( hiveManager );
        Preconditions.checkNotNull( tracker );
        Preconditions.checkNotNull( environmentManager );

        this.hiveManager = hiveManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    @Override
    public Response getClusters()
    {
        List<HiveConfig> configs = hiveManager.getClusters();
        ArrayList<String> clusterNames = Lists.newArrayList();

        for ( HiveConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        HiveConfig config = hiveManager.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + "cluster not found" )
                           .build();
        }

        String cluster = JsonUtil.GSON.toJson( parsePojo( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName, final String server,
                                    final String clients )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( server );
        Preconditions.checkNotNull( clients );

        Set<String> uuidSet = new HashSet<>();
        HiveConfig config = new HiveConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setServer( server );

        String[] arr = clients.replaceAll( "\\s+", "" ).split( "," );
        Collections.addAll( uuidSet, arr );

        config.setClients( uuidSet );

        UUID uuid = hiveManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.statusCheck( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.startNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.stopNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.addNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( hiveManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = hiveManager.uninstallNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HiveConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 90 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( HiveConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( JsonUtil.GSON.toJson( po.getLog() ) ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( po.getLog() ) ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( JsonUtil.GSON.toJson( "Timeout" ) )
                           .build();
        }
    }


    private HivePojo parsePojo( HiveConfig config )
    {
        HivePojo pojo = new HivePojo();
        try
        {
            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );

            pojo.setClusterName( config.getClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setServer( new NodePojo( config.getServer(), env ) );
            UUID uuid = hiveManager.statusCheck( config.getClusterName(), pojo.getServer().getHostname() );
            pojo.getServer().setStatus( checkStatus( tracker, uuid ) );

            Set<NodePojo> clients = new HashSet<>();
            for ( String slave : config.getClients() )
            {
                clients.add( new NodePojo( slave, env ) );
            }
            pojo.setClients( clients );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }


        return pojo;
    }


    protected String checkStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HiveConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Hive Thrift Server is not running" ) )
                    {
                        state = "STOPPED";
                    }
                    else if ( po.getLog().contains( "Hive Thrift Server is running" ) )
                    {
                        state = "RUNNING";
                    }
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
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        return state;
    }
}
