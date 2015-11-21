package io.subutai.plugin.cassandra.rest;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.FormParam;
import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.gson.reflect.TypeToken;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.Cassandra;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.common.api.ClusterException;


public class RestServiceImpl implements RestService
{
    private Cassandra cassandraManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    @Override
    public Response listClusters()
    {
        List<CassandraClusterConfig> configs = cassandraManager.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( CassandraClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        CassandraClusterConfig config = cassandraManager.getCluster( clusterName );

        boolean thrownException = false;
        if ( config == null )
        {
            thrownException = true;
        }


        Map< String, ContainerInfoJson > map = new HashMap<>(  );

        for( String node : config.getNodes() )
        {
            try
            {
                ContainerInfoJson containerInfoJson = new ContainerInfoJson();

                Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
                EnvironmentContainerHost containerHost = environment.getContainerHostById( node );

                String ip = containerHost.getIpByInterfaceName( "eth0" );
                containerInfoJson.setIp( ip );

                UUID uuid = cassandraManager.checkNode( clusterName, node );
                OperationState state = waitUntilOperationFinish( uuid );
                Response response = createResponse( uuid, state );
                if( response.getStatus() == 200 && response.getEntity().toString().toUpperCase().contains( "RUN" ) )
                {
                    containerInfoJson.setStatus( "RUNNING" );
                }
                else
                {
                    containerInfoJson.setStatus( "STOPPED" );
                }

                map.put( node, containerInfoJson );
            }
            catch ( Exception e )
            {
                map.put( node, new ContainerInfoJson() );
                thrownException = true;
            }
        }

        ClusterConfJson result = new ClusterConfJson();
        result.setSeeds( config.getSeedNodes() );
        result.setContainers( config.getNodes() );
        result.setContainersStatuses( map );
        result.setName( config.getClusterName() );
        result.setScaling( config.isAutoScaling() );

        String cluster = JsonUtil.toJson( config );




        if( thrownException )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( clusterName + " cluster not found." ).build();
        }
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.uninstallCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response removeCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.removeCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response configureCluster( final String environmentId, final String clusterName, final String nodes,
                                      final String seeds )
    {
        Preconditions.checkNotNull( environmentId );
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodes );
        Preconditions.checkNotNull( seeds );
        Environment environment = null;
        try
        {
            environment = environmentManager.loadEnvironment( environmentId );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( environment == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "Could not find environment with id : " + environmentId ).build();
        }

        if ( cassandraManager.getCluster( clusterName ) != null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( "There is already a cluster with same name !" ).build();
        }

        CassandraClusterConfig config = new CassandraClusterConfig();
        config.setEnvironmentId( environmentId );
        config.setClusterName( clusterName );
        Set<String> allNodes = new HashSet<>();
        Set<String> allSeeds = new HashSet<>();
        String[] configNodes = nodes.replaceAll( "\\s+", "" ).split( "," );
        String[] configSeeds = seeds.replaceAll( "\\s+", "" ).split( "," );
        Collections.addAll( allNodes, configNodes );
        Collections.addAll( allSeeds, configSeeds );
        config.setNodes( allNodes );
        config.setSeedNodes( allSeeds );


        UUID uuid = cassandraManager.installCluster( config );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.startCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.stopCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.checkCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        CassandraClusterConfig config = cassandraManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            cassandraManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }


        return Response.ok().build();
    }


    @Override
    public Response addNode( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.addNode( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String hostId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostId );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.destroyNode( clusterName, hostId );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String hostId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostId );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = cassandraManager.checkNode( clusterName, hostId );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxchostId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxchostId );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found" ).build();
        }
        UUID uuid = cassandraManager.startService( clusterName, lxchostId );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNodes( @FormParam( "clusterName" ) final String clusterName,
                                @FormParam( "lxcHosts" ) final String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );

        List<String> hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>(){}.getType() );

        if( hosts == null || hosts.isEmpty() )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
        }

        int errors = 0;

        for( String host : hosts )
        {
            UUID uuid = cassandraManager.startService( clusterName, host );
            OperationState state = waitUntilOperationFinish( uuid );
            Response response =createResponse( uuid, state );

            if( response.getStatus() != 200 )
            {
                errors++;
            }
        }

        if( errors > 0 )
        {
            return Response.status( Response.Status.EXPECTATION_FAILED ).entity( errors + " nodes are failed to execute" ).build();
        }

        return Response.ok().build();
    }


    @Override
    public Response stopNodes( @FormParam( "clusterName" ) final String clusterName,
                               @FormParam( "lxcHosts" ) final String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );

        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );

        List<String> hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>(){}.getType() );

        if( hosts == null || hosts.isEmpty() )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
        }

        int errors = 0;

        for( String host : hosts )
        {
            UUID uuid = cassandraManager.stopService( clusterName, host );
            OperationState state = waitUntilOperationFinish( uuid );
            Response response =createResponse( uuid, state );

            if( response.getStatus() != 200 )
            {
                errors++;
            }
        }

        if( errors > 0 )
        {
            return Response.status( Response.Status.EXPECTATION_FAILED ).entity( errors + " nodes are failed to execute" ).build();
        }

        return Response.ok().build();
    }


    @Override
    public Response stopNode( final String clusterName, final String lxchostId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxchostId );
        if ( cassandraManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found" ).build();
        }
        UUID uuid = cassandraManager.stopService( clusterName, lxchostId );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private Response createResponse( UUID uuid, OperationState state )
    {

        TrackerOperationView po = tracker.getTrackerOperation( CassandraClusterConfig.PRODUCT_NAME, uuid );


        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.ok( po.getLog() ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    private String wrapUUID( UUID uuid )
    {
        return JsonUtil.toJson( "OPERATION_ID", uuid );
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( CassandraClusterConfig.PRODUCT_NAME, uuid );
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Cassandra getCassandraManager()
    {
        return cassandraManager;
    }


    public void setCassandraManager( final Cassandra cassandraManager )
    {
        this.cassandraManager = cassandraManager;
    }

    public Response installCluster( String config )
    {
        ClusterConfJson clusterConfJson = JsonUtil.fromJson( config, ClusterConfJson.class );

        CassandraClusterConfig clusterConfig = new CassandraClusterConfig( );

        clusterConfig.setClusterName( clusterConfJson.getName() );
        clusterConfig.setDomainName( clusterConfJson.getDomainName() );
        clusterConfig.setDataDirectory( clusterConfJson.getDataDir() );
        clusterConfig.setCommitLogDirectory( clusterConfJson.getCommitDir() );
        clusterConfig.setSavedCachesDirectory( clusterConfJson.getCacheDir() );
        clusterConfig.setNodes( clusterConfJson.getContainers() );
        clusterConfig.setSeedNodes( clusterConfJson.getSeeds() );

        clusterConfig.setNumberOfNodes( clusterConfJson.getContainers().size() );
        clusterConfig.setNumberOfSeeds( clusterConfJson.getSeeds().size() );
        clusterConfig.setEnvironmentId( clusterConfJson.getEnvironmentId() );

        cassandraManager.installCluster( clusterConfig );
        return Response.ok().build();
    }
}
