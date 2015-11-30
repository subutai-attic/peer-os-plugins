package io.subutai.plugin.shark.rest;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.shark.api.Shark;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.shark.rest.pojo.ContainerPojo;
import io.subutai.plugin.shark.rest.pojo.SharkPojo;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class RestServiceImpl implements RestService
{

    private Shark sharkManager;
    private Spark sparkManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Shark sharkManager )
    {
        this.sharkManager = sharkManager;
    }


    @Override
    public Response listClusters()
    {

        List<SharkClusterConfig> configs = sharkManager.getClusters();
        Set<String> clusterNames = Sets.newHashSet();

        for ( SharkClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response installCluster( final String clusterName, final String sparkClusterName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( sparkClusterName );

        SharkClusterConfig config = new SharkClusterConfig();
        SparkClusterConfig sparkConfig = sparkManager.getCluster( sparkClusterName );
        config.getNodeIds().addAll( sparkConfig.getAllNodesIds() );
        config.setClusterName( clusterName );
        config.setSparkClusterName( sparkClusterName );

        UUID uuid = sharkManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response getCluster( String clusterName )
    {
        SharkClusterConfig config = sharkManager.getCluster( clusterName );

        String cluster = JsonUtil.GSON.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response uninstallCluster( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sharkManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( String clusterName, String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sharkManager.addNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( String clusterName, String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sharkManager.destroyNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response actualizeMasterIP( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sharkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sharkManager.actualizeMasterIP( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        SharkClusterConfig config = sharkManager.getCluster( clusterName );
        SparkClusterConfig sparkInfo = sparkManager.getCluster( config.getSparkClusterName () );
        Environment environment = null;
        try
        {
            environment = environmentManager.loadEnvironment( sparkInfo.getEnvironmentId() );
            Set<String> nodeIds = new HashSet<>( sparkInfo.getAllNodesIds() );
            nodeIds.removeAll( config.getNodeIds() );

            Set<EnvironmentContainerHost> availableNodes = Sets.newHashSet();
            availableNodes.addAll( environment.getContainerHostsByIds( nodeIds ) );

            for ( final EnvironmentContainerHost availableNode : availableNodes )
            {
                hostsName.add( availableNode.getHostname() );
            }
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private SharkPojo updateConfig( SharkClusterConfig config )
    {
        SharkPojo pojo = new SharkPojo();
        Set<ContainerPojo> nodes = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setSparkClusterName( config.getSparkClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getNodeIds() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                nodes.add( new ContainerPojo( ch.getHostname(), uuid, ch.getIpByInterfaceName( "eth0" ) ) );
            }
            pojo.setNodes( nodes );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( SharkClusterConfig.PRODUCT_KEY, uuid );
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


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( SharkClusterConfig.PRODUCT_KEY, uuid );
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setSparkManager( final Spark sparkManager )
    {
        this.sparkManager = sparkManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}