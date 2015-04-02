package org.safehaus.subutai.plugin.storm.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.storm.api.Storm;
import org.safehaus.subutai.plugin.storm.api.StormClusterConfiguration;
import org.safehaus.subutai.plugin.zookeeper.api.Zookeeper;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


public class RestServiceImpl implements RestService
{
    private static final Logger LOGGER = LoggerFactory.getLogger( RestServiceImpl.class );
    private static final String OPERATION_ID = "OPERATION_ID";
    private UUID nimbusID;
    private Storm stormManager;
    private Zookeeper zookeeperManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    public Tracker getTracker()
    {
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


    public void setStormManager( Storm stormManager )
    {
        this.stormManager = stormManager;
    }


    public Zookeeper getZookeeperManager()
    {
        return zookeeperManager;
    }


    public void setZookeeperManager( final Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }


    public Response getClusters()
    {

        List<StormClusterConfiguration> configs = stormManager.getClusters();
        ArrayList<String> clusterNames = Lists.newArrayList();

        for ( StormClusterConfiguration config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    public Response installCluster( String clusterName, String environmentId, boolean externalZookeeper,
                                    String zookeeperClusterName, String nimbus, String supervisors )
    {
        Set<UUID> uuidSet = new HashSet<>();
        StormClusterConfiguration config = new StormClusterConfiguration();
        config.setClusterName( clusterName );
        config.setExternalZookeeper( externalZookeeper );
        config.setZookeeperClusterName( zookeeperClusterName );
        config.setEnvironmentId( UUID.fromString( environmentId ) );

        if ( externalZookeeper )
        {
            ZookeeperClusterConfig zookeeperClusterConfig =
                    zookeeperManager.getCluster( config.getZookeeperClusterName() );
            Environment zookeeperEnvironment = null;
            try
            {
                zookeeperEnvironment = environmentManager.findEnvironment( zookeeperClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment with id not found.", e );
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "" ).build();
            }
            try
            {
                nimbusID = zookeeperEnvironment.getContainerHostByHostname( nimbus ).getId();
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
            config.setNimbus( nimbusID );
        }
        else
        {
            Environment stormEnvironment = null;
            try
            {
                stormEnvironment = environmentManager.findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOGGER.error( "Environment with id not found.", e );
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "" ).build();
            }
            nimbusID = UUID.fromString( nimbus );
            config.setNimbus( nimbusID );
        }

        String[] arr = supervisors.replaceAll( "\\s+", "" ).split( "," );
        for ( String client : arr )
        {
            uuidSet.add( UUID.fromString( client ) );
        }

        config.setSupervisors( uuidSet );

        UUID uuid = stormManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response uninstallCluster( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response getCluster( String clusterName )
    {
        StormClusterConfiguration config = stormManager.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + "cluster not found" )
                           .build();
        }

        String clusterInfo = JsonUtil.GSON.toJson( config );
        return Response.status( Response.Status.OK ).entity( clusterInfo ).build();
    }


    public Response statusCheck( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = stormManager.checkNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response startNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.startNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response stopNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.stopNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response addNode( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.addNode( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response destroyNode( String clusterName, String hostname )
    {
        UUID uuid = stormManager.destroyNode( clusterName, hostname );

        String operationId = JsonUtil.toJson( OPERATION_ID, uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response checkCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = stormManager.checkAll( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = stormManager.startAll( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = stormManager.stopAll( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message ="enabled";
        StormClusterConfiguration config = stormManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            stormManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
        if( scale == false )
        {
            message = "disabled";
        }

        return Response.status( Response.Status.OK ).entity( "Auto scale is "+ message+" successfully" ).build();
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( StormClusterConfiguration.PRODUCT_KEY, uuid );
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
        TrackerOperationView po = tracker.getTrackerOperation( StormClusterConfiguration.PRODUCT_KEY, uuid );
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
}
