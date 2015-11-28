package io.subutai.plugin.oozie.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

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
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.api.SetupType;
import io.subutai.plugin.oozie.rest.pojo.ContainerPojo;
import io.subutai.plugin.oozie.rest.pojo.OoziePojo;


public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Oozie oozieManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Oozie oozieManager )
    {
        Preconditions.checkNotNull( oozieManager );

        this.oozieManager = oozieManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    @Override
    public Response getClusters()
    {
        List<OozieClusterConfig> configs = oozieManager.getClusters();
        ArrayList<String> clusterNames = Lists.newArrayList();

        for ( OozieClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( String clusterName )
    {
        OozieClusterConfig config = oozieManager.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + "cluster not found" )
                           .build();
        }

        String cluster = JsonUtil.GSON.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( String clusterName, String hadoopClusterName, String server, String clients )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( server );
        Preconditions.checkNotNull( clients);

		Set<String> uuidSet = Sets.newHashSet ();
        OozieClusterConfig config = new OozieClusterConfig();
        config.setSetupType( SetupType.OVER_HADOOP );
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setServer( server );

        List<String> hosts = JsonUtil.fromJson( clients, new TypeToken<List<String>>()
        {
        }.getType() );

        for ( final String host : hosts )
        {
        	uuidSet.add (host);
        }
//		config.getClients().add( host );
		config.setClients (uuidSet);

        UUID uuid = oozieManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = oozieManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = oozieManager.checkNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = oozieManager.startNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = oozieManager.stopNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = oozieManager.addNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostName );
        if ( oozieManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = oozieManager.destroyNode( clusterName, hostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    private OoziePojo updateConfig( OozieClusterConfig config )
    {
        OoziePojo pojo = new OoziePojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getClients() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                containerPojoSet.add( new ContainerPojo( ch.getHostname(), ch.getIpByInterfaceName( "eth0" ) ) );
            }

            pojo.setClients( containerPojoSet );

            ContainerHost ch = environment.getContainerHostById( config.getServer() );
            UUID uuid = oozieManager.checkNode( config.getClusterName(), ch.getHostname() );
            pojo.setServer( new ContainerPojo( ch.getHostname(), ch.getIpByInterfaceName( "eth0" ),
                    checkStatus( tracker, uuid ) ) );

        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        OozieClusterConfig oozieClusterConfig = oozieManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( oozieClusterConfig.getHadoopClusterName() );

        Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
        nodes.removeAll( oozieClusterConfig.getAllNodes() );
        if ( !nodes.isEmpty() )
        {
            Set<EnvironmentContainerHost> hosts;
            try
            {
                hosts = environmentManager.loadEnvironment( hadoopConfig.getEnvironmentId() )
                                          .getContainerHostsByIds( nodes );

                for ( final EnvironmentContainerHost host : hosts )
                {
                    hostsName.add( host.getHostname() );
                }
            }
            catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        else
        {
            LOG.info( "All nodes in corresponding Hadoop cluster have Nutch installed" );
            return Response.status( Response.Status.NOT_FOUND ).build();
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private String checkStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( OozieClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Oozie Server is not running" ) )
                    {
                        state = "STOPPED";
                    }
                    else if ( po.getLog().contains( "Oozie Server is running" ) )
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


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( OozieClusterConfig.PRODUCT_KEY, uuid );
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


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( OozieClusterConfig.PRODUCT_KEY, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).build();
        }
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}