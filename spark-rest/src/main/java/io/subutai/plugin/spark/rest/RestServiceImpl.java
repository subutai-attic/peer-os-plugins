package io.subutai.plugin.spark.rest;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.spark.api.Spark;
import io.subutai.plugin.spark.api.SparkClusterConfig;
import io.subutai.plugin.spark.rest.pojo.NodePojo;
import io.subutai.plugin.spark.rest.pojo.SparkPojo;


public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Spark sparkManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;


    public RestServiceImpl( final Spark sparkManager, final Tracker tracker,
                            final EnvironmentManager environmentManager, final Hadoop hadoopManager )
    {
        this.sparkManager = sparkManager;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.hadoopManager = hadoopManager;
    }


    @Override
    public Response listClusters()
    {
        List<SparkClusterConfig> configs = sparkManager.getClusters();
        Set<String> clusterNames = Sets.newHashSet();

        for ( SparkClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        SparkClusterConfig config = sparkManager.getCluster( clusterName );

        String cluster = JsonUtil.GSON.toJson( parsePojo( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName, final String master,
                                    final String workers )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( master );
        Preconditions.checkNotNull( workers );

        SparkClusterConfig config = new SparkClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setMasterNodeId( master );

        config.setSlavesId( ( Set<String> ) JsonUtil.fromJson( workers, new TypeToken<Set<String>>()
        {
        }.getType() ) );

        UUID uuid = sparkManager.installCluster( config );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addSlaveNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.addSlaveNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroySlaveNode( final String clusterName, final String lxcHostName )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.destroySlaveNode( clusterName, lxcHostName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response changeMasterNode( final String clusterName, final String lxcHostName, final boolean keepSlave )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( keepSlave );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.changeMasterNode( clusterName, lxcHostName, keepSlave );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( master );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.startNode( clusterName, lxcHostName, master );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( master );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.stopNode( clusterName, lxcHostName, master );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostName, final boolean master )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostName );
        Preconditions.checkNotNull( master );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.checkNode( clusterName, lxcHostName, master );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.startCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.stopCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = sparkManager.checkCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startNodes( final String clusterName, final String lxcHosts )
    {
        return nodeOperation( clusterName, lxcHosts, true );
    }


    @Override
    public Response stopNodes( final String clusterName, final String lxcHosts )
    {
        return nodeOperation( clusterName, lxcHosts, false );
    }


    private Response nodeOperation( String clusterName, String lxcHosts, boolean startNode )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );
        List<String> hosts;


        if ( sparkManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        try
        {
            hosts = JsonUtil.fromJson( lxcHosts, new TypeToken<List<String>>()
            {
            }.getType() );
        }
        catch ( Exception e )
        {
            return Response.status( Response.Status.BAD_REQUEST ).entity( JsonUtil.toJson( "Bad input form" + e ) )
                           .build();
        }

        int errors = 0;

        for ( String host : hosts )
        {
            UUID uuid;
            if ( startNode )
            {
                uuid = sparkManager.startNode( clusterName, host, false );
            }
            else
            {
                uuid = sparkManager.stopNode( clusterName, host, false );
            }

            OperationState state = waitUntilOperationFinish( uuid );

            Response response = createResponse( uuid, state );

            if ( response.getStatus() != 200 )
            {
                errors++;
            }
        }

        if ( errors > 0 )
        {
            return Response.status( Response.Status.EXPECTATION_FAILED )
                           .entity( errors + " nodes are failed to execute" ).build();
        }

        return Response.ok().build();
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        SparkClusterConfig sparkClusterConfig = sparkManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( sparkClusterConfig.getHadoopClusterName() );

        Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
        nodes.removeAll( sparkClusterConfig.getAllNodesIds() );
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
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( SparkClusterConfig.PRODUCT_KEY, uuid );
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


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( SparkClusterConfig.PRODUCT_KEY, uuid );
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


    private SparkPojo parsePojo( SparkClusterConfig config )
    {
        SparkPojo pojo = new SparkPojo();
        try
        {
            Environment env = environmentManager.loadEnvironment( config.getEnvironmentId() );

            pojo.setClusterName( config.getClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setServer( new NodePojo( config.getMasterNodeId(), env ) );
            UUID uuid = sparkManager.checkNode( config.getClusterName(), pojo.getServer().getHostname(), true );
            pojo.getServer().setStatus( checkStatus( tracker, uuid ) );

            Set<NodePojo> clients = new HashSet<>();
            for ( String slave : config.getSlaveIds() )
            {
                NodePojo node = new NodePojo( slave, env );
                uuid = sparkManager.checkNode( config.getClusterName(), node.getHostname(), false );
                node.setStatus( checkStatus( tracker, uuid ) );

                clients.add( node );
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
            TrackerOperationView po = tracker.getTrackerOperation( SparkClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().toLowerCase().contains( "not" ) )
                    {
                        state = "STOPPED";
                    }
                    else if ( po.getLog().toLowerCase().contains( "is running" ) )
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


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
