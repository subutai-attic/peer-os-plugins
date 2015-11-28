package io.subutai.plugin.nutch.rest;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.nutch.api.Nutch;
import io.subutai.plugin.nutch.api.NutchConfig;
import io.subutai.plugin.nutch.rest.pojo.ContainerPojo;
import io.subutai.plugin.nutch.rest.pojo.NutchPojo;


public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Nutch nutchManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Nutch nutchManager )
    {
        this.nutchManager = nutchManager;
    }


    @Override
    public Response listClusters()
    {

        List<NutchConfig> configs = nutchManager.getClusters();
        Set<String> clusterNames = Sets.newHashSet();

        for ( NutchConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        NutchConfig config = nutchManager.getCluster( clusterName );

        String cluster = JsonUtil.GSON.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( final String clusterName, final String hadoopClusterName, final String nodeIds )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( nodeIds );

        NutchConfig config = new NutchConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );

		List<String> hosts = JsonUtil.fromJson( nodeIds, new TypeToken<List<String>> (){}.getType() );

        for ( String node : hosts )
        {

            config.getNodes().add( node );
        }
        UUID uuid = nutchManager.installCluster( config );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( nutchManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = nutchManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( nutchManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = nutchManager.addNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( nutchManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = nutchManager.destroyNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        NutchConfig nutchConfig = nutchManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( nutchConfig.getHadoopClusterName() );

        Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
        nodes.removeAll( nutchConfig.getNodes() );
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


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( NutchConfig.PRODUCT_KEY, uuid );
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


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( NutchConfig.PRODUCT_KEY, uuid );
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


    private NutchPojo updateConfig( NutchConfig config )
    {
        NutchPojo pojo = new NutchPojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setHadoopClusterName( config.getHadoopClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getNodes() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                containerPojoSet.add( new ContainerPojo( ch.getHostname() , ch.getIpByInterfaceName( "eth0" ) ) );
            }

            pojo.setNodes( containerPojoSet );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
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
