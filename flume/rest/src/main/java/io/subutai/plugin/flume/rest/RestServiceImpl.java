package io.subutai.plugin.flume.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.flume.rest.dto.ClusterDto;
import io.subutai.plugin.flume.rest.dto.ContainerDto;
import io.subutai.plugin.flume.rest.dto.VersionPojo;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class RestServiceImpl implements RestService
{
    private Flume flumeManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;


    public RestServiceImpl( final Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    @Override
    public Response listClusters()
    {
        List<FlumeConfig> configs = flumeManager.getClusters();
        List<String> clusterNames = Lists.newArrayList();

        for ( FlumeConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    public Response getCluster( final String clusterName )
    {
        FlumeConfig config = flumeManager.getCluster( clusterName );
        Environment environment = null;
        try
        {
            environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        boolean thrownException = false;


        ClusterDto clusterDto = new ClusterDto( clusterName );
        clusterDto.setEnvironmentId( environment.getId() );

        for ( String node : config.getNodes() )
        {
            try
            {
                ContainerDto containerDto = new ContainerDto();
                EnvironmentContainerHost containerHost = environment.getContainerHostById( node );
                HostInterface hostInterface = containerHost.getInterfaceByName( "eth0" );
                String ip = hostInterface.getIp();
                containerDto.setIp( ip );
                containerDto.setId( node );
                containerDto.setHostname( containerHost.getHostname() );

                UUID uuid = flumeManager.checkNode( clusterName, containerHost.getHostname() );
                OperationState state = waitUntilOperationFinish( uuid );
                Response response = createResponse( uuid, state );
                if ( response.getStatus() == 200 && !response.getEntity().toString().toUpperCase().contains( "NOT" ) )
                {
                    containerDto.setStatus( "RUNNING" );
                }
                else
                {
                    containerDto.setStatus( "STOPPED" );
                }

                clusterDto.addContainerDto( containerDto );
            }
            catch ( Exception e )
            {
                thrownException = true;
            }
        }

        if ( thrownException )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( clusterName + " cluster not found." ).build();
        }

        return Response.status( Response.Status.OK ).entity( JsonUtil.toJson( clusterDto ) ).build();
    }


    public Response installCluster( final String clusterName, final String hadoopClusterName, final String nodes )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hadoopClusterName );
        Preconditions.checkNotNull( nodes );

        FlumeConfig config = new FlumeConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );


        config.setNodes( ( Set<String> ) JsonUtil.fromJson( nodes, new TypeToken<Set<String>>()
        {
        }.getType() ) );

        UUID uuid = flumeManager.installCluster( config );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response uninstallCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response addNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.addNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response destroyNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.destroyNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response startNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.startNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response stopNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.stopNode( clusterName, hostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response checkNode( final String clusterName, final String hostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( hostname );
        if ( flumeManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = flumeManager.checkNode( clusterName, hostname );
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


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( FlumeConfig.PRODUCT_KEY, uuid );
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
            TrackerOperationView po = tracker.getTrackerOperation( FlumeConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 90 * 100000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response nodeOperation( String clusterName, String lxcHosts, boolean startNode )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );
        List<String> hosts;


        if ( flumeManager.getCluster( clusterName ) == null )
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
                uuid = flumeManager.startNode( clusterName, host );
            }
            else
            {
                uuid = flumeManager.stopNode( clusterName, host );
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


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        FlumeConfig flumeConfig = flumeManager.getCluster( clusterName );
        HadoopClusterConfig hadoopConfig = hadoopManager.getCluster( flumeConfig.getHadoopClusterName() );

        Set<String> nodes = new HashSet<>( hadoopConfig.getAllNodes() );
        nodes.removeAll( flumeConfig.getNodes() );
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

            }
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    @Override
    public Response getPluginInfo()
    {
        Properties prop = new Properties();
        VersionPojo pojo = new VersionPojo();
        InputStream input = null;
        try
        {
            input = getClass().getResourceAsStream( "/git.properties" );

            prop.load( input );
            pojo.setGitCommitId( prop.getProperty( "git.commit.id" ) );
            pojo.setGitCommitTime( prop.getProperty( "git.commit.time" ) );
            pojo.setGitBranch( prop.getProperty( "git.branch" ) );
            pojo.setGitCommitUserName( prop.getProperty( "git.commit.user.name" ) );
            pojo.setGitCommitUserEmail( prop.getProperty( "git.commit.user.email" ) );
            pojo.setProjectVersion( prop.getProperty( "git.build.version" ) );

            pojo.setGitBuildUserName( prop.getProperty( "git.build.user.name" ) );
            pojo.setGitBuildUserEmail( prop.getProperty( "git.build.user.email" ) );
            pojo.setGitBuildHost( prop.getProperty( "git.build.host" ) );
            pojo.setGitBuildTime( prop.getProperty( "git.build.time" ) );

            pojo.setGitClosestTagName( prop.getProperty( "git.closest.tag.name" ) );
            pojo.setGitCommitIdDescribeShort( prop.getProperty( "git.commit.id.describe-short" ) );
            pojo.setGitClosestTagCommitCount( prop.getProperty( "git.closest.tag.commit.count" ) );
            pojo.setGitCommitIdDescribe( prop.getProperty( "git.commit.id.describe" ) );
        }
        catch ( IOException ex )
        {
            ex.printStackTrace();
        }
        finally
        {
            if ( input != null )
            {
                try
                {
                    input.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }

        String projectInfo = JsonUtil.GSON.toJson( pojo );

        return Response.status( Response.Status.OK ).entity( projectInfo ).build();
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( flumeManager.getWebModule().getAngularDependecyList() ).build();
    }
}
