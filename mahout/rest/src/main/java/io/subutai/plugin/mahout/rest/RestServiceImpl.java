package io.subutai.plugin.mahout.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.ws.rs.core.Response;

import io.subutai.common.host.HostInterface;
import io.subutai.common.util.StringUtil;
import io.subutai.plugin.mahout.rest.pojo.VersionPojo;

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
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.rest.pojo.ContainerPojo;
import io.subutai.plugin.mahout.rest.pojo.MahoutPojo;


public class RestServiceImpl implements RestService
{
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private Mahout mahoutManager;
    private Tracker tracker;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;


    public RestServiceImpl( final Mahout mahoutManager )
    {
        Preconditions.checkNotNull( mahoutManager );

        this.mahoutManager = mahoutManager;
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Mahout getMahoutManager()
    {
        return mahoutManager;
    }


    public void setMahoutManager( final Mahout mahoutManager )
    {
        this.mahoutManager = mahoutManager;
    }


    @Override
    public Response listClusters()
    {
        List<MahoutClusterConfig> configs = mahoutManager.getClusters();
        ArrayList<String> clusterNames = Lists.newArrayList();

        for ( MahoutClusterConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        MahoutClusterConfig config = mahoutManager.getCluster( clusterName );
        if ( config == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( clusterName + "cluster not found" )
                           .build();
        }
        String cluster = JsonUtil.GSON.toJson( updateConfig( config ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( String clusterName, String hadoopClusterName, String nodes )
    {
        MahoutClusterConfig mahoutConfig = new MahoutClusterConfig();
        mahoutConfig.setClusterName( validateInput( clusterName, true ) );
        mahoutConfig.setHadoopClusterName( hadoopClusterName );

        mahoutConfig.setNodes( ( Set<String> ) JsonUtil.fromJson( nodes, new TypeToken<Set<String>>()
        {
        }.getType() ) );

        UUID uuid = mahoutManager.installCluster( mahoutConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        Preconditions.checkNotNull( clusterName );
        if ( mahoutManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = mahoutManager.uninstallCluster( clusterName );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mahoutManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = mahoutManager.checkNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mahoutManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = mahoutManager.addNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHostname );
        if ( mahoutManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = mahoutManager.uninstallNode( clusterName, lxcHostname );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response getAvailableNodes( final String clusterName )
    {
        Set<String> hostsName = Sets.newHashSet();

        MahoutClusterConfig nutchConfig = mahoutManager.getCluster( clusterName );
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
        }

        String hosts = JsonUtil.GSON.toJson( hostsName );
        return Response.status( Response.Status.OK ).entity( hosts ).build();
    }


    private MahoutPojo updateConfig( MahoutClusterConfig config )
    {
        MahoutPojo pojo = new MahoutPojo();
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
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                containerPojoSet.add( new ContainerPojo( ch.getHostname(), hostInterface.getIp(), ch.getId() ) );
            }

            pojo.setNodes( containerPojoSet );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MahoutClusterConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 9000 * 100000 ) )
            {
                break;
            }
        }
        return state;
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( MahoutClusterConfig.PRODUCT_KEY, uuid );
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


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( mahoutManager.getWebModule().getAngularDependecyList() ).build();
    }


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }
}