package io.subutai.plugin.storm.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.ws.rs.core.Response;

import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.Host;
import io.subutai.common.util.StringUtil;
import io.subutai.plugin.storm.rest.pojo.VersionPojo;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.rest.pojo.ContainerPojo;
import io.subutai.plugin.storm.rest.pojo.StormPojo;
//import io.subutai.plugin.zookeeper.api.Zookeeper;


public class RestServiceImpl implements RestService
{
    private static final Logger LOGGER = LoggerFactory.getLogger( RestServiceImpl.class );
    private Storm stormManager;
    //    private Zookeeper zookeeperManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


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


    //    public Zookeeper getZookeeperManager()
    //    {
    //        return zookeeperManager;
    //    }
    //
    //
    //    public void setZookeeperManager( final Zookeeper zookeeperManager )
    //    {
    //        this.zookeeperManager = zookeeperManager;
    //    }


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


    public Response installCluster( String clusterName, String environmentId, String nimbus, String supervisors )
    {
        StormClusterConfiguration config = new StormClusterConfiguration();
        config.setClusterName( clusterName );
        config.setEnvironmentId( environmentId );
        config.setNimbus( nimbus );
        config.setExternalZookeeper( false );

        try
        {
            environmentManager.loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Environment with id not found.", e );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "" ).build();
        }

        List<String> hosts = JsonUtil.fromJson( supervisors, new TypeToken<List<String>>()
        {
        }.getType() );

        for ( String node : hosts )
        {
            config.getSupervisors().add( node );
        }

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

        String clusterInfo = JsonUtil.GSON.toJson( updateConfig( config ) );
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


    public Response startNode( String clusterName, String nodeId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodeId );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.startNode( clusterName, nodeId );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    public Response stopNode( String clusterName, String nodeId )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( nodeId );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }

        UUID uuid = stormManager.stopNode( clusterName, nodeId );
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


    public Response destroyNode( String clusterName, String nodeId )
    {
        Preconditions.checkNotNull( clusterName );
        if ( stormManager.getCluster( clusterName ) == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).
                    entity( clusterName + " cluster not found." ).build();
        }
        UUID uuid = stormManager.destroyNode( clusterName, nodeId );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
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
    public Response startNodes( String clusterName, String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );

        try
        {
            JSONArray arr = new JSONArray( lxcHosts );
            List<String> names = new ArrayList<>();
            for ( int i = 0; i < arr.length(); ++i )
            {
                JSONObject obj = arr.getJSONObject( i );
                String name = obj.getString( "name" );
                names.add( name );
            }

            if ( names == null || names.isEmpty() )
            {
                return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
            }

            int errors = 0;

            for ( int i = 0; i < names.size(); ++i )
            {
                UUID uuid = stormManager.startNode( clusterName, names.get( i ) );
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
        }
        catch ( JSONException e )
        {
            e.printStackTrace();
        }

        return Response.ok().build();
    }


    @Override
    public Response stopNodes( String clusterName, String lxcHosts )
    {
        Preconditions.checkNotNull( clusterName );
        Preconditions.checkNotNull( lxcHosts );
        try
        {
            JSONArray arr = new JSONArray( lxcHosts );
            List<String> names = new ArrayList<>();
            for ( int i = 0; i < arr.length(); ++i )
            {
                JSONObject obj = arr.getJSONObject( i );
                String name = obj.getString( "name" );
                names.add( name );
            }

            if ( names == null || names.isEmpty() )
            {
                return Response.status( Response.Status.BAD_REQUEST ).entity( "Error parsing lxc hosts" ).build();
            }

            int errors = 0;

            for ( int i = 0; i < names.size(); ++i )
            {
                UUID uuid = stormManager.stopNode( clusterName, names.get( i ) );
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
        }
        catch ( JSONException e )
        {
            e.printStackTrace();
        }

        return Response.ok().build();
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
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
        if ( scale == false )
        {
            message = "disabled";
        }

        return Response.status( Response.Status.OK ).entity( "Auto scale is " + message + " successfully" ).build();
    }


    private StormPojo updateConfig( StormClusterConfiguration config )
    {
        StormPojo pojo = new StormPojo();
        Set<ContainerPojo> containerPojoSet = Sets.newHashSet();

        try
        {
            pojo.setClusterName( config.getClusterName() );
            pojo.setEnvironmentId( config.getEnvironmentId() );
            pojo.setAutoScaling( config.isAutoScaling() );

            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( final String uuid : config.getSupervisors() )
            {
                ContainerHost ch = environment.getContainerHostById( uuid );
                UUID uuidStatus = stormManager.checkNode( config.getClusterName(), ch.getId() );
                HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
                containerPojoSet.add( new ContainerPojo( ch.getHostname(), uuid, hostInterface.getIp(),
                        checkSupervisorStatus( tracker, uuidStatus ) ) );
            }

            pojo.setSupervisors( containerPojoSet );

            ContainerHost ch = environment.getContainerHostById( config.getNimbus() );
            HostInterface hostInterface = ch.getInterfaceByName( "eth0" );
            UUID uuid = stormManager.checkNode( config.getClusterName(), ch.getId() );
            pojo.setNimbus( new ContainerPojo( ch.getHostname(), config.getNimbus(), hostInterface.getIp(),
                    checkNimbusStatus( tracker, uuid ) ) );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        return pojo;
    }


    private String checkNimbusStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( StormClusterConfiguration.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "nimbus" ) && po.getLog().contains( "core" ) )
                    {
                        state = "RUNNING";
                    }
                    else
                    {
                        state = "STOPPED";
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


    private String checkSupervisorStatus( Tracker tracker, UUID uuid )
    {
        String state = "UNKNOWN";
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( StormClusterConfiguration.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "supervisor" ) )
                    {
                        state = "RUNNING";
                    }
                    else
                    {
                        state = "STOPPED";
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
            if ( System.currentTimeMillis() - start > ( 200 * 100000 ) )
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


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }
}
