/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import io.subutai.plugin.appscale.rest.pojo.VersionPojo;
import io.subutai.webui.api.WebuiModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.time.DateUtils;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.identity.api.IdentityManager;
import io.subutai.core.identity.api.model.UserToken;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class RestServiceImpl implements RestService
{

    private AppScaleInterface appScaleInterface;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private IdentityManager identityManager;
    private WebuiModule webuiModule;


    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );


    public RestServiceImpl( AppScaleInterface appScaleInterface, Tracker tracker, EnvironmentManager environmentManager,
                            IdentityManager identityManager, WebuiModule webuiModule )
    {
        this.appScaleInterface = appScaleInterface;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.identityManager = identityManager;
        this.webuiModule = webuiModule;
    }


    /**
     * @return list of clusters in environment.
     */
    @Override
    public Response listCluster( Environment environmentID )
    {
        Set<EnvironmentContainerHost> containerHosts = environmentID.getContainerHosts();
        return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( containerHosts ) ).build();
    }


    /**
     * @return return list of cluster in format of master : mastername; cassandra : cassandraname; zookeeper :
     * zookeepername
     */
    @Override
    public Response listClusters()
    {
        List<AppScaleConfig> ascs = appScaleInterface.getClusters();
        return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( ascs ) ).build();
    }


    @Override
    public Response oneClick( String ename, String udom )
    {
        LOG.info( ename + udom );
        if ( ename != null && udom != null )
        {
            Date permanentDate = DateUtils.addYears( new Date( System.currentTimeMillis() ), 10 );
            final UserToken t = identityManager
                    .createUserToken( identityManager.getActiveUser(), null, null, null, 2, permanentDate );
            String token = t.getFullToken();
            AppScaleConfig appScaleConfig = new AppScaleConfig();
            appScaleConfig.setPermanentToken( token );
            appScaleConfig.setUserEnvironmentName( ename );
            appScaleConfig.setUserDomain( udom );
            LOG.info( appScaleConfig.toString() );
            UUID uuid = appScaleInterface.oneClickInstall( appScaleConfig );
            OperationState op = waitUntilOperationFinish( uuid );
            return createResponse( uuid, op );
        }
        return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "null" ).build();
    }


    @Override
    public Response runSsh( String clusterName )
    {
        AppScaleConfig config = appScaleInterface.getConfig( clusterName );

        UUID configureSSH = appScaleInterface.configureSSH( config );
        OperationState operationState = waitUntilOperationFinish( configureSSH );
        TrackerOperationView tov = tracker.getTrackerOperation( clusterName, configureSSH );
        switch ( operationState )
        {
            case SUCCEEDED:
                return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            case FAILED:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                               .entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            default:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "timeout" ).build();
        }
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( webuiModule.getAngularDependecyList() ).build();
    }


    @Override
    public Response getConfigureSsh( String clusterName )
    {
        AppScaleConfig config = appScaleInterface.getConfig( clusterName );
        appScaleInterface.configureSsh( config );
        return Response.status( Response.Status.OK ).entity( clusterName ).build();
    }


    @Override

    public Response growenvironment( String clusterName )
    {
        AppScaleConfig appScaleConfig = appScaleInterface.getConfig( clusterName );
        UUID uuid = appScaleInterface.growEnvironment( appScaleConfig );
        OperationState operationState = waitUntilOperationFinish( uuid );
        if ( uuid == null )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "failed" ).build();
        }
        TrackerOperationView tov = tracker.getTrackerOperation( clusterName, uuid );
        switch ( operationState )
        {
            case SUCCEEDED:
                return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            case FAILED:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                               .entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            default:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "timeout" ).build();
        }
    }


    @Override
    public Response getCluster( String clusterName )
    {
        AppScaleConfig appScaleConfig = appScaleInterface.getCluster( clusterName );
        String cluster = JsonUtil.GSON.toJson( appScaleConfig );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response startStopMaster( Environment envID, String operation )
    {
        Set<EnvironmentContainerHost> containerHosts = envID.getContainerHosts();
        for ( EnvironmentContainerHost e : containerHosts )
        {
            String cn = e.getContainerName();
            AppScaleConfig as = appScaleInterface.getConfig( cn );
            if ( as.getClusterName() != null )
            {
                Boolean b =
                        appScaleInterface.checkIfContainerInstalled( as ); // this will also finds the master container
                if ( b )
                {
                    switch ( operation )
                    {
                        case "start":
                        {
                            UUID startCluster = appScaleInterface.startCluster( cn );
                            OperationState operationState = waitUntilOperationFinish( startCluster );
                            return createResponse( startCluster, operationState );
                        }
                        case "stop":
                        {
                            UUID startCluster = appScaleInterface.stopCluster( cn );
                            OperationState operationState = waitUntilOperationFinish( startCluster );
                            return createResponse( startCluster, operationState );
                        }
                        default:
                            return Response.status( Response.Status.NOT_FOUND ).entity( envID ).build();
                    }
                }
            }
        }
        return Response.status( Response.Status.NOT_FOUND ).entity( envID ).build();
    }


    @Override
    public Response configureCluster( String clusterName, String appengineName, String zookeeperName,
                                      String cassandraName, String envID, String userDomain, String scaleOption )
    {
        AppScaleConfig appScaleConfig = new AppScaleConfig();

        appScaleConfig.setClusterName( clusterName );
        appScaleConfig.setUserDomain( userDomain );
        if ( scaleOption == null )
        {
            scaleOption = "static";
        }
        appScaleConfig.setScaleOption( scaleOption );
        if ( !zookeeperName.isEmpty() )
        {

            appScaleConfig.setZooList( Arrays.asList( zookeeperName.split( "," ) ) );
        }
        if ( !cassandraName.isEmpty() )
        {
            appScaleConfig.setCassList( Arrays.asList( cassandraName.split( "," ) ) );
        }
        if ( !appengineName.isEmpty() )
        {
            appScaleConfig.setAppenList( Arrays.asList( appengineName.split( "," ) ) );
        }

        appScaleConfig.setEnvironmentId( envID );

        UUID uuid = appScaleInterface.installCluster( appScaleConfig );

        OperationState operationState = waitUntilOperationFinish( uuid );
        return createResponse( uuid, operationState );
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( AppScaleConfig.PRODUCT_NAME, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( JsonUtil.toJson( po.getLog() ) )
                           .build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( JsonUtil.toJson( JsonUtil.toJson( po.getLog() ) ) )
                           .build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    @Override
    public Response uninstallCluster( String clusterName )
    {
        AppScaleConfig appScaleConfig = appScaleInterface.getConfig( clusterName );
        UUID uuid = appScaleInterface.uninstallCluster( appScaleConfig );
        OperationState operationState = waitUntilOperationFinish( uuid );
        TrackerOperationView tov = tracker.getTrackerOperation( clusterName, uuid );
        switch ( operationState )
        {
            case SUCCEEDED:
                return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            case FAILED:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                               .entity( JsonUtil.GSON.toJson( tov.getLog() ) ).build();
            default:
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "timeout" ).build();
        }
    }


    public AppScaleInterface getAppScaleInterface()
    {
        return appScaleInterface;
    }


    public void setAppScaleInterface( AppScaleInterface appScaleInterface )
    {
        this.appScaleInterface = appScaleInterface;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        // OperationState state = OperationState.RUNNING;
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( AppScaleConfig.PRODUCT_NAME, uuid );
            LOG.info( "*********\n" + po.getState() + "\n********" );
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
            if ( System.currentTimeMillis() - start > ( 6000 * 1000 ) )
            {
                break;
            }
        }

        return state;
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
}

