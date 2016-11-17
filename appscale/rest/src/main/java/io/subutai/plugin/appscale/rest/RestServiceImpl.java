/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import io.subutai.common.util.StringUtil;
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


    @Override
    public Response listClusters()
    {
        List<AppScaleConfig> appScaleConfigList = appScaleInterface.getClusters();
        ArrayList<String> clusterNames = new ArrayList<>();

        for ( AppScaleConfig hadoopClusterConfig : appScaleConfigList )
        {
            clusterNames.add( hadoopClusterConfig.getClusterName() );
        }

        return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( clusterNames ) ).build();
    }


    @Override
    public Response getAngularConfig()
    {
        return Response.ok( webuiModule.getAngularDependecyList() ).build();
    }


    @Override
    public Response getCluster( String clusterName )
    {
        AppScaleConfig appScaleConfig = appScaleInterface.getCluster( clusterName );
        String cluster = JsonUtil.GSON.toJson( appScaleConfig );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( String clusterName, String appengineName, String zookeeperName,
                                    String cassandraName, String envID, String userDomain, String login,
                                    String password, String controller )
    {
        // TODO add XSS validation for inputs

        AppScaleConfig appScaleConfig = new AppScaleConfig();

        appScaleConfig.setClusterName( validateInput( clusterName, true ) );
        appScaleConfig.setControllerNode( controller );
        appScaleConfig.setDomain( userDomain );
        if ( !zookeeperName.isEmpty() )
        {

            appScaleConfig.setZookeeperNodes( Arrays.asList( zookeeperName.split( "," ) ) );
        }
        if ( !cassandraName.isEmpty() )
        {
            appScaleConfig.setCassandraNodes( Arrays.asList( cassandraName.split( "," ) ) );
        }
        if ( !appengineName.isEmpty() )
        {
            appScaleConfig.setAppengineNodes( Arrays.asList( appengineName.split( "," ) ) );
        }

        appScaleConfig.setLogin( login );
        appScaleConfig.setPassword( password );
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
        UUID uuid = appScaleInterface.uninstallCluster( clusterName );
        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
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


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }
}

