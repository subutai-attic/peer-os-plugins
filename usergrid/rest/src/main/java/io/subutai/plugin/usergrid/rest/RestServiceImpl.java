/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.rest;


import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.time.DateUtils;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.common.util.StringUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.identity.api.IdentityManager;
import io.subutai.core.identity.api.model.UserToken;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.api.UsergridInterface;
import io.subutai.plugin.usergrid.rest.pojo.VersionPojo;


public class RestServiceImpl implements RestService
{

    private UsergridInterface userGridInterface;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( RestServiceImpl.class.getName() );
    private IdentityManager identityManager;


    public RestServiceImpl( UsergridInterface userGridInterface, Tracker tracker, EnvironmentManager environmentManager,
                            IdentityManager identityManager )
    {
        this.userGridInterface = userGridInterface;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
        this.identityManager = identityManager;
    }


    @Override
    public Response configureCluster( String clusterName, String userDomain, String cassandraCSV,
                                      String elasticSearchCSV, String environmentId )
    {
        UsergridConfig usergridConfig = new UsergridConfig();
        usergridConfig.setClusterName( clusterName );
        usergridConfig.setEnvironmentId( environmentId );
        usergridConfig.setUserDomain( userDomain );
        usergridConfig.setCassandraName( Arrays.asList( cassandraCSV.split( "," ) ) );
        usergridConfig.setElasticSName( Arrays.asList( elasticSearchCSV.split( "," ) ) );
        UUID installCluster = userGridInterface.installCluster( usergridConfig );
        OperationState opState = waitUntilOperationFinish( installCluster );
        return createResponse( installCluster, opState );
    }


    @Override
    public Response oneClick( String ename, String udom )
    {
        Date permanentDate = DateUtils.addYears( new Date( System.currentTimeMillis() ), 10 );
        final UserToken t =
                identityManager.createUserToken( identityManager.getActiveUser(), null, null, null, 2, permanentDate );
        UsergridConfig localConfig = new UsergridConfig();
        localConfig.setPermanentToken( t.getFullToken() );
        localConfig.setUserDomain( udom );
        localConfig.setEnvironmentName( ename );
        UUID uuid = userGridInterface.oneClickInstall( localConfig );
        OperationState waitUntilOperationFinish = waitUntilOperationFinish( uuid );
        return createResponse( uuid, waitUntilOperationFinish );
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
    public Response listClusterUI( Environment environmentID )
    {
        Set<EnvironmentContainerHost> containerHosts = environmentID.getContainerHosts();
        return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( containerHosts ) ).build();
    }


    @Override
    public Response listClustersDB()
    {
        List<UsergridConfig> clusters = userGridInterface.getClusters();
        return Response.status( Response.Status.OK ).entity( JsonUtil.GSON.toJson( clusters ) ).build();
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( UsergridConfig.PRODUCT_NAME, uuid );
        if ( null != state )
        {
            switch ( state )
            {
                case FAILED:
                    return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                   .entity( JsonUtil.toJson( po.getLog() ) ).build();
                case SUCCEEDED:
                    return Response.status( Response.Status.OK )
                                   .entity( JsonUtil.toJson( JsonUtil.toJson( po.getLog() ) ) ).build();
            }
        }
        return Response.status( javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        // OperationState state = OperationState.RUNNING;
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( UsergridConfig.PRODUCT_KEY, uuid );

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


    public UsergridInterface getUserGridInterface()
    {
        return userGridInterface;
    }


    public void setUserGridInterface( UsergridInterface userGridInterface )
    {
        this.userGridInterface = userGridInterface;
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


    private String validateInput( String inputStr, boolean removeSpaces )
    {
        return StringUtil.removeHtmlAndSpecialChars( inputStr, removeSpaces );
    }
}

