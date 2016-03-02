/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.rest;


import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.common.util.JsonUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.api.UsergridInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class RestServiceImpl implements RestService
{

    private UsergridInterface userGridInterface;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger ( RestServiceImpl.class.getName () );


    public RestServiceImpl ( UsergridInterface userGridInterface, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.userGridInterface = userGridInterface;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    @Override
    public Response configureCluster ( String clusterName, String userDomain, String cassandraCSV,
                                       String elasticSearchCSV, String environmentId )
    {
        UsergridConfig usergridConfig = new UsergridConfig ();
        usergridConfig.setClusterName ( clusterName );
        usergridConfig.setEnvironmentId ( environmentId );
        usergridConfig.setUserDomain ( userDomain );
        usergridConfig.setCassandraName ( Arrays.asList ( cassandraCSV.split ( "," ) ) );
        usergridConfig.setElasticSName ( Arrays.asList ( elasticSearchCSV.split ( "," ) ) );
        UUID installCluster = userGridInterface.installCluster ( usergridConfig );
        OperationState opState = waitUntilOperationFinish ( installCluster );
        return createResponse ( installCluster, opState );

    }


    @Override
    public Response listClusterUI ( Environment environmentID )
    {
        Set<EnvironmentContainerHost> containerHosts = environmentID.getContainerHosts ();
        return Response.status ( Response.Status.OK ).entity ( JsonUtil.GSON.toJson ( containerHosts ) ).build ();
    }


    @Override
    public Response listClustersDB ()
    {
        List<UsergridConfig> clusters = userGridInterface.getClusters ();
        return Response.status ( Response.Status.OK ).entity ( JsonUtil.GSON.toJson ( clusters ) ).build ();
    }


    private Response createResponse ( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation ( UsergridConfig.getPRODUCT_NAME (), uuid );
        if ( null != state )
        {
            switch ( state )
            {
                case FAILED:
                    return Response.status ( Response.Status.INTERNAL_SERVER_ERROR ).entity (
                            JsonUtil.toJson ( po.getLog () ) )
                            .build ();
                case SUCCEEDED:
                    return Response.status ( Response.Status.OK ).entity (
                            JsonUtil.toJson (
                                    JsonUtil.toJson ( po.getLog () ) ) )
                            .build ();
            }
        }
        return Response.status ( javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR ).entity (
                "Timeout" ).build ();
    }


    private OperationState waitUntilOperationFinish ( UUID uuid )
    {
        // OperationState state = OperationState.RUNNING;
        OperationState state = null;
        long start = System.currentTimeMillis ();
        while ( !Thread.interrupted () )
        {
            TrackerOperationView po = tracker.getTrackerOperation ( UsergridConfig.getPRODUCT_NAME (), uuid );
            LOG.info ( "*********\n" + po.getState () + "\n********" );
            if ( po != null )
            {

                if ( po.getState () != OperationState.RUNNING )
                {
                    state = po.getState ();
                    break;
                }

            }
            try
            {
                Thread.sleep ( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis () - start > ( 6000 * 1000 ) )
            {
                break;
            }
        }

        return state;
    }


    public UsergridInterface getUserGridInterface ()
    {
        return userGridInterface;
    }


    public void setUserGridInterface ( UsergridInterface userGridInterface )
    {
        this.userGridInterface = userGridInterface;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager ()
    {
        return environmentManager;
    }


    public void setEnvironmentManager ( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


}

