/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.rest;


import io.subutai.common.command.Response;
import io.subutai.common.environment.Environment;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
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


    public RestServiceImpl ( UsergridInterface userGridInterface, Tracker tracker, EnvironmentManager environmentManager )
    {
        this.userGridInterface = userGridInterface;
        this.tracker = tracker;
        this.environmentManager = environmentManager;
    }


    @Override
    public Response configureCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public Response listClusterUI ( Environment environmentID )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public Response listClustersDB ()
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
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

