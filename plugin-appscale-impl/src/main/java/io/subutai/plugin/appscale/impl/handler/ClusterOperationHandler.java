/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;
import io.subutai.plugin.appscale.impl.ClusterConfiguration;
import io.subutai.plugin.appscale.impl.Commands;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;


/**
 *
 * @author caveman
 */
public class ClusterOperationHandler extends AbstractOperationHandler<AppScaleImpl, AppScaleConfig> implements
        ClusterOperationHandlerInterface
{
    private final ClusterOperationType clusterOperationType;
    private final AppScaleConfig appScaleConfig;
    private AppScaleImpl appScaleImpl;
    private final String clstrName;
    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterOperationHandler ( AppScaleImpl appScaleImpl, AppScaleConfig appScaleConfig,
                                     ClusterOperationType clusterOperationType )
    {
        super ( appScaleImpl, appScaleConfig );
        this.appScaleConfig = appScaleConfig;
        clstrName = this.appScaleConfig.getClusterName ();
        this.clusterOperationType = clusterOperationType;
        String msg = String.format ( "Starting %s operation on %s(%s) cluster...", clusterOperationType, clstrName,
                                     appScaleConfig.getProductKey () );
        LOG.info ( msg );
        // appScaleImpl.getTracker ().createTrackerOperation ( AppScaleConfig.PRODUCT_KEY, msg );

    }


    @Override
    public void run ()
    {
        Preconditions.checkNotNull ( appScaleConfig, "Configuration is null" );
        switch ( clusterOperationType )
        {
            case INSTALL:
            {
                setupCluster ();
                break;
            }
            case UNINSTALL:
            {
                destroyCluster ();
                break;
            }
            case REMOVE:
            {
                removeCluster ( clstrName );
                break;
            }
            case START_ALL:
            {
                runOperationOnContainers ( clusterOperationType );
                break;
            }


        }
    }


    /**
     *
     * @param cot
     *
     * run operations in containers... like starting up container etc.
     */
    @Override
    public void runOperationOnContainers ( ClusterOperationType cot )
    {
        try
        {
            Environment environment = appScaleImpl.getEnvironmentManager ().loadEnvironment (
                    appScaleConfig.getClusterName () );
            EnvironmentContainerHost environmentContainerHost;
            EnvironmentContainerHost containerHostById = environment.getContainerHostById (
                    appScaleConfig.getClusterName () );
            CommandResult res;
            switch ( cot )
            {
                case START_ALL:
                {
                    res = containerHostById.execute ( new RequestBuilder ( Commands.getAppScaleStartCommand () ) );
                    if ( res.hasSucceeded () )
                    {
                        trackerOperation.addLogDone ( res.getStdOut () );
                    }
                    else
                    {
                        trackerOperation.addLogFailed ( res.getStdErr () );
                    }
                    break;
                }
                case STOP_ALL:
                {
                    res = containerHostById.execute ( new RequestBuilder ( Commands.getAppScaleStopCommand () ) );
                    if ( res.hasSucceeded () )
                    {
                        trackerOperation.addLogDone ( res.getStdOut () );
                    }
                    else
                    {
                        trackerOperation.addLogFailed ( res.getStdErr () );
                    }
                    break;
                }
            }

        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException ex )
        {
            LOG.error ( ex.getLocalizedMessage () );
        }

    }


    /**
     * set up cluster appscale with pre - requirements in tutorial
     */
    @Override
    public void setupCluster ()
    {
        LOG.info ( "setupCluster started..." );
        LOG.info ( appScaleConfig.getEnvironmentId () );
        Environment env = this.returnEnvironment ();
        LOG.info ( String.format ( "Configuring %s environment for %s(%s) cluster", env.getName (),
                                   appScaleConfig.getClusterName (), appScaleConfig.getProductKey () ) );
        try
        {
            new ClusterConfiguration ( trackerOperation, appScaleImpl ).configureCluster ( appScaleConfig, env );
        }
        catch ( ClusterConfigurationException cce )
        {
            LOG.error ( "ClusterConfigurationException: " + cce.getLocalizedMessage () );
        }
    }


    /**
     * @return environment
     *
     * this is problematic part... returns null
     */
    private Environment returnEnvironment ()
    {
        EnvironmentManager environmentManager = null;
        try
        {
            environmentManager = appScaleImpl.getEnvironmentManager ();
        }
        catch ( Exception e )
        {
            LOG.info ( "Environment Manager Exception: " + e.getLocalizedMessage () );
        }

        Environment env = null;
        try
        {
            env = environmentManager.loadEnvironment ( appScaleConfig.getEnvironmentId () );
        }
        catch ( Exception e )
        {
            LOG.info ( "Exception Load Env: " + e.getLocalizedMessage () );
        }
        return env;
    }


    /**
     * destroy cluster process... if needed..
     */
    @Override
    public void destroyCluster ()
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    /**
     *
     * @param clusterName
     *
     * remove cluster process... if needed...
     */
    private void removeCluster ( String clusterName )
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }

}

