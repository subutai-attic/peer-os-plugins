/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterConfigurationInterface;
import io.subutai.plugin.common.api.ConfigBase;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterConfiguration implements ClusterConfigurationInterface<ConfigBase>
{

    private TrackerOperation trackerOperation;
    private AppScaleImpl appScaleImpl;
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );


    public ClusterConfiguration( TrackerOperation trackerOperation, AppScaleImpl appScaleImpl )
    {
        this.trackerOperation = trackerOperation;
        this.appScaleImpl = appScaleImpl;
    }


    /**
     *
     * @param t
     * @param e
     * @throws ClusterConfigurationException
     *
     * configure cluster with appscale pre - requirements
     *
     * tutorial: https://confluence.subutai.io/display/APPSTALK/How+to+configure+container+for+the+needs+of+AppScale
     *
     */
    @Override
    public void configureCluster( ConfigBase t, Environment e ) throws ClusterConfigurationException
    {
        AppScaleConfig appScaleConfig = ( AppScaleConfig ) t;
        EnvironmentContainerHost containerHostById;
        CommandResult result;
        try
        {
            containerHostById = e.getContainerHostById( appScaleConfig.getClusterName() );
            result = containerHostById.execute( new RequestBuilder( Commands.getAddUbuntuUser() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getAddUserToRoot() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getCreateSshFolder() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getCreateAppscaleFolder() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getInstallGit() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getGitAppscale() ) );
            resultCheck( result );
            result = containerHostById.execute( new RequestBuilder( Commands.getGitAppscaleTools() ) );
            resultCheck( result );
        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error( "No environment found..." );
            trackerOperation.addLog( "error getting environment for container...." );
        }
        catch ( CommandException ex )
        {
            LOG.error( ex.getLocalizedMessage() );
        }

    }


    private void resultCheck( CommandResult result )
    {
        if ( result.hasCompleted() )
        {
            trackerOperation.addLogDone( result.getStdOut() );
        }
        else
        {
            trackerOperation.addLogFailed( result.getStdErr() );
        }
    }

}

