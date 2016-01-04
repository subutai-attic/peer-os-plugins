/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.List;

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

    private final TrackerOperation trackerOperation;
    private final AppScaleImpl appScaleImpl;
    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterConfiguration ( TrackerOperation trackerOperation, AppScaleImpl appScaleImpl )
    {
        this.trackerOperation = trackerOperation;
        this.appScaleImpl = appScaleImpl;
    }


    /**
     *
     * @param configBase
     * @param environment
     * @throws ClusterConfigurationException
     *
     * configure cluster with appscale pre - requirements
     *
     * tutorial: https://confluence.subutai.io/display/APPSTALK/How+to+configure+container+for+the+needs+of+AppScale
     *
     */
    @Override
    public void configureCluster ( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        AppScaleConfig appScaleConfig = ( AppScaleConfig ) configBase;
        EnvironmentContainerHost containerHostById;
        CommandResult result;
        try
        {
            containerHostById = environment.getContainerHostById ( appScaleConfig.getClusterName () );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getExportHome () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getFixLocale () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getChangeRootPasswd () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getEditSSHD () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getAddUbuntuUser () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getAddUserToRoot () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getCreateSshFolder () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getCreateAppscaleFolder () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getInstallGit () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getGitAppscale () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getGitAppscaleTools () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getInstallZookeeper () ) );
            resultCheck ( result );

            List<String> zookeeperStopAndDisable = Commands.getZookeeperStopAndDisable ();
            for ( String z : zookeeperStopAndDisable )
            {
                result = containerHostById.execute ( new RequestBuilder ( z ) );
                resultCheck ( result );
            }
            result = containerHostById.execute ( new RequestBuilder ( Commands.getEditZookeeperConf () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getEditAppscaleInstallSH () ) );
            resultCheck ( result );


            // last commands if all went good.
            result = containerHostById.execute ( new RequestBuilder ( Commands.getAppscaleBuild () ) );
            resultCheck ( result );
            result = containerHostById.execute ( new RequestBuilder ( Commands.getAppscaleToolsBuild () ) );
            resultCheck ( result );


            // check: tide up all and save to db
            trackerOperation.addLog ( "Configuration is finished" );
            appScaleConfig.setEnvironmentId ( environment.getId () );
            appScaleImpl.getPluginDAO ().saveInfo ( AppScaleConfig.PRODUCT_KEY, configBase.getClusterName (),
                                                    configBase );
            trackerOperation.addLogDone ( "Appscale is saved to database" );

            // now it is time to make ip changes and init the appscale

        }
        catch ( ContainerHostNotFoundException ex )
        {
            LOG.error ( "No environment found..." + ex.getLocalizedMessage () );
            trackerOperation.addLog ( "error getting environment for container...." );
        }
        catch ( CommandException ex )
        {
            LOG.error ( ex.getLocalizedMessage () );
        }

    }


    private void resultCheck ( CommandResult result )
    {
        if ( result.hasCompleted () )
        {
            trackerOperation.addLogDone ( result.getStdOut () );
        }
        else
        {
            trackerOperation.addLogFailed ( result.getStdErr () );
        }
    }

}

