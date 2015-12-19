/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
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

    }

}

