/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.usergrid.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.core.plugincommon.api.ConfigBase;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterConfiguration implements ClusterConfigurationInterface
{

    private final TrackerOperation po;
    private final UsergridIMPL usergridImplManager;

    private static final Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public ClusterConfiguration ( final TrackerOperation operation, final UsergridIMPL usergridIMPL )
    {
        this.po = operation;
        this.usergridImplManager = usergridIMPL;
    }


    @Override
    public void configureCluster ( ConfigBase configBase, Environment environment ) throws ClusterConfigurationException
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }

}

