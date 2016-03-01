/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.usergrid.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.usergrid.api.UsergridConfig;
import io.subutai.usergrid.impl.ClusterConfiguration;
import io.subutai.usergrid.impl.UsergridIMPL;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class ClusterOperationHandler extends AbstractOperationHandler<UsergridIMPL, UsergridConfig>
        implements ClusterOperationHandlerInterface
{

    private final ClusterOperationType operationType;
    private final UsergridConfig config;

    private final String clusterName;
    private static final Logger LOG = LoggerFactory.getLogger ( ClusterOperationHandler.class.getName () );


    public ClusterOperationHandler ( final UsergridIMPL manager, final UsergridConfig config,
                                     final ClusterOperationType operationType )
    {
        super ( manager, config );
        this.operationType = operationType;
        this.config = config;
        clusterName = config.getClusterName ();
        trackerOperation = manager.getTracker ().createTrackerOperation ( UsergridConfig.getPACKAGE_NAME (), "starting" );
    }


    @Override
    public void run ()
    {
        Preconditions.checkNotNull ( config, "Configuration is null" );
        switch ( operationType )
        {
            case INSTALL:
            {
                setupCluster ();
                break;
            }
        }

    }


    @Override
    public void runOperationOnContainers ( ClusterOperationType cot )
    {
        new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void setupCluster ()
    {
        LOG.info ( "setupCluster started..." );
        Environment env = null;

        try
        {
            env = manager.getEnvironmentManager ().loadEnvironment ( config.getEnvironmentId () );
            trackerOperation.addLog ( String.format (
                    "Configuring environment name: %s for cluster name: %s(%s)", env.getName (),
                    config.getClusterName (), config.getProductKey () ) );
            LOG.info ( "tracker operations" );
        }
        catch ( EnvironmentNotFoundException ex )
        {
            LOG.error ( "load environment: " + ex );
        }

        LOG.info ( "Before" );
        try
        {
            new ClusterConfiguration ( trackerOperation, manager ).configureCluster ( config, env );
        }
        catch ( ClusterConfigurationException ex )
        {
            LOG.error ( "ClusterConfigurationHandler :> " + ex );
        }
        LOG.info ( "After" );

    }


    @Override
    public void destroyCluster ()
    {
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }

}

