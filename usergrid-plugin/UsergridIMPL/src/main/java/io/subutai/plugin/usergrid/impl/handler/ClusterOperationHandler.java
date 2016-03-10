/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.impl.ClusterConfiguration;
import io.subutai.plugin.usergrid.impl.UsergridIMPL;


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
        trackerOperation = manager.getTracker ().createTrackerOperation ( UsergridConfig.PACKAGE_NAME, "starting" );
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
        throw new UnsupportedOperationException ( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public void setupCluster ()
    {
        LOG.info ( "setupCluster started..." );
        Environment env = null;

        try
        {
            LOG.info ( "before finding env..." );
            LOG.info ( "envid: " + config.getEnvironmentId () );
            LOG.info ( "clusterName: " + config.getClusterName () );
            LOG.info ( "cassandraName: " + config.getCassandraName () );
            LOG.info ( "elastic: " + config.getElasticSName () );
            LOG.info ( "userdomain: " + config.getUserDomain () );
            env = manager.getEnvironmentManager ().loadEnvironment ( config.getEnvironmentId () );
            trackerOperation.addLog ( String.format (
                    "Configuring environment name: %s for cluster name: %s(%s)", env.getName (),
                    config.getClusterName (), config.getProductKey () ) );
            LOG.info ( "environment: " + env );
        }
        catch ( EnvironmentNotFoundException ex )
        {
            LOG.error ( "load environment: " + ex );
        }
        try
        {
            new ClusterConfiguration ( trackerOperation, manager ).configureCluster ( config, env );
        }
        catch ( ClusterConfigurationException ex )
        {
            LOG.error ( "ClusterConfigurationHandler :> " + ex );
        }

    }


    @Override
    public void destroyCluster ()
    {
        try
        {
            Environment env = manager.getEnvironmentManager ().loadEnvironment ( clusterName );
            Set<EnvironmentContainerHost> containerHosts = env.getContainerHosts ();
            containerHosts.stream ().forEach ( (cont)
                    ->
                    {
                        try
                        {
                            env.destroyContainer ( cont, true );
                        }
                        catch ( EnvironmentNotFoundException | EnvironmentModificationException ex )
                        {
                            LOG.error ( "Destroy container error: " + ex );
                        }
            } );
        }
        catch ( EnvironmentNotFoundException ex )
        {
            LOG.error ( "destroy container environment not found error: " + ex );
        }
    }

}

