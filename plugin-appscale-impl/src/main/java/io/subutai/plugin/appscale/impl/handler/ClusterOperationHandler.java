/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl.handler;


import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;
import io.subutai.plugin.appscale.impl.ClusterConfiguration;
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
    private ClusterOperationType clusterOperationType;
    private AppScaleConfig appScaleConfig;
    private AppScaleImpl appScaleImpl;


    public ClusterOperationHandler( AppScaleImpl appScaleImpl, AppScaleConfig appScaleConfig,
                                    ClusterOperationType clusterOperationType )
    {
        super( appScaleImpl, appScaleConfig );
        this.appScaleConfig = appScaleConfig;
        this.clusterOperationType = clusterOperationType;
        String msg = String.format( "Starting %s operation on %s(%s) cluster...", clusterOperationType, clusterName,
                                    appScaleConfig.getProductKey() );
        appScaleImpl.getTracker().createTrackerOperation( AppScaleConfig.PRODUCT_KEY, msg );

    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull( appScaleConfig, "Configuration is null" );
        switch ( clusterOperationType )
        {
            case INSTALL:
            {
                setupCluster();
                break;
            }
            case UNINSTALL:
            {
                destroyCluster();
                break;
            }
            case REMOVE:
            {
                removeCluster( clusterName );
                break;
            }
            case START_ALL:
            {
                runOperationOnContainers( clusterOperationType );
                break;
            }
        }
    }


    /**
     *
     * @param cot
     *
     * run operations in containers... like changing the ip values in AppScalefile
     */
    @Override
    public void runOperationOnContainers( ClusterOperationType cot )
    {
        throw new UnsupportedOperationException( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    /**
     * set up cluster appscale with pre - requirements in tutorial
     */
    @Override
    public void setupCluster()
    {
        try
        {
            Environment env = appScaleImpl.getEnvironmentManager().loadEnvironment( clusterName );
            trackerOperation.addLog( String.format( "Configuring %s environment for %s(%s) cluster", env.getName(),
                                                    appScaleConfig.getClusterName(), appScaleConfig.getProductKey() ) );

            new ClusterConfiguration( trackerOperation, appScaleImpl ).configureCluster( appScaleConfig, env );

        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException ex )
        {
            Logger.getLogger( ClusterOperationHandler.class.getName() ).log( Level.SEVERE, null, ex );
        }
    }


    /**
     * destroy cluster process... if needed..
     */
    @Override
    public void destroyCluster()
    {
        throw new UnsupportedOperationException( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    /**
     *
     * @param clusterName
     *
     * remove cluster process... if needed...
     */
    private void removeCluster( String clusterName )
    {
        throw new UnsupportedOperationException( "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }

}

