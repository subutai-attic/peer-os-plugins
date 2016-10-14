/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;
import io.subutai.plugin.appscale.impl.ClusterConfiguration;
import io.subutai.plugin.appscale.impl.Commands;


public class ClusterOperationHandler extends AbstractOperationHandler<AppScaleImpl, AppScaleConfig>
        implements ClusterOperationHandlerInterface
{
    private final ClusterOperationType operationType;
    private final AppScaleConfig config;
    private final PeerManager peerManager;
    private final String clstrName;
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class.getName() );


    public ClusterOperationHandler( final AppScaleImpl manager, final AppScaleConfig config,
                                    final ClusterOperationType operationType, PeerManager peerManager )
    {
        super( manager, config );
        this.config = config;
        clstrName = config.getClusterName();
        this.operationType = operationType;
        String msg = String.format( "Starting %s operation on %s(%s) cluster...", operationType, clstrName,
                config.getProductKey() );
        this.peerManager = peerManager;

        LOG.info( msg );
        trackerOperation = manager.getTracker().createTrackerOperation( AppScaleConfig.PRODUCT_KEY, msg );
        if ( trackerOperation == null )
        {
            LOG.error( "trackerOperation is null " );
        }
        else
        {
            LOG.info( "trackerOperation: " + trackerOperation );
        }
    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        switch ( operationType )
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
                removeCluster( clstrName );
                break;
            }
            case START_ALL:
            {
                runOperationOnContainers( operationType );
                break;
            }

            case DECOMISSION_STATUS:
            {

            }
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType cot )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getClusterName() );
            EnvironmentContainerHost containerHostById =
                    environment.getContainerHostByHostname( config.getClusterName() );
            CommandResult res;
            switch ( cot )
            {
                case START_ALL:
                {
                    Environment env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
                    Set<EnvironmentContainerHost> containerHosts = env.getContainerHosts();
                    int numberOfContainers = containerHosts.size();
                    String cmd = Commands.getAppScaleStartCommand() + Integer.toString( numberOfContainers );
                    res = containerHostById.execute( new RequestBuilder( cmd ) );
                    if ( res.hasSucceeded() )
                    {
                        trackerOperation.addLogDone( res.getStdOut() );
                    }
                    else
                    {
                        trackerOperation.addLogFailed( res.getStdErr() );
                    }
                    break;
                }
                case STOP_ALL:
                {
                    res = containerHostById.execute( new RequestBuilder( Commands.getAppScaleStopCommand() ) );
                    if ( res.hasSucceeded() )
                    {
                        trackerOperation.addLogDone( res.getStdOut() );
                    }
                    else
                    {
                        trackerOperation.addLogFailed( res.getStdErr() );
                    }
                    break;
                }
            }
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException ex )
        {
            LOG.error( ex.getLocalizedMessage() );
        }
    }


    @Override
    public void setupCluster()
    {
        LOG.info( "setupCluster started..." );
        Environment env = null;

        try
        {
            env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            LOG.info( "ENV found: " + env.getName() );
            trackerOperation.addLog(
                    String.format( "Configuring environment name: %s for cluster name: %s(%s)", env.getName(),
                            config.getClusterName(), config.getProductKey() ) );
            LOG.info( "tracker operations" );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "EnvironmentNotFound: " + e );
        }


        LOG.info( String.format( "Configuring environment name: %s for cluster name: %s(%s)", env.getName(),
                config.getClusterName(), config.getProductKey() ) );
        try
        {
            ClusterConfiguration configuration =
                    new ClusterConfiguration( this.trackerOperation, this.manager, this.peerManager );
            configuration.configureCluster( config, env );
        }
        catch ( ClusterConfigurationException cce )
        {
            LOG.error( "ClusterConfigurationException: " + cce );
        }
    }


    /**
     * destroy cluster process... if needed..
     */
    @Override
    public void destroyCluster()
    {
        LOG.info( "Destroying cluster..." );
        Environment env = null;

        try
        {
            env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

            ContainerHost controllerHost = env.getContainerHostByHostname( config.getControllerNode() );

            CommandResult result = controllerHost.execute(
                    new RequestBuilder( "sudo /var/lib/appscale-tools/bin/appscale down" ).withTimeout( 10000 )
                                                                                          .withCwd( "root" ) );

            if ( result.hasSucceeded() )
            {
                if ( manager.getPluginDAO().deleteInfo( AppScaleConfig.PRODUCT_KEY, config.getClusterName() ) )
                {
                    trackerOperation.addLogDone( "Cluster information deleted from database" );
                    LOG.info( "Cluster information deleted from database" );
                }
                else
                {
                    trackerOperation.addLogFailed( "Failed to delete cluster information from database" );
                }
            }
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException | CommandException ex )
        {
            trackerOperation.addLogFailed( "Destroy cluster failed..." );
            LOG.error( "Destroy cluster failed..." );
        }
    }


    /**
     * @param clusterName remove cluster process... if needed...
     */
    private void removeCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }
}

