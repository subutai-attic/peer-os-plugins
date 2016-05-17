/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.environment.EnvironmentStatus;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.identity.api.IdentityManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;
import io.subutai.plugin.appscale.impl.handler.AppscaleAlertHandler;
import io.subutai.plugin.appscale.impl.handler.ClusterOperationHandler;


public class AppScaleImpl implements AppScaleInterface, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( AppScaleImpl.class.getName() );
    private final Monitor monitor;
    private final PluginDAO pluginDAO;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private NetworkManager networkManager;
    private QuotaManager quotaManager;
    private PeerManager peerManager;
    private Environment environment;
    private AppScaleConfig appScaleConfig;
    private ExecutorService executor;


    public AppScaleImpl( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
        this.executor = SubutaiExecutors.newCachedThreadPool();
    }


    @Override
    public UUID installCluster( AppScaleConfig appScaleConfig )
    {
        LOG.info( "install cluster started" );

        Preconditions.checkNotNull( appScaleConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( appScaleConfig.getClusterName() ),
                "clusterName is empty or null" );


        AbstractOperationHandler abstractOperationHandler =
                new ClusterOperationHandler( this, appScaleConfig, ClusterOperationType.INSTALL, this.peerManager );
        LOG.info( "install cluster " + abstractOperationHandler );
        executor.execute( abstractOperationHandler );
        LOG.info( "install executor " + " tracker id: " + abstractOperationHandler.getTrackerId() );
        return abstractOperationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( AppScaleConfig appScaleConfig )
    {
        Preconditions.checkNotNull( appScaleConfig, "Configuration is null" );
        Preconditions
                .checkArgument( !Strings.isNullOrEmpty( appScaleConfig.getClusterName() ), "clusterName is empty" );
        AbstractOperationHandler abstractOperationHandler =
                new ClusterOperationHandler( this, appScaleConfig, ClusterOperationType.UNINSTALL, this.peerManager );
        executor.execute( abstractOperationHandler );
        return abstractOperationHandler.getTrackerId();
    }


    public AppScaleConfig getAppScaleConfig()
    {
        return appScaleConfig;
    }


    public void setAppScaleConfig( AppScaleConfig appScaleConfig )
    {
        this.appScaleConfig = appScaleConfig;
    }


    @Override
    public List<String> getClusterList( Environment name )
    {
        List<String> c = new ArrayList();
        Set<EnvironmentContainerHost> containerHosts = name.getContainerHosts();
        containerHosts.stream().forEach( ( ech ) -> {
            c.add( ech.toString() );
        } );
        return c;
    }


    @Override
    public UUID uninstallCluster( String clustername )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clustername ), "Cluster name is null or empty" );

        AppScaleConfig config = getCluster( clustername );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, config, ClusterOperationType.UNINSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startCluster( String clusterName )
    {

        UUID uuid = null;
        try
        {
            EnvironmentContainerHost masterContainerHost = environment.getContainerHostByHostname( clusterName );
            AbstractOperationHandler a = ( AbstractOperationHandler ) masterContainerHost
                    .execute( new RequestBuilder( Commands.getAppScaleStartCommand() ) );
            uuid = a.getTrackerId();
        }
        catch ( ContainerHostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger( AppScaleImpl.class.getName() ).log( Level.SEVERE, null, ex );
        }
        return uuid;
    }


    @Override
    public UUID stopCluster( String clusterName )
    {
        UUID uuid = null;
        try
        {
            EnvironmentContainerHost masterContainerHost = environment.getContainerHostByHostname( clusterName );
            AbstractOperationHandler a = ( AbstractOperationHandler ) masterContainerHost
                    .execute( new RequestBuilder( Commands.getAppScaleStopCommand() ) );
            uuid = a.getTrackerId();
        }
        catch ( ContainerHostNotFoundException | CommandException ex )
        {
            java.util.logging.Logger.getLogger( AppScaleImpl.class.getName() ).log( Level.SEVERE, null, ex );
        }
        return uuid;
    }


    @Override
    public UUID cleanCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment e, final TrackerOperation t,
                                                         final AppScaleConfig ac )
    {
        return null;
    }


    @Override
    public void saveConfig( final AppScaleConfig ac ) throws ClusterException
    {

    }


    @Override
    public void deleteConfig( final AppScaleConfig ac )
    {

    }


    @Override
    public AppScaleConfig getConfig( final String clusterName )
    {
        return null;
    }


/*    @Override
    public UUID growEnvironment ( AppScaleConfig appScaleConfig )
    {

        Boolean createAppEngineInstance = new AppscaleAlertHandler ( this ).createAppEngineInstance ( environment,
                                                                                                      appScaleConfig );
        if ( createAppEngineInstance )
        {
            return UUID.randomUUID ();
        }
        else
        {
            return null;
        }

    }*/


    @Override
    public UUID oneClickInstall( final AppScaleConfig appScaleConfig )
    {
        return null;
    }


    @Override
    public UUID statusCluster( String clusterName )
    {
        throw new UnsupportedOperationException(
                "Not supported yet." ); //To change body of generated methods, choose Tools | Templates.
    }


    @Override
    public List<AppScaleConfig> getClusters()
    {
        List<AppScaleConfig> returnConfig = new ArrayList();
        List<AppScaleConfig> info = pluginDAO.getInfo( AppScaleConfig.PRODUCT_KEY, AppScaleConfig.class );
        for ( AppScaleConfig c : info )
        {
            try
            {
                Environment loadEnvironment = environmentManager.loadEnvironment( c.getEnvironmentId() );
                if ( EnvironmentStatus.HEALTHY == loadEnvironment.getStatus() )
                {
                    returnConfig.add( c );
                }
            }
            catch ( EnvironmentNotFoundException ex )
            {

            }
        }
        return returnConfig;
    }


    @Override
    public AppScaleConfig getCluster( String string )
    {
        return pluginDAO.getInfo( AppScaleConfig.PRODUCT_KEY, string, AppScaleConfig.class );
    }


    @Override
    public UUID addNode( final String s, final String s1 )
    {
        return null;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public NetworkManager getNetworkManager()
    {
        return networkManager;
    }


    public void setNetworkManager( NetworkManager networkManager )
    {
        this.networkManager = networkManager;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public Environment getEnvironment()
    {
        return environment;
    }


    public void setEnvironment( Environment environment )
    {
        this.environment = environment;
    }


    public static Logger getLOG()
    {
        return LOG;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {

    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {

    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String s )
    {

    }


    @Override
    public void onEnvironmentDestroyed( final String s )
    {

    }
}

