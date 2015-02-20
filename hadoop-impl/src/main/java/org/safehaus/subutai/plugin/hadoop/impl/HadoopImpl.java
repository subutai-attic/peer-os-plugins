package org.safehaus.subutai.plugin.hadoop.impl;


import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.Blueprint;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.UUIDUtil;
import org.safehaus.subutai.core.env.api.EnvironmentEventListener;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.lxc.quota.api.QuotaManager;
import org.safehaus.subutai.core.metric.api.Monitor;
import org.safehaus.subutai.core.metric.api.MonitorException;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.network.api.NetworkManager;
import org.safehaus.subutai.core.peer.api.PeerManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.PluginDAO;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.alert.HadoopAlertListener;
import org.safehaus.subutai.plugin.hadoop.impl.handler.AddOperationHandler;
import org.safehaus.subutai.plugin.hadoop.impl.handler.ClusterOperationHandler;
import org.safehaus.subutai.plugin.hadoop.impl.handler.ConfigureEnvironmentClusterHandler;
import org.safehaus.subutai.plugin.hadoop.impl.handler.NodeOperationHandler;
import org.safehaus.subutai.plugin.hadoop.impl.handler.RemoveNodeOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;


public class HadoopImpl implements Hadoop, EnvironmentEventListener
{
    public static final int INITIAL_CAPACITY = 2;
    private static final Logger LOG = LoggerFactory.getLogger( HadoopImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Monitor monitor;
    private QuotaManager quotaManager;
    private PeerManager peerManager;
    private NetworkManager networkManager;


    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );
    private HadoopAlertListener hadoopAlertListener;


    public HadoopImpl( Monitor monitor )
    {
        this.monitor = monitor;
        hadoopAlertListener = new HadoopAlertListener( this );
        monitor.addAlertListener( hadoopAlertListener );
    }

    public void subscribeToAlerts( Environment environment ) throws MonitorException
    {
        getMonitor().startMonitoring( hadoopAlertListener, environment, alertSettings );
    }


    public void subscribeToAlerts( ContainerHost host ) throws MonitorException
    {
        getMonitor().activateMonitoring( host, alertSettings );
    }


    public void unsubscribeFromAlerts( final Environment environment ) throws MonitorException
    {
        getMonitor().stopMonitoring( hadoopAlertListener, environment );
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void init()
    {
        try
        {
            this.pluginDAO = new PluginDAO( null );
        }
        catch ( SQLException e )
        {
            LOG.error( e.getMessage(), e );
        }
        executor = Executors.newCachedThreadPool();
    }


    public void setPluginDAO( final PluginDAO pluginDAO )
    {
        this.pluginDAO = pluginDAO;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public NetworkManager getNetworkManager()
    {
        return networkManager;
    }


    public void setNetworkManager( final NetworkManager networkManager )
    {
        this.networkManager = networkManager;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public ExecutorService getExecutor()
    {
        return executor;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public void setMonitor( final Monitor monitor )
    {
        this.monitor = monitor;
    }


    @Override
    public UUID installCluster( final HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.INSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( final HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.UNINSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID removeCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        HadoopClusterConfig hadoopClusterConfig = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.REMOVE, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public List<HadoopClusterConfig> getClusters()
    {
        return pluginDAO.getInfo( HadoopClusterConfig.PRODUCT_KEY, HadoopClusterConfig.class );
    }


    @Override
    public HadoopClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        return pluginDAO.getInfo( HadoopClusterConfig.PRODUCT_KEY, clusterName, HadoopClusterConfig.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    @Override
    public UUID uninstallCluster( final String clustername )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clustername ), "Cluster name is null or empty" );
        HadoopClusterConfig hadoopClusterConfig = getCluster( clustername );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.UNINSTALL, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startNameNode( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.START_ALL,
                        NodeType.NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNameNode( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.STOP_ALL,
                        NodeType.NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusNameNode( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.STATUS_ALL,
                        NodeType.NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusSecondaryNameNode( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.STATUS_ALL,
                        NodeType.SECONDARY_NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname, NodeOperationType.START,
                        NodeType.DATANODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname, NodeOperationType.STOP,
                        NodeType.DATANODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname,
                        NodeOperationType.STATUS, NodeType.DATANODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startJobTracker( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.START_ALL,
                        NodeType.JOBTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopJobTracker( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.STOP_ALL,
                        NodeType.JOBTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusJobTracker( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.STATUS_ALL,
                        NodeType.JOBTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname, NodeOperationType.START,
                        NodeType.TASKTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname, NodeOperationType.STOP,
                        NodeType.TASKTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname,
                        NodeOperationType.STATUS, NodeType.TASKTRACKER );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName, int nodeCount )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler = new AddOperationHandler( this, clusterName, nodeCount );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName )
    {
        return addNode( clusterName, 1 );
    }


    @Override
    public UUID destroyNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new RemoveNodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkDecomissionStatus( HadoopClusterConfig hadoopClusterConfig )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        AbstractOperationHandler operationHandler =
                new ClusterOperationHandler( this, hadoopClusterConfig, ClusterOperationType.DECOMISSION_STATUS, null );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID excludeNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname,
                        NodeOperationType.EXCLUDE, NodeType.SLAVE_NODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID includeNode( HadoopClusterConfig hadoopClusterConfig, String hostname )
    {
        Preconditions.checkNotNull( hadoopClusterConfig, "Configuration is null" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hadoopClusterConfig.getClusterName() ),
                "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, hadoopClusterConfig.getClusterName(), hostname,
                        NodeOperationType.INCLUDE, NodeType.SLAVE_NODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment,
                                                         HadoopClusterConfig hadoopClusterConfig, TrackerOperation po )
    {
        return new HadoopSetupStrategy( environment, hadoopClusterConfig, po, this );
    }


    @Override
    public Blueprint getDefaultEnvironmentBlueprint( final HadoopClusterConfig config ) throws ClusterSetupException
    {

        NodeGroup nodeGroup =
                new NodeGroup( "Hadoop node group", HadoopClusterConfig.TEMPLATE_NAME, Common.DEFAULT_DOMAIN_NAME,
                        HadoopClusterConfig.DEFAULT_HADOOP_MASTER_NODES_QUANTITY + config.getCountOfSlaveNodes(), 1, 1,
                        new PlacementStrategy( "ROUND_ROBIN" ) );
        return new Blueprint(
                String.format( "%s-%s", HadoopClusterConfig.PRODUCT_KEY, UUIDUtil.generateTimeBasedUUID() ),
                Sets.newHashSet( nodeGroup ) );
    }


    public UUID configureEnvironmentCluster( final HadoopClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler = new ConfigureEnvironmentClusterHandler( this, config );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created " + environment.toString() );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<ContainerHost> set )
    {
        String hostNames = "";
        for ( final ContainerHost containerHost : set )
        {
            hostNames += containerHost.getHostname() + "; ";
        }
        LOG.info( String.format( "Environment: %s bred with containers: %s", environment.getName(), hostNames ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final UUID uuid )
    {
        List<HadoopClusterConfig> clusterConfigs = getClusters();
        for ( final HadoopClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getAllNodes().contains( uuid ) )
                {
                    clusterConfig.removeNode( uuid );
                    getPluginDAO()
                            .saveInfo( HadoopClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(), clusterConfig );
                    LOG.info( String.format( "Container host: %s removed from cluster: %s with environment id: %s",
                            uuid.toString(), clusterConfig.getClusterName(),
                            clusterConfig.getEnvironmentId().toString() ) );
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final UUID uuid )
    {
        List<HadoopClusterConfig> clusterConfigs = getClusters();
        for ( final HadoopClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info(
                        String.format( "Hadoop cluster: %s destroyed in environment %s", clusterConfig.getClusterName(),
                                uuid.toString() ) );
                getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
            }
        }
    }
}
