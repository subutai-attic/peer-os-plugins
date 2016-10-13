package io.subutai.plugin.hadoop.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.Node;
import io.subutai.common.environment.NodeSchema;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.util.UUIDUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.network.api.NetworkManager;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.handler.AddOperationHandler;
import io.subutai.plugin.hadoop.impl.handler.ClusterOperationHandler;
import io.subutai.plugin.hadoop.impl.handler.NodeOperationHandler;
import io.subutai.plugin.hadoop.impl.handler.RemoveNodeOperationHandler;
import io.subutai.webui.api.WebuiModule;


public class HadoopImpl implements Hadoop, EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( HadoopImpl.class.getName() );
    private Tracker tracker;
    private ExecutorService executor;
    private EnvironmentManager environmentManager;
    private PluginDAO pluginDAO;
    private Monitor monitor;
    private QuotaManager quotaManager;
    private PeerManager peerManager;
    private NetworkManager networkManager;
    private StrategyManager strategyManager;
    private HadoopWebModule webModule;
    private TemplateManager templateManager;

    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );


    public HadoopImpl( StrategyManager strategyManager, Monitor monitor, PluginDAO pluginDAO,
                       HadoopWebModule webModule )
    {
        this.strategyManager = strategyManager;
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
        this.webModule = webModule;
    }


    public StrategyManager getStrategyManager()
    {
        return strategyManager;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
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
    public UUID startNameNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName, "Cluster name is null" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.START, NodeType.NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopNameNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName, "Cluster name is null" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.STOP, NodeType.NAMENODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID statusNameNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName, "Cluster name is null" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.STATUS, NodeType.NAMENODE );
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
    public UUID statusDataNode( String clusterName, String hostName )
    {
        Preconditions.checkNotNull( clusterName, "Cluster name is null" );
        AbstractOperationHandler operationHandler =
                new NodeOperationHandler( this, clusterName, hostName, NodeOperationType.STATUS, NodeType.DATANODE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName, int nodeCount )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new AddOperationHandler( this, templateManager, clusterName, nodeCount );
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


    @Override
    public Blueprint getDefaultEnvironmentBlueprint( final HadoopClusterConfig config ) throws ClusterSetupException
    {

        List<NodeSchema> schema = new ArrayList<>();
        //        schema.add( new NodeSchema( "hadoop-master-1", ContainerSize.TINY, "hadoop", 1, 1 ) );
        //        schema.add( new NodeSchema( "hadoop-master-2", ContainerSize.TINY, "hadoop", 1, 1 ) );
        //        schema.add( new NodeSchema( "hadoop-master-2", ContainerSize.TINY, "hadoop", 1, 1 ) );
        //        for ( int i = 0; i < config.getCountOfSlaveNodes(); i++ )
        //        {
        //            schema.add( new NodeSchema( "hadoop-slave-" + ( i + 1 ), ContainerSize.SMALL, "hadoop", 1, 1 ) );
        //        }
        //        NodeGroup nodeGroup = new NodeGroup( "Hadoop node group", HadoopClusterConfig.TEMPLATE_NAME,
        //                HadoopClusterConfig.DEFAULT_HADOOP_MASTER_NODES_QUANTITY + config.getCountOfSlaveNodes(),
        // 1, 1,
        //                peerManager.getLocalPeer().getId(), resourceHostId );
        return new Blueprint(
                String.format( "%s-%s", HadoopClusterConfig.PRODUCT_KEY, UUIDUtil.generateTimeBasedUUID() ), schema );
    }


    @Override
    public void saveConfig( final HadoopClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created " + environment.toString() );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        String hostNames = "";
        for ( final EnvironmentContainerHost containerHost : set )
        {
            hostNames += containerHost.getHostname() + "; ";
        }
        LOG.info( String.format( "Environment: %s bred with containers: %s", environment.getName(), hostNames ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String id )
    {
        List<HadoopClusterConfig> clusterConfigs = getClusters();
        for ( final HadoopClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getAllNodes().contains( id ) )
                {
                    //                    clusterConfig.removeNode( id );
                    getPluginDAO()
                            .saveInfo( HadoopClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(), clusterConfig );
                    LOG.info( String.format( "Container host: %s removed from cluster: %s with environment id: %s", id,
                            clusterConfig.getClusterName(), clusterConfig.getEnvironmentId() ) );
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String envId )
    {
        List<HadoopClusterConfig> clusterConfigs = getClusters();
        for ( final HadoopClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( envId ) )
            {
                LOG.info(
                        String.format( "Hadoop cluster: %s destroyed in environment %s", clusterConfig.getClusterName(),
                                envId ) );
                getPluginDAO().deleteInfo( HadoopClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
            }
        }
    }


    @Override
    public WebuiModule getWebModule()
    {
        return webModule;
    }


    @Override
    public void setWebModule( final WebuiModule webModule )
    {
        this.webModule = ( HadoopWebModule ) webModule;
    }


    public void setTemplateManager( final TemplateManager templateManager )
    {
        this.templateManager = templateManager;
    }
}
