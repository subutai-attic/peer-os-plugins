package io.subutai.plugin.zookeeper.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.peer.api.PeerManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.zookeeper.api.CommandType;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.handler.AddPropertyOperationHandler;
import io.subutai.plugin.zookeeper.impl.handler.RemovePropertyOperationHandler;
import io.subutai.plugin.zookeeper.impl.handler.ZookeeperClusterOperationHandler;
import io.subutai.plugin.zookeeper.impl.handler.ZookeeperNodeOperationHandler;


//TODO: Add parameter validation
public class ZookeeperImpl implements Zookeeper, EnvironmentEventListener
{

    private static final Logger LOG = LoggerFactory.getLogger( ZookeeperImpl.class );
    private final MonitoringSettings alertSettings = new MonitoringSettings().withIntervalBetweenAlertsInMin( 45 );

    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Hadoop hadoopManager;
    private Commands commands;
    private Monitor monitor;
    private ExecutorService executor;
    private PeerManager peerManager;

    private PluginDAO pluginDAO;
    private QuotaManager quotaManager;
    private TemplateManager templateManager;


    public ZookeeperImpl( Monitor monitor, PluginDAO pluginDAO )
    {
        this.monitor = monitor;
        this.pluginDAO = pluginDAO;
    }


    public Commands getCommands()
    {
        return commands;
    }


    public PluginDAO getPluginDAO()
    {
        return pluginDAO;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public Monitor getMonitor()
    {
        return monitor;
    }


    public MonitoringSettings getAlertSettings()
    {
        return alertSettings;
    }


    public void setExecutor( final ExecutorService executor )
    {
        this.executor = executor;
    }


    public void setCommands( final Commands commands )
    {
        this.commands = commands;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public PeerManager getPeerManager()
    {
        return peerManager;
    }


    public void setPeerManager( final PeerManager peerManager )
    {
        this.peerManager = peerManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void init()
    {
        executor = SubutaiExecutors.newCachedThreadPool();
    }


    public void destroy()
    {
        executor.shutdown();
    }


    public UUID installCluster( ZookeeperClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, config, ClusterOperationType.INSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    public UUID uninstallCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.UNINSTALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID configureEnvironmentCluster( ZookeeperClusterConfig config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, config, ClusterOperationType.INSTALL );

        executor.execute( operationHandler );

        return operationHandler.getTrackerId();
    }


    @Override
    public void saveConfig( final ZookeeperClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final ZookeeperClusterConfig config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !pluginDAO.deleteInfo( ZookeeperClusterConfig.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    public List<ZookeeperClusterConfig> getClusters()
    {

        return pluginDAO.getInfo( ZookeeperClusterConfig.PRODUCT_KEY, ZookeeperClusterConfig.class );
    }


    @Override
    public ZookeeperClusterConfig getCluster( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        return pluginDAO.getInfo( ZookeeperClusterConfig.PRODUCT_KEY, clusterName, ZookeeperClusterConfig.class );
    }


    @Override
    public UUID startNode( String clusterName, String hostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID startAllNodes( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.START_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID stopAllNodes( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, getCluster( clusterName ), ClusterOperationType.STOP_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID checkAllNodes( final String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperClusterOperationHandler( this, getCluster( clusterName ),
                        ClusterOperationType.STATUS_ALL );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID stopNode( String clusterName, String hostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID checkNode( String clusterName, String hostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Hostname is null or empty" );


        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID addNode( String clusterName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        ZookeeperClusterConfig zookeeperClusterConfig = getCluster( clusterName );

        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, templateManager, zookeeperClusterConfig,
                        NodeOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID addNode( String clusterName, String lxcHostname )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostname ), "Lxc hostname is null or empty" );
        ZookeeperClusterConfig zookeeperClusterConfig = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, zookeeperClusterConfig.getClusterName(), lxcHostname,
                        NodeOperationType.ADD );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    public UUID destroyNode( String clusterName, String lxcHostName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( lxcHostName ), "Lxc hostname is null or empty" );

        AbstractOperationHandler operationHandler =
                new ZookeeperNodeOperationHandler( this, clusterName, lxcHostName, NodeOperationType.DESTROY );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID addProperty( String clusterName, String fileName, String propertyName, String propertyValue )
    {

        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( fileName ), "File name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyName ), "Property name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyValue ), "Property value is null or empty" );

        AbstractOperationHandler operationHandler =
                new AddPropertyOperationHandler( this, clusterName, fileName, propertyName, propertyValue );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID removeProperty( String clusterName, String fileName, String propertyName )
    {
        Preconditions.checkArgument( !Strings.isNullOrEmpty( clusterName ), "Cluster name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( fileName ), "File name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyName ), "Property name is null or empty" );

        AbstractOperationHandler operationHandler =
                new RemovePropertyOperationHandler( this, clusterName, fileName, propertyName );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment,
                                                         final ZookeeperClusterConfig config,
                                                         final TrackerOperation po )
    {
        Preconditions.checkNotNull( config, "Zookeeper cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation is null" );
        if ( config.getSetupType() != SetupType.OVER_HADOOP && config.getSetupType() != SetupType.OVER_ENVIRONMENT )
        {
            Preconditions.checkNotNull( environment, "Environment is null" );
        }

        if ( config.getSetupType() == SetupType.STANDALONE )
        {
            //this is a standalone ZK cluster setup
            return new ZookeeperStandaloneSetupStrategy( environment, config, po, this );
        }
        else if ( config.getSetupType() == SetupType.WITH_HADOOP )
        {
            //this is a with-Hadoop ZK cluster setup
            return new ZookeeperWithHadoopSetupStrategy( environment, config, po, this );
        }
        else if ( config.getSetupType() == SetupType.OVER_ENVIRONMENT )
        {
            //while installing zoo over existing environment
            return new ZookeeperOverEnvironmentSetupStrategy( environment, config, po, this );
        }
        else
        {
            //this is an over-Hadoop ZK cluster setup
            return new ZookeeperOverHadoopSetupStrategy( environment, config, po, this );
        }
    }


    //these command substitutions are not valid,
    // this was made so to omit wrong command execution
    @Override
    public String getCommand( final CommandType commandType )
    {
        switch ( commandType )
        {
            case INSTALL:
                return Commands.getInstallCommand();
            case UNINSTALL:
                return Commands.getStartCommand();
            case START:
                return Commands.getStartCommand();
            case STOP:
                return Commands.getStopCommand();
            case STATUS:
                return Commands.getStatusCommand();
        }
        return null;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created" );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        LOG.info( "Environment grown" );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String nodeId )
    {
        List<ZookeeperClusterConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final ZookeeperClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                if ( clusterConfig.getNodes().contains( nodeId ) )
                {
                    clusterConfig.getNodes().remove( nodeId );
                    getPluginDAO().saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName(),
                            clusterConfig );
                    LOG.info( "Container host destroyed from environment." );
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String environmentId )
    {
        List<ZookeeperClusterConfig> clusterConfigs = new ArrayList<>( getClusters() );
        for ( final ZookeeperClusterConfig clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environmentId ) )
            {
                getPluginDAO().deleteInfo( ZookeeperClusterConfig.PRODUCT_KEY, clusterConfig.getClusterName() );
                LOG.info( "Environment destroyed from cluster: " + clusterConfig.getClusterName() );
            }
        }
    }


    public void setTemplateManager( final TemplateManager templateManager )
    {
        this.templateManager = templateManager;
    }
}
