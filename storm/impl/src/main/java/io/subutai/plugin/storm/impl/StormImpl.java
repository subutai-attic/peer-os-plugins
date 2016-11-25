package io.subutai.plugin.storm.impl;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.lxc.quota.api.QuotaManager;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.template.api.TemplateManager;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.handler.ConfigureEnvironmentClusterHandler;
import io.subutai.plugin.storm.impl.handler.StormClusterOperationHandler;
import io.subutai.plugin.storm.impl.handler.StormNodeOperationHandler;


public class StormImpl extends StormBase implements EnvironmentEventListener
{
    private static final Logger LOG = LoggerFactory.getLogger( StormImpl.class.getName() );

    private StrategyManager strategyManager;
    private QuotaManager quotaManager;
    private TemplateManager templateManager;


    public StormImpl( Monitor monitor1, PluginDAO pluginDAO )
    {
        this.monitor = monitor1;
        this.pluginDAO = pluginDAO;
    }


    @Override
    public UUID installCluster( StormClusterConfiguration config )
    {
        Preconditions.checkNotNull( config, "Configuration is null" );
        AbstractOperationHandler operationHandler = new ConfigureEnvironmentClusterHandler( this, config );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public UUID uninstallCluster( String clusterName )
    {
        AbstractOperationHandler h = new StormClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                ClusterOperationType.UNINSTALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public List<StormClusterConfiguration> getClusters()
    {
        return pluginDAO.getInfo( StormClusterConfiguration.PRODUCT_NAME, StormClusterConfiguration.class );
    }


    @Override
    public StormClusterConfiguration getCluster( String clusterName )
    {
        return pluginDAO
                .getInfo( StormClusterConfiguration.PRODUCT_NAME, clusterName, StormClusterConfiguration.class );
    }


    @Override
    public UUID addNode( final String clusterName, final String agentHostName )
    {
        return null;
    }


    @Override
    public UUID startNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.START );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID startAll( String clusterName )
    {
        AbstractOperationHandler h = new StormClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                ClusterOperationType.START_ALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkAll( final String clusterName )
    {
        AbstractOperationHandler h = new StormClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                ClusterOperationType.STATUS_ALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STOP );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID stopAll( String clusterName )
    {
        AbstractOperationHandler h = new StormClusterOperationHandler( this, templateManager, getCluster( clusterName ),
                ClusterOperationType.STOP_ALL );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID checkNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.STATUS );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID restartNode( String clusterName, String hostname )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, hostname, NodeOperationType.RESTART );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID addNode( String clusterName )
    {
        StormClusterConfiguration zookeeperClusterConfig = getCluster( clusterName );

        AbstractOperationHandler h = new StormClusterOperationHandler( this, templateManager, zookeeperClusterConfig,
                ClusterOperationType.ADD );
        executor.execute( h );
        return h.getTrackerId();
    }


    @Override
    public UUID destroyNode( String clusterName, String nodeId )
    {
        AbstractOperationHandler h =
                new StormNodeOperationHandler( this, clusterName, nodeId, NodeOperationType.DESTROY );
        executor.execute( h );
        return h.getTrackerId();
    }


    public UUID removeCluster( final String clusterName )
    {
        StormClusterConfiguration config = getCluster( clusterName );
        AbstractOperationHandler operationHandler =
                new StormClusterOperationHandler( this, templateManager, config, ClusterOperationType.REMOVE );
        executor.execute( operationHandler );
        return operationHandler.getTrackerId();
    }


    @Override
    public ClusterSetupStrategy getClusterSetupStrategy( StormClusterConfiguration config, TrackerOperation po )
    {
        return new StormSetupStrategyDefault( this, config, po );
    }


    @Override
    public void saveConfig( final StormClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().saveInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not save cluster info" );
        }
    }


    @Override
    public void deleteConfig( final StormClusterConfiguration config ) throws ClusterException
    {
        Preconditions.checkNotNull( config );

        if ( !getPluginDAO().deleteInfo( StormClusterConfiguration.PRODUCT_KEY, config.getClusterName() ) )
        {
            throw new ClusterException( "Could not delete cluster info" );
        }
    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {
        LOG.info( "Environment created: " + environment.getName() );
    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {
        List<String> containerNames = new ArrayList<>();
        for ( final EnvironmentContainerHost containerHost : set )
        {
            containerNames.add( containerHost.getHostname() );
        }
        LOG.info( String.format( "Environment: %s has been grown with containers: %s", environment.getName(),
                containerNames.toString() ) );
    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String containerHostId )
    {
        LOG.info( String.format( "Storm environment event: Container destroyed: %s", containerHostId ) );
        List<StormClusterConfiguration> clusterConfigs = getClusters();
        for ( final StormClusterConfiguration clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( environment.getId() ) )
            {
                LOG.info( String.format( "Storm environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                if ( clusterConfig.getAllNodes().contains( containerHostId ) )
                {
                    LOG.info( String.format( "Storm environment event: Before: %s", clusterConfig ) );
                    if ( !CollectionUtil.isCollectionEmpty( clusterConfig.getSupervisors() ) )
                    {
                        clusterConfig.getSupervisors().remove( containerHostId );
                    }
                    try
                    {
                        saveConfig( clusterConfig );
                        LOG.info( String.format( "Storm environment event: After: %s", clusterConfig ) );
                    }
                    catch ( ClusterException e )
                    {
                        LOG.error( "Error updating cluster config", e );
                    }
                    break;
                }
            }
        }
    }


    @Override
    public void onEnvironmentDestroyed( final String uuid )
    {
        LOG.info( String.format( "Storm environment event: Environment destroyed: %s", uuid ) );

        List<StormClusterConfiguration> clusterConfigs = getClusters();
        for ( final StormClusterConfiguration clusterConfig : clusterConfigs )
        {
            if ( clusterConfig.getEnvironmentId().equals( uuid ) )
            {
                LOG.info( String.format( "Storm environment event: Target cluster: %s",
                        clusterConfig.getClusterName() ) );

                try
                {
                    deleteConfig( clusterConfig );
                    LOG.info( String.format( "Storm environment event: Cluster %s removed",
                            clusterConfig.getClusterName() ) );
                }
                catch ( ClusterException e )
                {
                    LOG.error( "Error deleting cluster config", e );
                }
                break;
            }
        }
    }


    @Override
    public void onContainerStarted( final Environment environment, final String s )
    {

    }


    @Override
    public void onContainerStopped( final Environment environment, final String s )
    {

    }


    public StrategyManager getStrategyManager()
    {
        return strategyManager;
    }


    public void setStrategyManager( final StrategyManager strategyManager )
    {
        this.strategyManager = strategyManager;
    }


    public QuotaManager getQuotaManager()
    {
        return quotaManager;
    }


    public void setQuotaManager( final QuotaManager quotaManager )
    {
        this.quotaManager = quotaManager;
    }


    public void setTemplateManager( final TemplateManager templateManager )
    {
        this.templateManager = templateManager;
    }
}
