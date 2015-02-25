//package org.safehaus.subutai.plugin.mongodb.impl.handler;
//
//
//import org.safehaus.subutai.common.environment.Environment;
//import org.safehaus.subutai.common.peer.ContainerHost;
//import org.safehaus.subutai.common.util.UUIDUtil;
//import org.safehaus.subutai.core.env.api.exception.EnvironmentCreationException;
//import org.safehaus.subutai.core.metric.api.MonitorException;
//import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
//import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
//import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
//import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
//import org.safehaus.subutai.plugin.mongodb.impl.MongoImpl;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
///**
// * Handles install mongo cluster operation
// */
//public class InstallOperationHandler extends AbstractOperationHandler<MongoImpl, MongoClusterConfig>
//{
//
//    private final MongoClusterConfig config;
//    private static final Logger LOGGER = LoggerFactory.getLogger( InstallOperationHandler.class );
//
//
//    public InstallOperationHandler( final MongoImpl manager, final MongoClusterConfig config )
//    {
//        super( manager, config.getClusterName() );
//        this.config = config;
//        trackerOperation = manager.getTracker().createTrackerOperation( MongoClusterConfig.PRODUCT_KEY,
//                String.format( "Setting up %s cluster...", config.getClusterName() ) );
//    }
//
//
//    @Override
//    public void run()
//    {
//
//        trackerOperation.addLog( "Building environment..." );
//
//        try
//        {
//            Environment env = manager.getEnvironmentManager().createEnvironment(
//                    String.format( "%s-%s", MongoClusterConfig.PRODUCT_KEY, UUIDUtil.generateTimeBasedUUID() ),
//                    config.getTopology(), false );
//            config.setEnvironmentId( env.getId() );
//            for ( final ContainerHost containerHost : env.getContainerHosts() )
//            {
//                if ( config.getConfigHostIds().size() != config.getNumberOfConfigServers() )
//                {
//                    config.getConfigHostIds().add( containerHost.getId() );
//                }
//                else if ( config.getRouterHostIds().size() != config.getNumberOfRouters() )
//                {
//                    config.getRouterHostIds().add( containerHost.getId() );
//                }
//                else if ( config.getDataHostIds().size() != config.getNumberOfDataNodes() )
//                {
//                    config.getDataHostIds().add( containerHost.getId() );
//                }
//            }
//            ClusterSetupStrategy clusterSetupStrategy =
//                    manager.getClusterSetupStrategy( env, config, trackerOperation );
//            clusterSetupStrategy.setup();
//
//            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
//            manager.subscribeToAlerts( env );
//        }
//        catch ( ClusterSetupException | MonitorException | EnvironmentCreationException e )
//        {
//
//            trackerOperation.addLogFailed( String.format( "Failed to setup cluster %s", clusterName ) );
//            LOGGER.error( String.format( "Failed to setup cluster: %s", clusterName ), e );
//        }
//    }
//}
