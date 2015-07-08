package io.subutai.plugin.lucene.impl.handler;


import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;
import io.subutai.plugin.lucene.impl.LuceneImpl;
import io.subutai.plugin.lucene.impl.Commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class ClusterOperationHandler extends AbstractOperationHandler<LuceneImpl, LuceneConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private LuceneConfig config;


    public ClusterOperationHandler( final LuceneImpl manager, final LuceneConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( LuceneConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {

        try
        {
            HadoopClusterConfig hadoopClusterConfig =
                    manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throw new ClusterSetupException(
                        String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
            }

            Environment env;
            try
            {
                env = manager.getEnvironmentManager().findEnvironment( hadoopClusterConfig.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterSetupException( e );
            }

            ClusterSetupStrategy s = manager.getClusterSetupStrategy( env, config, trackerOperation );
            s.setup();
            trackerOperation.addLogDone( "Done" );
        }
        catch ( ClusterSetupException ex )
        {
            trackerOperation.addLogFailed( "Failed to setup cluster: " + ex.getMessage() );
        }
    }


    @Override
    public void destroyCluster()
    {
        LuceneConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        TrackerOperation po = trackerOperation;
        po.addLog( "Uninstalling Lucene..." );

        Set<ContainerHost> nodes;
        try
        {
            nodes = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                           .getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed obtaining environment containers: %s", e ) );
            return;
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Environment not found: %s", e ) );
            return;
        }
        for ( ContainerHost containerHost : nodes )
        {
            CommandResult result;
            try
            {
                result = containerHost.execute( new RequestBuilder( Commands.uninstallCommand ) );
                if ( !result.hasSucceeded() )
                {
                    po.addLog( result.getStdErr() );
                    po.addLogFailed( "Uninstallation failed" );
                    return;
                }
            }
            catch ( CommandException e )
            {
                LOG.error( e.getMessage(), e );
            }
        }
        po.addLog( "Updating db..." );
        manager.getPluginDao().deleteInfo( LuceneConfig.PRODUCT_KEY, config.getClusterName() );
        po.addLogDone( "Cluster info deleted from DB\nDone" );
    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case DESTROY:
                destroyCluster();
                break;
        }
    }
}
