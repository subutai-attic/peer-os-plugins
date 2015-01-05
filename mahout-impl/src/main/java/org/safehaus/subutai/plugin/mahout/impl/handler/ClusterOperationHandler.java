package org.safehaus.subutai.plugin.mahout.impl.handler;


import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.exception.ClusterException;
import org.safehaus.subutai.common.exception.ClusterSetupException;
import org.safehaus.subutai.common.protocol.AbstractOperationHandler;
import org.safehaus.subutai.common.protocol.ClusterSetupStrategy;
import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentDestroyException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.SetupType;
import org.safehaus.subutai.plugin.mahout.impl.MahoutImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Created by ebru on 25.12.2014.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<MahoutImpl, MahoutClusterConfig>
    implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private MahoutClusterConfig config;
    private HadoopClusterConfig hadoopConfig;
    private ExecutorService executor = Executors.newCachedThreadPool();


    public ClusterOperationHandler( final MahoutImpl manager, final MahoutClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( MahoutClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }

    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {
        try
        {
            Environment env = null;

            if ( config.getSetupType() == SetupType.WITH_HADOOP )
            {

                if ( hadoopConfig == null )
                {
                    trackerOperation.addLogFailed( "No Hadoop configuration specified" );
                    return;
                }

                trackerOperation.addLog( "Building environment..." );
                hadoopConfig.setTemplateName( MahoutClusterConfig.TEMPLATE_NAME );
                env = build();
                trackerOperation.addLog( "Environment preparation completed" );
            }
            else
            {
                env = manager.getEnvironmentManager().getEnvironmentByUUID( hadoopConfig.getEnvironmentId() );
                if ( env == null )
                {
                    throw new ClusterException( String.format( "Could not find environment of Hadoop cluster by id %s",
                            hadoopConfig.getEnvironmentId() ) );
                }
            }

            ClusterSetupStrategy s = manager.getClusterSetupStrategy( env, config, trackerOperation );
            if ( s == null )
            {
                throw new ClusterSetupException( "No setup strategy" );
            }
            s.setup();
            trackerOperation.addLogDone( "Done" );
        }
        catch ( ClusterSetupException ex )
        {
            trackerOperation.addLogFailed( "Failed to setup cluster: " + ex.getMessage() );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Could not start all nodes : %s", e.getMessage() ) );
        }

    }


    private Environment build()
    {
        Environment env = null;
        try
        {
            trackerOperation.addLog( "Building environment..." );
            EnvironmentBlueprint eb = manager.getHadoopManager().getDefaultEnvironmentBlueprint( hadoopConfig );
            env = manager.getEnvironmentManager().buildEnvironment( eb );
        }
        catch ( ClusterSetupException ex )
        {
            trackerOperation.addLogFailed( "Failed to prepare environment: " + ex.getMessage() );
        }
        catch ( EnvironmentBuildException ex )
        {
            trackerOperation.addLogFailed( "Failed to build environment: " + ex.getMessage() );
        }
        return env;
    }

    public void setHadoopConfig( HadoopClusterConfig hadoopConfig )
    {
        this.hadoopConfig = hadoopConfig;
    }



    @Override
    public void destroyCluster()
    {
        MahoutClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        try
        {
            trackerOperation.addLog( "Destroying environment..." );
            manager.getEnvironmentManager().destroyEnvironment( config.getEnvironmentId() );
            manager.getPluginDAO().deleteInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName() );
            trackerOperation.addLogDone( "Cluster destroyed" );
        }
        catch ( EnvironmentDestroyException e )
        {
            trackerOperation.addLogFailed( String.format( "Error running command, %s", e.getMessage() ) );
            LOG.error( e.getMessage(), e );
        }

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
                if ( config.getSetupType() == SetupType.OVER_HADOOP )
                {
                    uninstallCluster();
                }
                else if ( config.getSetupType() == SetupType.WITH_HADOOP )
                {
                    destroyCluster();
                }

                break;
        }

    }

    public void uninstallCluster()
    {
        TrackerOperation po = trackerOperation;
        po.addLog( "Uninstalling Mahout..." );

        for ( UUID uuid : config.getNodes() )
        {
            ContainerHost containerHost =
                    manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() )
                           .getContainerHostById( uuid );
            CommandResult result = null;
            try
            {
                result = containerHost.execute(  manager.getCommands().getUninstallCommand() );
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
        manager.getPluginDAO().deleteInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName() );
        po.addLogDone( "Cluster info deleted from DB\nDone" );
    }

}
