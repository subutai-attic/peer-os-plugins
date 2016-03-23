package io.subutai.plugin.sqoop.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.sqoop.api.SqoopConfig;
import io.subutai.plugin.sqoop.impl.CommandFactory;
import io.subutai.plugin.sqoop.impl.SqoopImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<SqoopImpl, SqoopConfig>
        implements ClusterOperationHandlerInterface
{

    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private HadoopClusterConfig hadoopConfig;


    public ClusterOperationHandler( SqoopImpl manager, SqoopConfig config, ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;

        String desc = String.format( "Executing %s operation on cluster %s", operationType.name(), clusterName );
        this.trackerOperation = manager.getTracker().createTrackerOperation( SqoopConfig.PRODUCT_KEY, desc );
    }


    public void setHadoopConfig( HadoopClusterConfig hadoopConfig )
    {
        this.hadoopConfig = hadoopConfig;
    }


    @Override
    public void run()
    {
        runOperationOnContainers( operationType );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType operationType )
    {
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
                LOG.warn( "Command not applicable: " + operationType );
                break;
        }
    }


    @Override
    public void setupCluster()
    {
        Environment env = null;
        try
        {
            HadoopClusterConfig hc = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
            if ( hc == null )
            {
                throw new ClusterException( "Hadoop cluster not found: " + config.getHadoopClusterName() );
            }

            try
            {
                env = manager.getEnvironmentManager().loadEnvironment( hc.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }

            if ( env == null )
            {
                throw new ClusterException( String.format( "Could not find environment of Hadoop cluster by id %s",
                        hadoopConfig.getEnvironmentId() ) );
            }

            ClusterSetupStrategy s = manager.getClusterSetupStrategy( env, config, trackerOperation );
            if ( s == null )
            {
                throw new ClusterException( "No setup strategy" );
            }
            try
            {
                trackerOperation.addLog( "Installing Sqoop nodes..." );
                s.setup();
                trackerOperation.addLogDone( "Installing successfully completed" );
            }
            catch ( ClusterSetupException ex )
            {
                throw new ClusterException( "Failed to setup cluster: " + ex.getMessage() );
            }
        }
        catch ( ClusterException e )
        {
            String msg = "Installation failed\n" + e.getMessage();
            LOG.error( msg, e );
            trackerOperation.addLogFailed( msg );
        }
    }


    @Override
    public void destroyCluster()
    {
        try
        {
            if ( manager.getCluster( clusterName ) == null )
            {
                throw new ClusterException( "Sqoop installation not found: " + clusterName );
            }
            Environment env = null;
            try
            {
                env = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }

            if ( env == null )
            {
                throw new ClusterException( "Environment not found: " + config.getEnvironmentId() );
            }

            Set<EnvironmentContainerHost> nodes = null;
            try
            {
                nodes = env.getContainerHostsByIds( config.getNodes() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }

            if ( CollectionUtil.isCollectionEmpty( nodes ) || nodes.size() < config.getNodes().size() )
            {
                throw new ClusterException( "Fewer nodes found in the environment than expected" );
            }


            for ( EnvironmentContainerHost node : nodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            trackerOperation.addLog( "Uninstalling Sqoop..." );

            RequestBuilder rb = new RequestBuilder( CommandFactory.build( NodeOperationType.UNINSTALL, null ) );
            for ( EnvironmentContainerHost node : nodes )
            {
                try
                {
                    CommandResult result = node.execute( rb );
                    if ( result.hasSucceeded() )
                    {
                        trackerOperation.addLog( "Sqoop uninstalled from " + node.getHostname() );
                    }
                    else
                    {
                        throw new ClusterException(
                                String.format( "Could not uninstall Sqoop from node %s : %s", node.getHostname(),
                                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
                    }
                }
                catch ( CommandException e )
                {
                    throw new ClusterException(
                            String.format( "Failed to uninstall Sqoop on node %s", node.getHostname() ), e );
                }
            }

            boolean deleted = manager.getPluginDao().deleteInfo( SqoopConfig.PRODUCT_KEY, config.getClusterName() );
            if ( !deleted )
            {
                throw new ClusterException( "Failed to delete installation info" );
            }
            trackerOperation.addLogDone( "Sqoop installation successfully removed" );
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in destroyCluster", e );
            trackerOperation.addLogFailed( String.format( "Failed to uninstall cluster: %s", e.getMessage() ) );
        }
    }
}

