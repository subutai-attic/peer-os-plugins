package org.safehaus.subutai.plugin.sqoop.impl.handler;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.sqoop.api.SqoopConfig;
import org.safehaus.subutai.plugin.sqoop.impl.CommandFactory;
import org.safehaus.subutai.plugin.sqoop.impl.SqoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
                env = manager.getEnvironmentManager().findEnvironment( hc.getEnvironmentId() );
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
                env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                e.printStackTrace();
            }

            if ( env == null )
            {
                throw new ClusterException( "Environment not found: " + config.getEnvironmentId() );
            }

            Set<ContainerHost> nodes = null;
            try
            {
                nodes = env.getContainerHostsByIds( config.getNodes() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
            for ( ContainerHost node : nodes )
            {
                if ( !node.isConnected() )
                {
                    throw new ClusterException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            trackerOperation.addLog( "Uninstalling Sqoop..." );

            RequestBuilder rb = new RequestBuilder( CommandFactory.build( NodeOperationType.UNINSTALL, null ) );
            for ( ContainerHost node : nodes )
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

