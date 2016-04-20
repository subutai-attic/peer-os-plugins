package io.subutai.plugin.hipi.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.impl.CommandFactory;
import io.subutai.plugin.hipi.impl.HipiImpl;


public class NodeOperationHandler extends AbstractOperationHandler<HipiImpl, HipiConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    private String hostName = null;
    private NodeOperationType operationType;


    public NodeOperationHandler( final HipiImpl manager, final String clusterName, String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostName = hostname;
        this.operationType = operationType;

        String desc = String.format( "Executing %s operation on node %s", operationType.name(), hostname );
        this.trackerOperation = manager.getTracker().createTrackerOperation( HipiConfig.PRODUCT_KEY, desc );
    }


    @Override
    public void run()
    {
        try
        {
            Environment environment;
            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException( "Environment not found: " + config.getEnvironmentId() );
            }


            ContainerHost node;
            try
            {
                node = environment.getContainerHostByHostname( hostName );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( "Node not found in environment: " + hostName );
            }

            if ( !node.isConnected() )
            {
                throw new ClusterException( "Node is not connected: " + hostName );
            }

            switch ( operationType )
            {
                case EXCLUDE:
                    removeNode( node );
                    break;
                case INCLUDE:
                    addNode( node );
                    break;
                default:
                    LOG.warn( "Unsupported operation type : " + operationType );
            }

            trackerOperation.addLogDone( String.format( "Operation %s succeeded", operationType.name() ) );
        }
        catch ( ClusterException ex )
        {
            LOG.error( "Exception " + ex.getMessage() + " thrown when handling operation type " + operationType.name(),
                    ex );
            trackerOperation
                    .addLogFailed( String.format( "Operation %s failed: %s", operationType.name(), ex.getMessage() ) );
        }
    }


    private void addNode( ContainerHost node ) throws ClusterException
    {


        setupHost( config, node );

        config.getNodes().add( node.getId() );

        trackerOperation.addLog( "Saving cluster info..." );
        manager.saveConfig( config );
        trackerOperation.addLogDone( "Saved cluster info" );
    }


    public void setupHost( HipiConfig config, ContainerHost node ) throws ClusterException
    {
        //check if node is in the cluster
        if ( config.getNodes().contains( node.getId() ) )
        {
            throw new ClusterException( "Node already belongs to cluster" + clusterName );
        }

        trackerOperation.addLog( "Checking prerequisites..." );

        try
        {
            String checkCommand = CommandFactory.build( NodeOperationType.CHECK_INSTALLATION );
            CommandResult checkCommandResult = node.execute( new RequestBuilder( checkCommand ) );
            String hadoopPack = Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME;
            if ( !checkCommandResult.hasCompleted() )
            {
                throw new ClusterException( "Failed to check installed packages" );
            }

            boolean skipInstall = false;
            if ( checkCommandResult.hasSucceeded() && checkCommandResult.getStdOut()
                                                                        .contains( HipiConfig.PRODUCT_PACKAGE ) )
            {
                skipInstall = true;
                trackerOperation.addLog( "Node already has " + HipiConfig.PRODUCT_KEY + " installed" );
            }
            if ( checkCommandResult.hasSucceeded() && !checkCommandResult.getStdOut().contains( hadoopPack ) )
            {
                throw new ClusterException( "Node has no Hadoop installation" );
            }
            if ( !skipInstall )
            {
                trackerOperation.addLog( "Installing " + HipiConfig.PRODUCT_KEY );
                String installCommand = CommandFactory.build( NodeOperationType.INSTALL );
                CommandResult installCommandResult = node.execute( new RequestBuilder( installCommand ) );
                checkCommandResult = node.execute( new RequestBuilder( checkCommand ) );

                if ( installCommandResult.hasSucceeded() && checkCommandResult.getStdOut().
                        contains( HipiConfig.PRODUCT_PACKAGE ) )
                {
                    trackerOperation.addLog( "Installation succeeded" );
                }
                else
                {
                    throw new ClusterException( "Installation failed: " + installCommandResult.getStdErr() );
                }
            }
        }
        catch ( CommandException ex )
        {
            throw new ClusterException( ex );
        }
    }


    private void removeNode( ContainerHost node )
    {

        if ( config.getNodes().size() == 1 )
        {
            trackerOperation.addLogFailed(
                    "This is the last slave node in the cluster. Please, destroy cluster instead\nOperation aborted" );
            return;
        }

        //check if node is in the cluster
        if ( !config.getNodes().contains( node.getId() ) )
        {
            trackerOperation.addLogFailed(
                    String.format( "Node %s does not belong to this cluster\nOperation aborted", node.getHostname() ) );
            return;
        }

        boolean ok = false;
        try
        {

            trackerOperation.addLog( "Uninstalling " + HipiConfig.PRODUCT_KEY );

            String uninstallCommand = CommandFactory.build( NodeOperationType.UNINSTALL );
            CommandResult uninstallCommandResult = node.execute( new RequestBuilder( uninstallCommand ) );

            if ( uninstallCommandResult.hasSucceeded() )
            {
                trackerOperation.addLog( HipiConfig.PRODUCT_KEY + " removed from " + node.getHostname() );
                ok = true;
            }
            else
            {
                trackerOperation.addLog( "Uninstallation failed: " + uninstallCommandResult.getStdErr() );
                ok = false;
            }
        }
        catch ( CommandException ex )
        {
            trackerOperation.addLogFailed( "Failed to remove with message " + ex.getMessage() );
        }

        if ( ok )
        {
            config.getNodes().remove( node.getId() );
            trackerOperation.addLog( "Updating db..." );

            try
            {
                manager.saveConfig( config );
                trackerOperation.addLogDone( "Cluster info updated in DB\nDone" );
            }
            catch ( ClusterException e )
            {
                trackerOperation.addLogFailed( String.format( "Failed to save cluster info: %s", e ) );
            }
        }
        else
        {
            trackerOperation.addLogFailed( "Failed to destroy node" );
        }
    }
}
