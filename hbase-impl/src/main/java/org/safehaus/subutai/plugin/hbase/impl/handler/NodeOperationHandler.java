package org.safehaus.subutai.plugin.hbase.impl.handler;


import java.util.Iterator;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.safehaus.subutai.plugin.hbase.impl.Commands;
import org.safehaus.subutai.plugin.hbase.impl.HBaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class NodeOperationHandler extends AbstractOperationHandler<HBaseImpl, HBaseConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );
    private String hostname;
    private NodeOperationType operationType;
    private HBaseConfig config;
    private ContainerHost node;

    public NodeOperationHandler( final HBaseImpl manager, final HBaseConfig config, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, config );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Invalid hostname" );
        Preconditions.checkNotNull( operationType );
        this.hostname = hostname;
        this.operationType = operationType;
        this.config = config;
        this.node = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() ).getContainerHostByHostname( hostname );
        trackerOperation = manager.getTracker().createTrackerOperation( HBaseConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
        Iterator iterator = environment.getContainerHosts().iterator();
        ContainerHost host = null;
        while ( iterator.hasNext() )
        {
            host = ( ContainerHost ) iterator.next();
            if ( host.getHostname().equals( hostname ) )
            {
                break;
            }
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }
        runCommand( host, operationType );
    }


    protected void runCommand( ContainerHost host, NodeOperationType operationType )
    {
        CommandResult result = null;
        switch ( operationType )
        {
            case START:
                break;
            case STOP:
                break;
            case STATUS:
                checkServiceStatus( host );
                break;
        }
    }


    private void checkServiceStatus( ContainerHost host )
    {
        try
        {
            CommandResult result = node.execute( Commands.getStatusCommand() );
            if ( result.hasSucceeded() )
            {
                trackerOperation.addLog( result.getStdOut() );
            }
            else
            {
                trackerOperation.addLogFailed( result.getStdErr() );
            }
            trackerOperation.addLogDone( "Check service status command executed" );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( e.getMessage() );
            LOG.error( e.getMessage(), e );
        }
    }


    private void removeNode() throws ClusterException
    {

        //check if node is in the cluster
        if ( !config.getAllNodes().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to this cluster", hostname ) );
        }

        if ( config.getAllNodes().size() == 1 )
        {
            throw new ClusterException( "This is the last node in the cluster. Please, destroy cluster instead" );
        }

        trackerOperation.addLog( "Uninstalling HBase..." );

        executeCommand( node, manager.getCommands().getUninstallCommand(), true );

        config.getAllNodes().remove( node.getId() );

        trackerOperation.addLog( "Updating db..." );

        if ( !manager.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not update cluster info" );
        }
    }


    private void addNode() throws ClusterException
    {
        //check if node is in the cluster
        if ( config.getAllNodes().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s already belongs to this cluster", hostname ) );
        }

        CommandResult result = executeCommand( node, manager.getCommands().getCheckInstalledCommand() );

        if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
        {
            throw new ClusterException( "Node already has HBase installed" );
        }

        config.getAllNodes().add( node.getId() );


        trackerOperation.addLog( "Installing HBase..." );

        executeCommand( node, manager.getCommands().getInstallCommand() );

        trackerOperation.addLog( "Setting Master IP..." );

        //        executeCommand( node, manager.getCommands().getSetMasterIPCommand( sparkMaster ) );

        trackerOperation.addLog( "Updating db..." );

        if ( !manager.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, config.getClusterName(), config ) )
        {
            throw new ClusterException( "Could not update cluster info" );
        }
    }


    public CommandResult executeCommand( ContainerHost host, RequestBuilder command ) throws ClusterException
    {

        return executeCommand( host, command, false );
    }


    public CommandResult executeCommand( ContainerHost host, RequestBuilder command, boolean skipError )
            throws ClusterException
    {

        CommandResult result = null;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            if ( skipError )
            {
                trackerOperation
                        .addLog( String.format( "Error on container %s: %s", host.getHostname(), e.getMessage() ) );
            }
            else
            {
                throw new ClusterException( e );
            }
        }
        if ( skipError )
        {
            if ( result != null && !result.hasSucceeded() )
            {
                trackerOperation.addLog( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        else
        {
            if ( !result.hasSucceeded() )
            {
                throw new ClusterException( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        return result;
    }
}
