package org.safehaus.subutai.plugin.lucene.impl.handler;


import java.util.Iterator;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;
import org.safehaus.subutai.plugin.lucene.impl.Commands;
import org.safehaus.subutai.plugin.lucene.impl.LuceneImpl;


public class NodeOperationHandler extends AbstractOperationHandler<LuceneImpl, LuceneConfig>
{

    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;


    public NodeOperationHandler( final LuceneImpl manager, String clusterName, final String hostname,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostname;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( LuceneConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        LuceneConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Environment not found: %s", e ) );
            return;
        }
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


        switch ( operationType )
        {
            case INSTALL:
                installProductOnNode( host );
                break;
            case UNINSTALL:
                uninstallProductOnNode( host );
                break;
        }
    }


    private CommandResult installProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( new RequestBuilder( Commands.installCommand ).withTimeout( 600 ) );
            if ( result.hasSucceeded() )
            {
                config.getNodes().add( host.getId() );

                manager.saveConfig( config );

                trackerOperation.addLogDone(
                        LuceneConfig.PRODUCT_KEY + " is installed on node " + host.getHostname() + " successfully." );
            }
            else
            {
                trackerOperation
                        .addLogFailed( "Could not install " + LuceneConfig.PRODUCT_KEY + " to node " + hostname );
            }
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation.addLogFailed( "Could not install " + LuceneConfig.PRODUCT_KEY + " to node " + hostname );
        }
        return result;
    }


    private CommandResult uninstallProductOnNode( ContainerHost host )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( new RequestBuilder( Commands.uninstallCommand ).withTimeout( 1000 ) );
            if ( result.hasSucceeded() )
            {
                config.getNodes().remove( host.getId() );

                manager.saveConfig( config );

                trackerOperation.addLogDone(
                        LuceneConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                + " successfully." );
            }
            else
            {
                trackerOperation
                        .addLogFailed( "Could not uninstall " + LuceneConfig.PRODUCT_KEY + " from node " + hostname );
            }
        }
        catch ( ClusterException | CommandException e )
        {
            trackerOperation
                    .addLogFailed( "Could not uninstall " + LuceneConfig.PRODUCT_KEY + " from node " + hostname );
        }
        return result;
    }
}
