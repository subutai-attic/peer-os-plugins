package io.subutai.plugin.hadoop.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentModificationException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.Commands;
import io.subutai.plugin.hadoop.impl.HadoopImpl;


public class RemoveNodeOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( RemoveNodeOperationHandler.class );
    private String lxcHostName;


    public RemoveNodeOperationHandler( HadoopImpl manager, String clusterName, String lxcHostName )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.lxcHostName = lxcHostName;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Destroying %s node and updating cluster information of %s", lxcHostName,
                        clusterName ) );
    }


    @Override
    public void run()
    {
        removeNode();
    }


    /**
     * Steps: 1) Exclude node from hadoop cluster 2) Destroy node
     */
    public void removeNode()
    {
        try
        {
            EnvironmentManager environmentManager = manager.getEnvironmentManager();
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            EnvironmentContainerHost host = environment.getContainerHostByHostname( lxcHostName );

            trackerOperation.addLog( "Excluding " + lxcHostName + " from cluster" );
            config.getDataNodes().remove( host.getId() );
            config.getTaskTrackers().remove( host.getId() );
            manager.excludeNode( config, lxcHostName );
            config.getBlockedAgents().remove( host.getId() );
            removeNodeFromConfigurationFiles( host );
            manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
            environmentManager.destroyContainer( environment.getId(), host.getId(), true, true );
            trackerOperation.addLogDone( "Container " + lxcHostName + " is destroyed!" );
        }
        catch ( ContainerHostNotFoundException | EnvironmentNotFoundException | EnvironmentModificationException e )
        {
            trackerOperation.addLogFailed( "Could not destroy container " + lxcHostName );
            LOGGER.error( "Could not destroy container " + lxcHostName, e );
        }
    }


    protected void removeNodeFromConfigurationFiles( EnvironmentContainerHost host )
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        try
        {
            EnvironmentContainerHost namenode = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                            .getContainerHostById( config.getNameNode() );
            EnvironmentContainerHost jobtracker = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                              .getContainerHostById( config.getJobTracker() );

            namenode.execute( new RequestBuilder( Commands.getRemoveDataNodeCommand( host.getHostname() ) ) );
            jobtracker.execute( new RequestBuilder( Commands.getRemoveTaskTrackerCommand( host.getHostname() ) ) );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Error running command" ) );
            LOGGER.error( "Error running command", e );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }
}
