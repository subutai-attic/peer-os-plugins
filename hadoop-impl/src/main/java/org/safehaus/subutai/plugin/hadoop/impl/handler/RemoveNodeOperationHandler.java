package org.safehaus.subutai.plugin.hadoop.impl.handler;


import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentManagerException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;


public class RemoveNodeOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{

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
     * Steps:
     *   1) Exclude node from hadoop cluster
     *   2) Destroy node
     */
    public void removeNode(){
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        Environment environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
        ContainerHost host = environment.getContainerHostByHostname( lxcHostName );

        trackerOperation.addLog( "Excluding " + lxcHostName + " from cluster" );
        config.getDataNodes().remove( host.getId() );
        config.getTaskTrackers().remove( host.getId() );
        manager.excludeNode( config, lxcHostName );
        config.getBlockedAgents().remove( host.getId() );
        removeNodeFromConfigurationFiles( host );
        manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        try
        {
            environmentManager.removeContainer( environment.getId(), environment.getContainerHostByHostname( lxcHostName ).getId() );
        }
        catch ( EnvironmentManagerException e )
        {
            trackerOperation.addLogFailed( "Could not destroy container " + lxcHostName +  ". " + e.getMessage() );
            e.printStackTrace();
        }
        trackerOperation.addLogDone( "Container " + lxcHostName + " is destroyed!" );
    }


    protected void removeNodeFromConfigurationFiles( ContainerHost host )
    {
        HadoopClusterConfig config = manager.getCluster( clusterName );
        try
        {
            ContainerHost namenode = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() )
                                            .getContainerHostById( config.getNameNode() );
            ContainerHost jobtracker = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() )
                                              .getContainerHostById( config.getJobTracker() );

            namenode.execute( new RequestBuilder( Commands.getRemoveDataNodeCommand( host.getHostname() ) ) );
            jobtracker.execute( new RequestBuilder( Commands.getRemoveTaskTrackerCommand( host.getHostname() ) ) );
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Error running command, %s", e.getMessage() ) );
        }
    }
}
