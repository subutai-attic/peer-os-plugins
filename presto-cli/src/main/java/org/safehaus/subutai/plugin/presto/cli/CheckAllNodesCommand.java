package org.safehaus.subutai.plugin.presto.cli;


import java.io.IOException;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.presto.api.Presto;
import org.safehaus.subutai.plugin.presto.api.PrestoClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      presto:check-cluster test \ {cluster name}
 */
@Command(scope = "presto", name = "check-cluster", description = "Command to check Presto cluster")
public class CheckAllNodesCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Presto prestoManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );

    protected Object doExecute() throws IOException
    {
        System.out.println( "Checking cluster nodes ... " );
        PrestoClusterConfig config = prestoManager.getCluster( clusterName );
        for ( UUID uuid : config.getAllNodes() ){
            try
            {
                Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                try
                {
                    String hostname = environment.getContainerHostById( uuid ).getHostname();
                    UUID checkUUID = prestoManager.checkNode( clusterName, hostname );
                    System.out.println( hostname + " is " + waitUntilOperationFinish( tracker, checkUUID ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host." );
                    e.printStackTrace();
                }
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Could not find environment." );
                e.printStackTrace();
            }
        }
        return null;
    }


    protected static NodeState waitUntilOperationFinish( Tracker tracker, UUID uuid ){
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( PrestoClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().toLowerCase().contains( NodeState.STOPPED.name().toLowerCase() ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog().toLowerCase().contains( NodeState.RUNNING.name().toLowerCase() ) )
                    {
                        state = NodeState.RUNNING;
                    }
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }
        return state;
    }


    public Presto getPrestoManager()
    {
        return prestoManager;
    }


    public void setPrestoManager( Presto prestoManager )
    {
        this.prestoManager = prestoManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
