package org.safehaus.subutai.plugin.oozie.cli;


import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.NodeState;
import org.safehaus.subutai.plugin.oozie.api.Oozie;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *    oozie:check-cluster test \ {cluster name}
 */
@Command(scope = "oozie", name = "check-cluster", description = "Command to check Oozie cluster")
public class CheckClusterCommand extends OsgiCommandSupport
{
    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Oozie oozieManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( CheckClusterCommand.class.getName() );

    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Checking cluster nodes ... " );
        OozieClusterConfig config = oozieManager.getCluster( clusterName );
        for ( UUID uuid : config.getAllNodes() ){
            try
            {
                Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
                try
                {
                    String hostname = environment.getContainerHostById( uuid ).getHostname();
                    UUID checkUUID = oozieManager.checkNode( clusterName, hostname );
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
            TrackerOperationView po = tracker.getTrackerOperation( OozieClusterConfig.PRODUCT_KEY, uuid );
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


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( Oozie oozieManager)
    {
        this.oozieManager= oozieManager;
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
