package io.subutai.plugin.oozie.cli;


import java.util.UUID;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : oozie:check-cluster test \ {cluster name}
 */
@Command( scope = "oozie", name = "check-cluster", description = "Command to check Oozie cluster" )
public class CheckClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Oozie oozieManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( CheckClusterCommand.class.getName() );


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Checking cluster nodes ... " );
        UUID checkUUID = oozieManager.checkNode( clusterName, server );
        System.out.println( server + " is " + waitUntilOperationFinish( tracker, checkUUID ) );

        return null;
    }


    protected static NodeState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( OozieClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Oozie Server is not running" ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog().contains( "Oozie Server is running" ) )
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


    public void setOozieManager( Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
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
