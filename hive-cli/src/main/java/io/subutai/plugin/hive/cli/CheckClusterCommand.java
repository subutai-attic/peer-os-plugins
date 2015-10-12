package io.subutai.plugin.hive.cli;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.hive.api.Hive;
import io.subutai.plugin.hive.api.HiveConfig;


/**
 * sample command : hive:check-cluster test \ {cluster name}
 */
@Command( scope = "hive", name = "check-cluster", description = "Command to check Hive cluster" )
public class CheckClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    @Argument( index = 1, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;
    private Hive hiveManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( CheckClusterCommand.class.getName() );


    @Override
    protected Object doExecute() throws Exception
    {
        System.out.println( "Checking cluster nodes ... " );
        UUID checkUUID = hiveManager.statusCheck( clusterName, server );
        System.out.println( server + " is " + waitUntilOperationFinish( tracker, checkUUID ) );
        return null;
    }


    protected static NodeState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HiveConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    if ( po.getLog().contains( "Hive Thrift Server is not running" ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog().contains( "Hive Thrift Server is running" ) )
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


    public Hive getHiveManager()
    {
        return hiveManager;
    }


    public void setHiveManager( Hive hiveManager )
    {
        this.hiveManager = hiveManager;
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
