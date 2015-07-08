package io.subutai.plugin.mysql.cli;


import java.util.UUID;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Created by tkila on 5/18/15.
 */

@Command( scope = "mysql", name = "start-cluster", description = "Command to start MySQL Cluster" )
public class StartClusterCommand extends OsgiCommandSupport
{
    private static final Logger LOG = LoggerFactory.getLogger( MySQLClusterConfig.class );


    @Argument( index = 0, name = "clusterName", description = "The name of the cluster ", required = true,
            multiValued = false )
    String clusterName;

    private MySQLC mySqlManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = getMySqlManager().startCluster( clusterName );

        System.out.print("Starting cluster " + waitUntilOperationFinish(tracker,uuid));
        return null;

    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public MySQLC getMySqlManager()
    {
        return mySqlManager;
    }


    public void setMySqlManager( final MySQLC mySqlManager )
    {
        this.mySqlManager = mySqlManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }

    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MySQLClusterConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    state = po.getState();
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
            if ( System.currentTimeMillis() - start > ( 180 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }
}
