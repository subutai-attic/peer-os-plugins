package io.subutai.plugin.flume.cli;


import java.util.UUID;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.flume.api.Flume;
import io.subutai.plugin.flume.api.FlumeConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "flume", name = "start-cluster", description = "Starts cluster" )
public class StartNodeCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "hostname", description = "The hostname of flume node.", required = true,
            multiValued = false )
    String hostname = null;

    private Tracker tracker;
    private Flume flumeManager;


    @Override
    protected Object doExecute() throws Exception
    {
        UUID uuid = flumeManager.startNode( clusterName, hostname );
        System.out.println( "Start cluster operation is " + waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( FlumeConfig.PRODUCT_KEY, uuid );
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


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Flume getFlumeManager()
    {
        return flumeManager;
    }


    public void setFlumeManager( final Flume flumeManager )
    {
        this.flumeManager = flumeManager;
    }
}
