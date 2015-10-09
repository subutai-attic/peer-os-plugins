package io.subutai.plugin.hadoop.cli;


import java.io.IOException;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


/**
 * sample command : hadoop:start-cluster test \ {cluster name}
 */
@Command( scope = "hadoop", name = "start-cluster", description = "Command to start Hadoop cluster" )
public class StartClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Hadoop hadoopManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws IOException
    {

        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );
        System.out.println( "Staring namenode ..." );
        UUID uuid = hadoopManager.startNameNode( config );

        System.out.println( "Staring jobtracker ..." );
        UUID uuid2 = hadoopManager.startJobTracker( config );

        System.out.println( "Start namenode operation is " + waitUntilOperationFinish( tracker, uuid ) );
        System.out.println( "Start jobtracker operation is " + waitUntilOperationFinish( tracker, uuid2 ) );
        return null;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
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
            if ( System.currentTimeMillis() - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }
        return state;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }
}
