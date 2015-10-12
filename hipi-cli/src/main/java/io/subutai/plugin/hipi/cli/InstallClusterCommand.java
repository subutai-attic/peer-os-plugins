package io.subutai.plugin.hipi.cli;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hipi.api.Hipi;
import io.subutai.plugin.hipi.api.HipiConfig;


@Command( scope = "hipi", name = "install-cluster", description = "Command to install Hipi cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "flume cluster "
            + "name" )
    String clusterName;

    @Argument( index = 1, name = "hadoopClusterName", description = "The hadoop cluster name", required = true,
            multiValued = false )
    String hadoopClusterName;

    @Argument( index = 2, name = "nodes", description = "The list of nodes that Hipi will be installed", required =
            true,
            multiValued = false )
    String nodes[];


    private Tracker tracker;
    private Hipi hipiManager;
    private Hadoop hadoopManager;


    protected Object doExecute()
    {
        HipiConfig config = new HipiConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

        Set<String> nodesSet = new HashSet<>();
        Collections.addAll( nodesSet, nodes );
        config.setNodes( nodesSet );
        UUID uuid = hipiManager.installCluster( config );
        System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HipiConfig.PRODUCT_KEY, uuid );
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


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hipi getHipiManager()
    {
        return hipiManager;
    }


    public void setHipiManager( final Hipi hipiManager )
    {
        this.hipiManager = hipiManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
