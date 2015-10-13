package io.subutai.plugin.mahout.cli;


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
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;


/**
 * sample command : mahout:install-cluster test \ {cluster name} test \ { hadoop cluster name } [ hadoop1, hadoop2 ] \ {
 * list of nodes }
 */
@Command( scope = "mahout", name = "install-cluster", description = "Command to install Mahout cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "hadoopClusterName", description = "The name of hadoop cluster.", required = true,
            multiValued = false )
    String hadoopClusterName = null;

    @Argument( index = 2, name = "nodes", description = "The hostname list of nodes", required = true,
            multiValued = false )
    String nodes[] = null;

    private Mahout mahoutManager;
    private Hadoop hadoopManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        MahoutClusterConfig config = new MahoutClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

        Set<String> nodeSet = new HashSet<>();
        Collections.addAll( nodeSet, nodes );
        config.setNodes( nodeSet );

        System.out.println( "Installing lucene cluster..." );
        UUID uuid = mahoutManager.installCluster( config );
        System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) );

        return null;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MahoutClusterConfig.PRODUCT_KEY, uuid );
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
            if ( System.currentTimeMillis() - start > ( 90 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    public void setMahoutManager( final Mahout mahoutManager )
    {
        this.mahoutManager = mahoutManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}