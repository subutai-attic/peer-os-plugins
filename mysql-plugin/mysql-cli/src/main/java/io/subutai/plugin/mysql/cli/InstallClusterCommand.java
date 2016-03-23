package io.subutai.plugin.mysql.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mysql.api.MySQLC;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;


@Command( scope = "mysql", name = "install-cluster", description = "Command to install cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{
    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "cluster name" )
    String clusterName;

    @Argument( index = 1, name = "environmentId", description = "The environmentId to set up cluster", required =
            true, multiValued = false )
    String environmentId;
    @Argument( index = 2, name = "datanodes", description = "The list of data nodes", required = true, multiValued =
            false )
    String datanodes[];
    @Argument( index = 3, name = "managernodes", description = "The list of manager nodes", required = true,
            multiValued = false )
    String managernodes[];

    private Tracker tracker;
    private MySQLC manager;


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setManager( final MySQLC manager )
    {
        this.manager = manager;
    }


    @Override
    protected Object doExecute() throws Exception
    {
        MySQLClusterConfig config = new MySQLClusterConfig();
        config.setClusterName( clusterName );
        config.setEnvironmentId( environmentId );

        Set<String> dataNodes = new HashSet<>();
        for ( String node : datanodes )
        {
            dataNodes.add( node );
        }
        config.setDataNodes( dataNodes );

        Set<String> managerNodes = new HashSet<>();
        for ( String node : managernodes )
        {

            managerNodes.add( node );
        }
        config.setManagerNodes( managerNodes );
        UUID uuid = manager.installCluster( config );
        System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) );
        return null;
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
