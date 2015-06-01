package org.safehaus.subutai.plugin.mysql.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLC;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * Created by tkila on 5/27/15.
 */
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


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public MySQLC getManager()
    {
        return manager;
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
        config.setEnvironmentId( UUID.fromString( environmentId ) );

        Set<UUID> dataNodes = new HashSet<>();
        for ( String node : datanodes )
        {
            dataNodes.add( UUID.fromString( node ) );
        }

        Set<UUID> managerNodes = new HashSet<>();
        for ( String node : managernodes )
        {

            managerNodes.add( UUID.fromString( node ) );
        }
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
