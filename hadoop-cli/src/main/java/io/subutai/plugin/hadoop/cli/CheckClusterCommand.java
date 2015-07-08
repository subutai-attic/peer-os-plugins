package io.subutai.plugin.hadoop.cli;


import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.NodeState;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

/**
 * sample command :
 *      hadoop:check-cluster test \ {cluster name}
 */
@Command(scope = "hadoop", name = "check-cluster", description = "Command to check Hadoop cluster")
public class CheckClusterCommand extends OsgiCommandSupport
{

    @Argument(index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false)
    String clusterName = null;
    private Hadoop hadoopManager;
    private Tracker tracker;
    private EnvironmentManager environmentManager;


    @Override
    protected Object doExecute() throws IOException
    {

        HadoopClusterConfig config = hadoopManager.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );

            UUID nameNodeUUID = hadoopManager.statusNameNode( config );
            UUID jtUUID = hadoopManager.statusJobTracker( config );
            UUID snnUUID = hadoopManager.statusSecondaryNameNode( config );
            HashMap<UUID, UUID> dn = new HashMap<>();
            for ( UUID uuid : config.getDataNodes() ){
                try
                {
                    dn.put( uuid, hadoopManager
                            .statusDataNode( config, environment.getContainerHostById( uuid ).getHostname() ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }

            HashMap<UUID, UUID> tt = new HashMap<>();
            for ( UUID uuid : config.getTaskTrackers() ){
                try
                {
                    tt.put( uuid, hadoopManager
                            .statusTaskTracker( config, environment.getContainerHostById( uuid ).getHostname() ) );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }

            System.out.println( "Namenode : " + waitUntilOperationFinish( tracker, nameNodeUUID ).name() );
            System.out.println( "JobTracker : " + waitUntilOperationFinish( tracker, jtUUID ).name() );
            System.out.println( "SecondaryNameNode : " + waitUntilOperationFinish( tracker, snnUUID ).name() );
            StringBuilder sb = new StringBuilder();
            sb.append( "DataNodes : " );
            for ( UUID uuid : config.getDataNodes() ){
                try
                {
                    sb.append( environment.getContainerHostById(  uuid ).getHostname() + " " + waitUntilOperationFinish( tracker, dn.get( uuid ) ) + " ");
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
            System.out.println( sb.toString() );


            sb = new StringBuilder();
            sb.append( "TaskTrackers : " );
            for ( UUID uuid : config.getTaskTrackers() ){
                try
                {
                    sb.append( environment.getContainerHostById(  uuid ).getHostname() + " " + waitUntilOperationFinish( tracker, tt.get( uuid ) ) + " ");
                }
                catch ( ContainerHostNotFoundException e )
                {
                    e.printStackTrace();
                }
            }

            System.out.println( sb.toString()  );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return null;
    }


    protected static NodeState waitUntilOperationFinish( Tracker tracker, UUID uuid ){
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HadoopClusterConfig.PRODUCT_NAME, uuid );
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


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( Tracker tracker )
    {
        this.tracker = tracker;
    }

}
