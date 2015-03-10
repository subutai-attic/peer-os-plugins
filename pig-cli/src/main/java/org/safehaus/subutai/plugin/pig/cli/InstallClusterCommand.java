package org.safehaus.subutai.plugin.pig.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.pig.api.Pig;
import org.safehaus.subutai.plugin.pig.api.PigConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : pig:install-cluster test \ {cluster name} test \ { hadoop cluster name } [ hadoop1, hadoop2 ] \ {
 * list of client machines }
 */
@Command( scope = "pig", name = "install-cluster", description = "Command to install Pig cluster" )
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

    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );
    private Pig pigManager;
    private Hadoop hadoopManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;


    @Override
    protected Object doExecute() throws Exception
    {
        try
        {
            Environment environment = environmentManager
                    .findEnvironment( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );
            try
            {
                PigConfig config = new PigConfig();
                config.setClusterName( clusterName );
                config.setHadoopClusterName( hadoopClusterName );
                Set<UUID> workerUUIS = new HashSet<>();
                for ( String hostname : nodes )
                {
                    workerUUIS.add( environment.getContainerHostByHostname( hostname ).getId() );
                }
                config.getNodes().addAll( workerUUIS );
                config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );

                System.out.println( "Installing pig cluster..." );
                UUID uuid = pigManager.installCluster( config );
                System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Could not find container host !!!" );
                e.printStackTrace();
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !!!" );
            e.printStackTrace();
        }

        return null;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( PigConfig.PRODUCT_KEY, uuid );
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


    public Pig getPigManager()
    {
        return pigManager;
    }


    public void setPigManager( final Pig pigManager )
    {
        this.pigManager = pigManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
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
