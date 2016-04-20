package io.subutai.plugin.storm.cli;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.storm.api.Storm;
import io.subutai.plugin.storm.api.StormClusterConfiguration;


@Command( scope = "storm", name = "check-cluster", description = "Checks cluster nodes' statutes" )
public class CheckClusterCommand extends OsgiCommandSupport
{
    private static final Logger LOG = LoggerFactory.getLogger( CheckClusterCommand.class.getName() );
    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Tracker tracker;
    private EnvironmentManager environmentManager;
    private Storm stormManager;


    @Override
    protected Object doExecute() throws Exception
    {
        StormClusterConfiguration config = stormManager.getCluster( clusterName );
        Environment environment;
        try
        {
            environment = environmentManager.loadEnvironment( config.getEnvironmentId() );

            for ( String id : config.getAllNodes() )
            {

                String hostname = environment.getContainerHostById( id ).getHostname();
                UUID checkUUID = stormManager.checkNode( clusterName, hostname );
                System.out.println( "Status on " + hostname + ": \n" + waitUntilOperationFinish( tracker, checkUUID ) );
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !", e );
        }
        return null;
    }


    protected static String waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( StormClusterConfiguration.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    return po.getLog();
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
        return null;
    }


    public void setStormManager( final Storm stormManager )
    {
        this.stormManager = stormManager;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
