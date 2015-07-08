package io.subutai.plugin.solr.cli;


import java.io.IOException;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.solr.api.Solr;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : solr:install-cluster test \ {cluster name} uuid \ { environmentId } uuid \ { node }
 */
@Command( scope = "solr", name = "install-cluster", description = "Command to install Solr cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "environmentId", description = "The environment id to be used to setup Solr cluster.",
            required = true,
            multiValued = false )
    String environmentId = null;

    @Argument( index = 2, name = "node", description = "The id of container", required = true,
            multiValued = false )
    String node = null;

    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );
    private Solr solrManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {

        try
        {
            Environment environment = environmentManager.findEnvironment( UUID.fromString( environmentId ) );
            SolrClusterConfig config = new SolrClusterConfig();
            config.setClusterName( clusterName );
            config.setEnvironmentId( UUID.fromString( environmentId ) );
            if ( checkGivenUUID( environment, UUID.fromString( node ) ) )
            {
                config.getNodes().add( UUID.fromString( node ) );
            }
            else
            {
                System.out.println( "Could not find container host with given uuid : " + node );
                return null;
            }


            System.out.println( "Installing solr cluster..." );
            UUID uuid = solrManager.installCluster( config );
            System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment", e );
            e.printStackTrace();
        }

        return null;
    }


    private boolean checkGivenUUID( Environment environment, UUID uuid )
    {
        for ( ContainerHost host : environment.getContainerHosts() )
        {
            if ( host.getId().equals( uuid ) )
            {
                return true;
            }
        }
        return false;
    }


    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( SolrClusterConfig.PRODUCT_KEY, uuid );
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


    public Solr getSolrManager()
    {
        return solrManager;
    }


    public void setSolrManager( final Solr solrManager )
    {
        this.solrManager = solrManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
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
