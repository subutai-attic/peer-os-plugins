package io.subutai.plugin.zookeeper.cli;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.Zookeeper;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : zookeeper:install-cluster test \ {cluster name} uuid \ { environmentId } [ zookeeper1, zookeeper2 ]
 * \ { list of containers }
 */
@Command( scope = "zookeeper", name = "install-cluster", description = "Command to install Zookeeper cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "environmentId", description = "The environment id to be used to setup Zookeeper cluster.", required = true,
            multiValued = false )
    String environmentId = null;

    @Argument( index = 2, name = "nodes", description = "The list of node", required = true,
            multiValued = false )
    private String[] nodes = null;

    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );
    private Zookeeper zookeeperManager;
    private EnvironmentManager environmentManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        try
        {
            Environment environment = environmentManager.findEnvironment( UUID.fromString( environmentId ) );
            ZookeeperClusterConfig config = new ZookeeperClusterConfig();
            config.setClusterName( clusterName );
            config.setEnvironmentId( UUID.fromString( environmentId ) );

            Set<UUID> configNodes = new HashSet<>();
            for ( String node : nodes )
            {
                if( checkGivenUUID( environment, UUID.fromString( node ) ))
                {
                    configNodes.add( UUID.fromString( node ) );
                }
                else {
                    System.out.println( "Could not find container host with given uuid : " + node );
                    return null;
                }
            }
            config.setNodes( configNodes );
            config.setSetupType( SetupType.OVER_ENVIRONMENT );
            System.out.println( "Installing zookeeper cluster..." );
            UUID uuid = getZookeeperManager().installCluster( config );
            System.out.println( "Install operation is " + waitUntilOperationFinish( tracker, uuid ) );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !!!" );
            e.printStackTrace();
        }

        return null;
    }

    private boolean checkGivenUUID( Environment environment, UUID uuid ){
        for ( ContainerHost host : environment.getContainerHosts() ){
            if ( host.getId().equals( uuid ) ){
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
            TrackerOperationView po = tracker.getTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY, uuid );
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


    public Zookeeper getZookeeperManager()
    {
        return zookeeperManager;
    }


    public void setZookeeperManager( final Zookeeper zookeeperManager )
    {
        this.zookeeperManager = zookeeperManager;
    }
}
