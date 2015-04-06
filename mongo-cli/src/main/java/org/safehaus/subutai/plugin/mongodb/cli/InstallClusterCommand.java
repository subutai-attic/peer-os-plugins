package org.safehaus.subutai.plugin.mongodb.cli;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.mongodb.api.Mongo;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "mongo", name = "install-cluster", description = "Command to install Mongo cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", required = true, multiValued = false, description = "accumulo cluster "
            + "name" )
    String clusterName;

    @Argument( index = 1, name = "environmentId", description = "The environment id to setup Mongo cluster", required =
            true,
            multiValued = false )
    String environmentId;

    @Argument( index = 2, name = "domainName", description = "The domain name for Mongo cluster", required = true,
            multiValued = false )
    String domainName;

    @Argument( index = 3, name = "replicaSetName", description = "The replica set name for Mongo cluster", required
            = true,
            multiValued = false )
    String replicaSetName = null;

    @Argument( index = 4, name = "dataNodePort", description = "The port of the datanode for Mongo cluster", required = true,
            multiValued = false )
    int dataNodePort;

    @Argument( index = 5, name = "routerNodePort", description = "The port of the router node for Mongo cluster", required = true,
            multiValued = false )
    int routerNodePort;

    @Argument( index = 6, name = "configServerPort", description = "The port of the config server node for Mongo cluster", required = true,
            multiValued = false )
    int configServerPort;

    @Argument( index = 7, name = "configNodes", description = "The list of config nodes", required = true,
            multiValued = false )
    String configNodes[] = null;

    @Argument( index = 8, name = "routerNodes", description = "The list of router nodes", required = true,
            multiValued = false )
    String routerNodes[] = null;

    @Argument( index = 9, name = "dataNodes", description = "The list of data nodes", required = true,
            multiValued = false )
    String dataNodes[] = null;

    private Tracker tracker;
    private Mongo mongoManager;


    protected Object doExecute()
    {

        MongoClusterConfig mongoConfig = mongoManager.newMongoClusterConfigInstance();
        mongoConfig.setDomainName( domainName );
        mongoConfig.setReplicaSetName( replicaSetName );
        mongoConfig.setRouterPort( routerNodePort );
        mongoConfig.setDataNodePort( dataNodePort );
        mongoConfig.setCfgSrvPort( configServerPort );
        mongoConfig.setEnvironmentId( UUID.fromString( environmentId ) );
        mongoConfig.setClusterName( clusterName );


            Set<UUID> configs = new HashSet<>();
            for ( String node : configNodes )
            {
                configs.add( UUID.fromString( node ) );
            }
            mongoConfig.getConfigHosts().addAll( configs );


            Set<UUID> datas = new HashSet<>();
            for ( String node : dataNodes )
            {
                datas.add( UUID.fromString( node ) );
            }
//            mongoConfig.getDataHostIds().addAll( datas );


            Set<UUID> routers = new HashSet<>();
            for ( String node : routerNodes )
            {
                routers.add( UUID.fromString( node ) );
            }
            mongoConfig.getRouterHosts().addAll( routers );

        UUID uuid = mongoManager.installCluster( mongoConfig );
        System.out.println(
                "Install operation is " + waitUntilOperationFinish( tracker, uuid ) + "." );
        return null;
    }

    protected static OperationState waitUntilOperationFinish( Tracker tracker, UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( MongoClusterConfig.PRODUCT_KEY, uuid );
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


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Mongo getMongoManager()
    {
        return mongoManager;
    }


    public void setMongoManager( final Mongo mongoManager )
    {
        this.mongoManager = mongoManager;
    }

}

