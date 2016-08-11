/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package io.subutai.plugin.mongodb.impl.common;


import java.util.Set;

import org.apache.derby.impl.sql.catalog.SYSSTATISTICSRowFactory;

import io.subutai.common.command.RequestBuilder;
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.api.Timeouts;


/**
 * Holds all mongo related commands
 */
public class Commands
{

    private static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + MongoClusterConfig.PRODUCT_NAME;


    public static CommandDef getUnregisterSecondaryNodeFromPrimaryCommandLine( int dataNodePort,
                                                                               String removeNodehostname,
                                                                               String domainName )
    {
        return new CommandDef( "Unregister node from replica",
                String.format( "mongo --port %s --eval \"rs.remove('%s.%s:%s');\"", dataNodePort, removeNodehostname,
                        domainName, dataNodePort ), 300 );
    }


    public static RequestBuilder getPidCommand()
    {
        return new RequestBuilder( " ps aux | grep '[m]ongod --config' | awk  -F ' ' '{print $2}' " ).withTimeout( 30 );
    }


    public static CommandDef getStartRouterCommandLine( int routerPort, int cfgSrvPort, String domainName,
                                                        Set<EnvironmentContainerHost> configServers )
    {

        StringBuilder configServersArg = new StringBuilder();
        for ( EnvironmentContainerHost c : configServers )
        {
            HostInterface hostInterface = c.getInterfaceByName( "eth0" );
            configServersArg.append( hostInterface.getIp() ).append( "." ).
                    append( ":" ).append( cfgSrvPort ).append( "," );
        }
        //drop comma
        if ( configServersArg.length() > 0 )
        {
            configServersArg.setLength( configServersArg.length() - 1 );
        }
        return new CommandDef( "Start router(s)",
                String.format( "mongos --configdb %s --port %s --fork --logpath %s/mongodb.log",
                        configServersArg.toString(), routerPort, Constants.LOG_DIR ),
                Timeouts.START_ROUTER_TIMEOUT_SEC );
    }


    public static CommandDef getStartDataNodeCommandLine( int dataNodePort )
    {
        return new CommandDef( "Start data node", String.format(
                //TODO add after solving problem with utf encoding
                //export LANGUAGE=en_US.UTF-8 && export LANG=en_US.UTF-8 && "
                //+ "export LC_ALL=en_US.UTF-8 &&
                "mongod --config %s --port %s --fork --logpath %s/mongodb.log", Constants.DATA_NODE_CONF_FILE,
                dataNodePort, Constants.LOG_DIR ), Timeouts.START_DATE_NODE_TIMEOUT_SEC );
    }


    public static CommandDef getFindPrimaryNodeCommandLine( int dataNodePort )
    {
        return new CommandDef( "Find primary node",
                String.format( "/bin/echo 'db.isMaster()' | mongo --port %s", dataNodePort ), 30 );
    }


    public static RequestBuilder getReplSetCommand( String repl )
    {
        return new RequestBuilder( String.format( "bash /etc/scripts/mongo-conf.sh repl %s", repl ) );
    }


    public static RequestBuilder getSetBindIpCommand()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh bind.ip 0.0.0.0" );
    }


    public static RequestBuilder getSetLocaleCommand()
    {
        return new RequestBuilder( "export LC_ALL=C" );
    }


    public static RequestBuilder getReplInitiateCommand()
    {
        return new RequestBuilder( "mongo --eval \"rs.initiate();\"" );
    }


    public static RequestBuilder getAddDataReplCommand( String hostname )
    {
        return new RequestBuilder( String.format( "mongo --eval \"rs.add(\\\"%s\\\");\"", hostname ) );
    }


    public static RequestBuilder getSetPortCommand()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh config.port 27019" );
    }


    public static RequestBuilder getSetClusterRoleCommand()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh config.role configsvr" );
    }


    public static RequestBuilder getAddConfigReplCommand( String servers )
    {
        return new RequestBuilder( String.format(
                "mongo --port 27019 --eval \"rs.initiate( {_id: \\\"configReplSet\\\",configsvr: true, members: "
                        + "[%s]} )\";", servers ) );
    }


    public static RequestBuilder getCommentStorageCommand()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh comment storage" );
    }


    public static RequestBuilder getSetConfigDbCommand( String hostname )
    {
        return new RequestBuilder(
                String.format( "bash /etc/scripts/mongo-conf.sh mongos.config \"configReplSet\\/%s:27019\"",
                        hostname ) );
    }


    public static RequestBuilder getRenametoMongosCommand()
    {
        return new RequestBuilder( "mv /etc/systemd/system/mongodb.service /etc/systemd/system/mongos.service" );
    }


    public static RequestBuilder getRenametoMongodbCommand()
    {
        return new RequestBuilder( "mv /etc/systemd/system/mongos.service /etc/systemd/system/mongodb.service" );
    }


    public static RequestBuilder getChangeServiceCommand( String oldService, String newService )
    {
        return new RequestBuilder(
                String.format( "bash /etc/scripts/mongo-conf.sh service.change %s %s", oldService, newService ) );
    }


    public static RequestBuilder getSetShardCommand( String hostname, final String replicaSetName )
    {
        return new RequestBuilder(
                String.format( "mongo --eval \"sh.addShard(\\\"%s\\/%s:27017\\\");\"", replicaSetName, hostname ) );
    }


    public static RequestBuilder getMongosRestartCommand()
    {
        return new RequestBuilder( "systemctl restart mongos ; sleep 10" );
    }


    public static RequestBuilder getMongosStopCommand()
    {
        return new RequestBuilder( "systemctl stop mongos" );
    }


    public static RequestBuilder getMongosStartCommand()
    {
        return new RequestBuilder( "systemctl start mongos" );
    }


    public static RequestBuilder getMongosStatusCommand()
    {
        return new RequestBuilder( "systemctl status mongos" );
    }


    public static RequestBuilder getMongodbStatusCommand()
    {
        return new RequestBuilder( "service mongodb status" );
    }


    public static RequestBuilder getMongodbStartCommand()
    {
        return new RequestBuilder( "service mongodb start" );
    }


    public static RequestBuilder getMongodbStopCommand()
    {
        return new RequestBuilder( "systemctl stop mongodb" );
    }


    public static RequestBuilder getMongoDBRestartCommand()
    {
        return new RequestBuilder( "service mongodb restart" );
    }


    public static RequestBuilder getShutDownCommand()
    {
        return new RequestBuilder( "mongo localhost:27019/admin --eval \"db.shutdownServer()\"" );
    }


    public static RequestBuilder getRemoveFromReplicaSetCommand( final String hostname )
    {
        return new RequestBuilder( String.format( "mongo --eval \"rs.remove(\\\"%s:27017\\\")\"", hostname ) );
    }


    public static RequestBuilder getResetDataConfig()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh data.reset default" );
    }


    public static RequestBuilder getResetMongosConfig()
    {
        return new RequestBuilder( "bash /etc/scripts/mongo-conf.sh mongos.reset default" );
    }


    public static RequestBuilder getUncommentStorageCommand()
    {
        return null;
    }


    public static RequestBuilder getReloadDaemonCommand()
    {
        return new RequestBuilder( "systemctl daemon-reload" );
    }
}