/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package io.subutai.plugin.mongodb.impl.common;


import java.util.Set;

import io.subutai.common.command.RequestBuilder;
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


    public static CommandDef getRegisterSecondaryNodeWithPrimaryCommandLine( String secondaryNodeHostname,
                                                                             int dataNodePort, String domainName )
    {
        return new CommandDef( "Register node with replica",
                String.format( "mongo --port %s --eval \"%s\"", dataNodePort,
                        "rs.add('" + secondaryNodeHostname + "." + domainName + ":" + dataNodePort + "');" ), 900 );
    }


    public static CommandDef getUnregisterSecondaryNodeFromPrimaryCommandLine( int dataNodePort,
                                                                               String removeNodehostname,
                                                                               String domainName )
    {
        return new CommandDef( "Unregister node from replica",
                String.format( "mongo --port %s --eval \"rs.remove('%s.%s:%s');\"", dataNodePort, removeNodehostname,
                        domainName, dataNodePort ), 300 );
    }


    public static CommandDef getStopNodeCommand()
    {
        return new CommandDef( "Stop node", "/usr/bin/pkill -2 mongo", Timeouts.STOP_NODE_TIMEOUT_SEC );
    }


    public static CommandDef checkIfMongoInstalled()
    {
        return new CommandDef( "Check if mongo installed",
                String.format( "dpkg-query -W -f='${Status}\\n' %s", PACKAGE_NAME ), 60 );
    }


    public static CommandDef installMongoCommand()
    {
        return new CommandDef( String.format( "Update and install %s%s package", Common.PACKAGE_PREFIX,
                MongoClusterConfig.PRODUCT_NAME ),
                String.format( "apt-get --yes --force-yes install %s", PACKAGE_NAME ), 900 );
    }


    public static CommandDef getUninstallMongoCommand()
    {
        return new CommandDef( String.format( "Purge %s package", PACKAGE_NAME ),
                String.format( "apt-get --force-yes --assume-yes purge %s", PACKAGE_NAME ), 900 );
    }


    public static CommandDef getClearMongoConfigsCommand()
    {
        return new CommandDef( String.format( "Clear %s config files", PACKAGE_NAME ),
                String.format( "rm -r %s %s", Constants.CONFIG_DIR, Constants.DATA_NODE_CONF_FILE ), 900 );
    }


    public static CommandDef getUninstallClearMongoConfigsCommand()
    {
        return new CommandDef( String.format( "Purge & clear config files for %s package", PACKAGE_NAME ),
                String.format( "%s && %s", getUninstallMongoCommand().getCommand(),
                        getClearMongoConfigsCommand().getCommand() ), 900 );
    }


    public static CommandDef getSetReplicaSetNameCommandLine( String replicaSetName )
    {
        return new CommandDef( "Set replica set name",
                String.format( "sed -i 's/.*replSet =.*/replSet = %s/1' %s", replicaSetName,
                        Constants.DATA_NODE_CONF_FILE ), 30 );
    }


    // LIFECYCLE COMMANDS =======================================================
    public static CommandDef getStartConfigServerCommand( int cfgSrvPort )
    {
        return new CommandDef( "Start config server(s)", String.format(
                "/bin/mkdir -p %s ; mongod --configsvr --dbpath %s --port %s --fork --logpath %s/mongodb.log",
                Constants.CONFIG_DIR, Constants.CONFIG_DIR, cfgSrvPort, Constants.LOG_DIR ),
                Timeouts.START_CONFIG_SERVER_TIMEOUT_SEC );
    }


    public static CommandDef getStartRouterCommandLine( int routerPort, int cfgSrvPort, String domainName,
                                                        Set<EnvironmentContainerHost> configServers )
    {

        StringBuilder configServersArg = new StringBuilder();
        for ( EnvironmentContainerHost c : configServers )
        {
            configServersArg.append( c.getHostname() ).append( "." ).append( domainName ).
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
                "export LANGUAGE=en_US.UTF-8 && export LANG=en_US.UTF-8 && "
                        + "export LC_ALL=en_US.UTF-8 && mongod --config %s --port %s --fork --logpath %s/mongodb.log",
                Constants.DATA_NODE_CONF_FILE, dataNodePort, Constants.LOG_DIR ),
                Timeouts.START_DATE_NODE_TIMEOUT_SEC );
    }


    public static CommandDef getInitiateReplicaSetCommandLine( int port )
    {
        return new CommandDef( "Initiate replica set",
                String.format( "mongo --port %d --eval \"rs.initiate();\" ; sleep 30", port ), 180 );
    }


    public static CommandDef getFindPrimaryNodeCommandLine( int dataNodePort )
    {
        return new CommandDef( "Find primary node",
                String.format( "/bin/echo 'db.isMaster()' | mongo --port %s", dataNodePort ), 30 );
    }


    public static CommandDef getCheckConfigServer()
    {
        return new CommandDef( "Check node", "ps axu | grep \"[m]ongod --configsvr\"",
                Timeouts.CHECK_NODE_STATUS_TIMEOUT_SEC );
    }


    public static CommandDef getCheckRouterNode()
    {
        return new CommandDef( "Check node", "ps axu | grep \"[m]ongos --configdb\"",
                Timeouts.CHECK_NODE_STATUS_TIMEOUT_SEC );
    }


    public static CommandDef getCheckDataNode()
    {
        return new CommandDef( "Check node", "ps axu | grep \"[m]ongod --config \"",
                Timeouts.CHECK_NODE_STATUS_TIMEOUT_SEC );
    }


    public static RequestBuilder getPidCommand()
    {
        return new RequestBuilder( " ps aux | grep '[m]ongod --config' | awk  -F ' ' '{print $2}' " ).withTimeout( 30 );
    }
}
