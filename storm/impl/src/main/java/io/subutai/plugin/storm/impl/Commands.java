package io.subutai.plugin.storm.impl;


import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.storm.api.StormClusterConfiguration;


public class Commands
{

    public static final String PACKAGE_NAME =
            Common.PACKAGE_PREFIX + StormClusterConfiguration.PRODUCT_NAME.toLowerCase();
    private static final String EXEC_PROFILE = ". /etc/profile";
    public static final String CREATE_CONFIG_FILE = "touch /opt/zookeeper/conf/zoo.cfg";
    public static final String REMOVE_SNAPS_COMMAND = "rm -rf /var/lib/zookeeper/data/version-2/*";


    public static String make( CommandType type )
    {
        return make( type, null );
    }


    public static String make( CommandType type, StormService service )
    {
        StringBuilder sb = null;
        switch ( type )
        {
            case LIST:
                return "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            case INSTALL:
            case PURGE:
                sb = new StringBuilder( EXEC_PROFILE );
                // NODE: fix related to apt-get update
                if ( type == CommandType.INSTALL )
                {
                    sb.append( " && sleep 20" );
                }
                sb.append( " && apt-get --force-yes --assume-yes " );
                sb.append( type.toString().toLowerCase() ).append( " " );
                sb.append( PACKAGE_NAME );
                break;
            case STATUS:
            case START:
            case STOP:
            case RESTART:
            case KILL:
                if ( service != null )
                {
                    sb = new StringBuilder();
                    sb.append( "service " ).append( service.getService() );
                    sb.append( " " ).append( type.toString().toLowerCase() );
                }
                break;
            default:
                throw new AssertionError( type.name() );
        }
        return sb != null ? sb.toString() : null;
    }


    public static String configure( String cmd, String propFile, String property, String value )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( EXEC_PROFILE ).append( " && storm-property.sh " ).append( cmd );
        sb.append( " " ).append( propFile ).append( " " ).append( property );
        if ( value != null )
        {
            sb.append( " " ).append( value );
        }

        return sb.toString();
    }


    public static String getConfigureClusterCommand( String zooCfgFileContents, String zooCfgFilePath, int id )
    {
        return String
                .format( "bash /opt/zookeeper/conf/zookeeper-setID.sh %s && echo '%s' > %s", id, zooCfgFileContents,
                        zooCfgFilePath );
    }


    public static RequestBuilder getStartZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh start" );
    }


    public static RequestBuilder getStopZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh stop" );
    }


    public static RequestBuilder getCreateDataDirectoryCommand()
    {
        return new RequestBuilder( "mkdir -p /var/lib/storm/data" );
    }


    public static RequestBuilder getConfigureZookeeperServersCommand( String ips )
    {
        return new RequestBuilder( String.format(
                "sed -i -e 's/storm.zookeeper.servers:/storm.zookeeper.servers:  %s/g' /opt/storm/conf/storm.yaml",
                ips ) );
    }


    public static RequestBuilder getConfigureNimbusSeedsCommand( String ip )
    {
        return new RequestBuilder(
                String.format( "sed -i -e 's/nimbus.seeds:/nimbus.seeds:  %s/g' /opt/storm/conf/storm.yaml", ip ) );
    }


    public static RequestBuilder getStartNimbusCommand()
    {
        return new RequestBuilder( "start-stop-daemon --start --background --exec /opt/storm/bin/storm nimbus" );
    }


    public static RequestBuilder getStartStormUICommand()
    {
        return new RequestBuilder( "start-stop-daemon --start --background --exec /opt/storm/bin/storm ui" );
    }


    public static RequestBuilder getStartSupervisorCommand()
    {
        return new RequestBuilder( "start-stop-daemon --start --background --exec /opt/storm/bin/storm supervisor" );
    }
}
