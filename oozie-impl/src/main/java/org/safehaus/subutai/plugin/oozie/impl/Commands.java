package org.safehaus.subutai.plugin.oozie.impl;


import org.safehaus.subutai.common.command.OutputRedirection;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME =
            Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_KEY.toLowerCase() + "-*";
    public static final String SERVER_PACKAGE_NAME =
            Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_KEY.toLowerCase() + "-server";
    public static final String CLIENT_PACKAGE_NAME =
            Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_KEY.toLowerCase() + "-client";


    public Commands()
    {

    }


    public static String make( CommandType type )
    {
        switch ( type )
        {
            case STATUS:
                return "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            case INSTALL_SERVER:
                return "export DEBIAN_FRONTEND=noninteractive && apt-get --assume-yes --force-yes install "
                        + SERVER_PACKAGE_NAME;
            case INSTALL_CLIENT:
                return "export DEBIAN_FRONTEND=noninteractive && apt-get --assume-yes --force-yes install "
                        + CLIENT_PACKAGE_NAME;
            case PURGE:
                StringBuilder sb = new StringBuilder();
                sb.append( "apt-get --force-yes --assume-yes " );
                sb.append( type.toString().toLowerCase() ).append( " " );
                sb.append( PACKAGE_NAME );
                return sb.toString();
            case START:
            case STOP:
                String s = "service oozie-ng " + type.toString().toLowerCase() + " agent";
                if ( type == CommandType.START )
                {
                    s += " &"; // TODO:
                }
                return s;
            default:
                throw new AssertionError( type.name() );
        }
    }


    public static RequestBuilder getCheckInstalledCommand( String packageName )
    {
        String command = "dpkg -s " + packageName;
        return new RequestBuilder( command );
    }


    public static RequestBuilder getConfigureCommand()
    {
        return new RequestBuilder( "dpkg --configure -a && rm /var/cache/apt/archives/lock && rm /var/lib/dpkg/lock" );
    }


    public static RequestBuilder getStartServerCommand()
    {
        return new RequestBuilder( "service oozie-server start &" );
    }


    public static RequestBuilder getStopServerCommand()
    {
        return new RequestBuilder( "service oozie-server stop" );
    }


    public static RequestBuilder getStatusServerCommand()
    {
        return new RequestBuilder( "service oozie-server status" );
    }


    public static RequestBuilder getConfigureRootHostsCommand( String param )
    {

        return new RequestBuilder( String.format(
                ". /etc/profile && $HADOOP_HOME/bin/hadoop-property.sh add core-site.xml hadoop.proxyuser"
                        + ".root.hosts %s", param ) );
    }


    public static RequestBuilder getConfigureRootGroupsCommand()
    {

        return new RequestBuilder( String.format(
                ". /etc/profile && $HADOOP_HOME/bin/hadoop-property.sh add core-site.xml hadoop.proxyuser"
                        + ".root.groups '\\*' " ) );
    }


    public static RequestBuilder getUninstallServerCommand()
    {
        return

                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + SERVER_PACKAGE_NAME ).withTimeout( 90 )
                                                                                                     .withStdOutRedirection(
                                                                                                             OutputRedirection.NO )

                ;
    }


    public static RequestBuilder getUninstallClientsCommand()
    {
        return

                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + CLIENT_PACKAGE_NAME ).withTimeout( 90 )
                                                                                                     .withStdOutRedirection(
                                                                                                             OutputRedirection.NO )

                ;
    }
}
