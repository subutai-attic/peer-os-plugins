package io.subutai.plugin.oozie.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.oozie.api.OozieClusterConfig;


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
                return "apt-get --assume-yes --force-yes install " + SERVER_PACKAGE_NAME;
            case INSTALL_CLIENT:
                return "apt-get --assume-yes --force-yes install " + CLIENT_PACKAGE_NAME;
            case PURGE:
                return "apt-get --force-yes --assume-yes " + type.toString().toLowerCase() + " " + PACKAGE_NAME;
            case START:
            case STOP:
                String s = "service oozie-ng " + type.toString().toLowerCase() + " agent";
                if ( type == CommandType.START )
                {
                    s += " &"; // TODO:
                }
                return s;
            case UPDATE:
                return "apt-get --force-yes --assume-yes update";
            default:
                throw new AssertionError( type.name() );
        }
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 ).withStdOutRedirection(
                OutputRedirection.NO );
    }


    public static RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
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
        return new RequestBuilder( "service oozie-server start" ).daemon();
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
                        + ".root.hosts %s", param ) ).withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static RequestBuilder getConfigureRootGroupsCommand()
    {

        return new RequestBuilder( String.format(
                ". /etc/profile && $HADOOP_HOME/bin/hadoop-property.sh add core-site.xml hadoop.proxyuser"
                        + ".root.groups '\\*' " ) ).withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
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


    public static RequestBuilder getInstallServerCommand()
    {
        return

                new RequestBuilder( "apt-get --assume-yes --force-yes install " + SERVER_PACKAGE_NAME )
                        .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO )

                ;
    }


    public static RequestBuilder getInstallClientsCommand()
    {
        return

                new RequestBuilder( "apt-get --assume-yes --force-yes install " + CLIENT_PACKAGE_NAME )
                        .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO )

                ;
    }
}
