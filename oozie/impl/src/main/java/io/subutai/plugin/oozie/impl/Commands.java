package io.subutai.plugin.oozie.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.oozie.api.OozieClusterConfig;


public class Commands
{

    public static final String SERVER_PACKAGE_NAME = Common.PACKAGE_PREFIX + "oozie2";


    public Commands()
    {

    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
    }


    public static RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }


    public static RequestBuilder getStartServerCommand()
    {
        return new RequestBuilder( "/opt/oozie/bin/oozied.sh start" ).daemon();
    }


    public static RequestBuilder getStopServerCommand()
    {
        return new RequestBuilder( "/opt/oozie/bin/oozied.sh stop" );
    }


    public static RequestBuilder getStatusServerCommand()
    {
        return new RequestBuilder( "jps" );
    }


    public static RequestBuilder getBuildWarCommand()
    {
        return new RequestBuilder( "/opt/oozie/bin/oozie-setup.sh prepare-war" );
    }


    public static RequestBuilder getCopyToHdfsCommand( String hostname )
    {
        return new RequestBuilder(
                String.format( "/opt/oozie/bin/oozie-setup.sh sharelib create -fs hdfs://%s:8020", hostname ) );
    }


    public static RequestBuilder getCleanHdfsCommand()
    {
        return new RequestBuilder( "source /etc/profile ; hdfs dfs -rm -r /user" );
    }


    public static RequestBuilder getConfigureRootHostsCommand()
    {
        return new RequestBuilder(
                "bash /opt/oozie/conf/hadoop-property.sh add core-site.xml hadoop.proxyuser.root.hosts '\\*' " )
                .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static RequestBuilder getConfigureRootGroupsCommand()
    {

        return new RequestBuilder(
                "bash /opt/oozie/conf/hadoop-property.sh add core-site.xml hadoop.proxyuser.root.groups '\\*' " )
                .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static RequestBuilder getRemoveRootHostsCommand()
    {
        return new RequestBuilder(
                "bash /opt/oozie/conf/hadoop-property.sh remove core-site.xml hadoop.proxyuser.root.hosts '\\*' " )
                .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static RequestBuilder getRemoveRootGroupsCommand()
    {

        return new RequestBuilder(
                "bash /opt/oozie/conf/hadoop-property.sh remove core-site.xml hadoop.proxyuser.root.groups '\\*' " )
                .withTimeout( 600 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static RequestBuilder getUninstallServerCommand()
    {
        return

                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + SERVER_PACKAGE_NAME ).withTimeout( 90 )
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


    public static String getStartYarnCommand()
    {
        return "source /etc/profile ; start-yarn.sh";
    }


    public static String getStartDfsCommand()
    {
        return "source /etc/profile ; start-dfs.sh";
    }


    public static String getStopYarnCommand()
    {
        return "source /etc/profile ; stop-yarn.sh";
    }


    public static String getStopDfsCommand()
    {
        return "source /etc/profile ; stop-dfs.sh";
    }
}
