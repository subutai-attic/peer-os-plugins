package io.subutai.plugin.accumulo.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;


public class Commands
{
    public static final String CREATE_CONFIG_FILE = "touch /opt/zookeeper/conf/zoo.cfg";
    public static final String REMOVE_SNAPS_COMMAND = "rm -rf /var/lib/zookeeper/data/version-2/*";
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + "accumulo2";


    public static RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes install %s", PACKAGE_NAME ) )
                .withTimeout( 2000 );
    }


    public static RequestBuilder getUninstallAccumuloCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes purge %s", PACKAGE_NAME ) )
                .withTimeout( 300 );
    }


    public static RequestBuilder getUninstallZkCommand()
    {
        return new RequestBuilder( String.format( "apt-get --force-yes --assume-yes purge %s", "subutai-zookeeper2" ) )
                .withTimeout( 300 );
    }


    public static RequestBuilder getCheckInstallationCommand()
    {
        return new RequestBuilder( String.format( "dpkg -l | grep '^ii' | grep %s", Common.PACKAGE_PREFIX ) );
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
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


    public static RequestBuilder getRestartZkServerCommand()
    {
        return new RequestBuilder( "/opt/zookeeper/bin/zkServer.sh restart" );
    }


    public static RequestBuilder getCopyConfigsCommand()
    {
        return new RequestBuilder( "cp /opt/accumulo/conf/examples/512MB/standalone/* /opt/accumulo/conf/" );
    }


    public static RequestBuilder getRemoveConfigsCommand()
    {
        return new RequestBuilder( "rm /opt/accumulo/conf/accumulo-site.xml ; rm /opt/accumulo/conf/masters ; rm "
                + "/opt/accumulo/conf/slaves" );
    }


    public static RequestBuilder getSetSlavesCommand( final String hostnames )
    {
        return new RequestBuilder( String.format( "echo -e \"%s\" > /opt/accumulo/conf/slaves", hostnames ) );
    }


    public static RequestBuilder getClearSlaveCommand( String hostname )
    {
        return new RequestBuilder( String.format( "sed -i -e \"/%s/d\" /opt/accumulo/conf/slaves", hostname ) );
    }


    public static RequestBuilder getSetMasterCommand( final String hostname )
    {
        return new RequestBuilder( String.format( "echo -e \"%s\" > /opt/accumulo/conf/masters", hostname ) );
    }


    public static RequestBuilder getClearMasterCommand( String hostname )
    {
        return new RequestBuilder( String.format( "sed -i -e \"/%s/d\" /opt/accumulo/conf/masters", hostname ) );
    }


    private static String addAccumuloProperty( String cmd, String propFile, String property, String value )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "bash /opt/accumulo/conf/accumulo-property.sh " ).append( cmd ).append( " " );
        sb.append( propFile ).append( " " ).append( property );
        if ( value != null )
        {
            sb.append( " " ).append( value );
        }
        return sb.toString();
    }


    public static RequestBuilder getSetInstanceVolume( final String hostname )
    {
        String uri = String.format( "hdfs://%s:8020/accumulo", hostname );
        return new RequestBuilder(
                addAccumuloProperty( "add", "accumulo/conf/accumulo-site.xml", "instance.volumes", uri ) );
    }


    public static RequestBuilder getSetInstanceZkHost( final String hostnames )
    {
        return new RequestBuilder(
                addAccumuloProperty( "add", "accumulo/conf/accumulo-site.xml", "instance.zookeeper.host", hostnames ) );
    }


    public static RequestBuilder getSetPassword( final String password )
    {
        return new RequestBuilder(
                addAccumuloProperty( "add", "accumulo/conf/accumulo-site.xml", "trace.token.property.password",
                        password ) );
    }


    public static RequestBuilder getSetJavaHeapSize()
    {
        return new RequestBuilder( "sed -i -e 's/ACCUMULO_TSERVER_OPTS=\"${POLICY} -Xmx128m "
                + "-Xms128m/ACCUMULO_TSERVER_OPTS=\"${POLICY} -Xmx1024m -Xms1024m/g' /opt/accumulo/conf/accumulo-env"
                + ".sh" );
    }


    public static RequestBuilder getAccumucoSiteConfig()
    {
        return new RequestBuilder(
                "cp /opt/accumulo/conf/accumulo-site.xml.example /opt/accumulo/conf/accumulo-site.xml" );
    }


    public static RequestBuilder getInitializeCommand( final String password, final String clusterName )
    {
        return new RequestBuilder(
                String.format( "source /etc/profile ; /opt/accumulo/bin/accumulo init --instance-name %s --password %s",
                        clusterName, password ) );
    }


    public static RequestBuilder getStartMasterCommand()
    {
        return new RequestBuilder( "source /etc/profile ; /opt/accumulo/bin/start-all.sh" );
    }


    public static RequestBuilder getStartSlaveCommand()
    {
        return new RequestBuilder( "source /etc/profile ; /opt/accumulo/bin/start-here.sh" );
    }


    public static RequestBuilder getStopMasterCommand()
    {
        return new RequestBuilder( "source /etc/profile ; /opt/accumulo/bin/stop-all.sh" );
    }


    public static RequestBuilder getStopSlaveCommand()
    {
        return new RequestBuilder( "source /etc/profile ; /opt/accumulo/bin/stop-here.sh" );
    }


    public static RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "jps" );
    }


    public static RequestBuilder getStopAllCommand()
    {
        return new RequestBuilder( "kill `jps | grep \"Main\" | cut -d \" \" -f 1`" );
    }


    public static RequestBuilder getDeleteHdfsFolderCommand()
    {
        return new RequestBuilder( "source /etc/profile ; hdfs dfs -rmr /accumulo" );
    }
}
