package io.subutai.plugin.hbase.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.hbase.api.HBaseConfig;


public class Commands
{

    public final static String PACKAGE_NAME = Common.PACKAGE_PREFIX + "hbase2";


    public final static String UPDATE_COMAND = "apt-get --force-yes --assume-yes update";
    public static final String INSTALL_COMMAND =
            String.format( "apt-get install %s -y --allow-unauthenticated", PACKAGE_NAME );


    public static RequestBuilder getInstallCommand()
    {

        return new RequestBuilder( String.format( "apt-get install %s -y --allow-unauthenticated", PACKAGE_NAME ) )
                .withTimeout( 20000 );
    }


    public static RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "source /etc/profile ; start-hbase.sh" ).daemon();
    }


    public static RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "source /etc/profile ; stop-hbase.sh" ).withTimeout( 360 );
    }


    public static RequestBuilder getStartRegionServerCommand()
    {
        return new RequestBuilder( "source /etc/profile ; hbase-daemon.sh start regionserver" ).withTimeout( 360 );
    }


    public static RequestBuilder getStopRegionServerCommand()
    {
        return new RequestBuilder( "kill `jps | grep \"HRegionServer\" | cut -d \" \" -f 1`" ).withTimeout( 360 );
    }


    public static RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "jps" );
    }


    public static RequestBuilder getConfigBackupMastersCommand( String backUpMasters )
    {
        return new RequestBuilder( String.format( ". /etc/profile && backUpMasters.sh add %s", backUpMasters ) );
    }


    public static RequestBuilder getRemoveBackupMasterCommand( String backUpMaster )
    {
        return new RequestBuilder( String.format( ". /etc/profile && backUpMasters.sh remove %s", backUpMaster ) );
    }


    public static RequestBuilder getConfigQuorumCommand( String quorumPeers )
    {
        return new RequestBuilder( String.format( ". /etc/profile && quorum.sh %s", quorumPeers ) );
    }


    public static RequestBuilder getConfigRegionCommand( String regionServers )
    {
        return new RequestBuilder( String.format( ". /etc/profile && region.sh add %s", regionServers ) );
    }


    public static RequestBuilder getRemoveRegionServerCommand( String regionServer )
    {
        return new RequestBuilder( String.format( "sed -i -e \"/%s/d\" /opt/hbase/conf/regionservers", regionServer ) );
    }


    public static RequestBuilder getAddRegionServerCommand( String regionServer )
    {
        return new RequestBuilder( String.format( "echo \"%s\" >> /opt/hbase/conf/regionservers", regionServer ) );
    }


    public static RequestBuilder getClearRegionServerConfFile()
    {
        return new RequestBuilder( String.format( ". /etc/profile && region.sh remove" ) );
    }


    public static RequestBuilder getClearBackupMastersConfFile()
    {
        return new RequestBuilder( String.format( ". /etc/profile && backUpMasters.sh remove" ) );
    }


    public static RequestBuilder getConfigMasterCommand( String hadoopNameNodeHostname, String hMasterMachineHostname )
    {
        return new RequestBuilder(
                String.format( ". /etc/profile && master.sh %s %s", hadoopNameNodeHostname, hMasterMachineHostname ) );
    }


    public static RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }


    public static RequestBuilder getStartRegionServer()
    {
        return new RequestBuilder( ". /etc/profile &&  hbase-daemon.sh start regionserver " ).daemon();
    }


    public static RequestBuilder getStopRegionServer()
    {
        return new RequestBuilder( ". /etc/profile &&  hbase-daemon.sh stop regionserver " ).daemon();
    }


    public static RequestBuilder getStartHMaster()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemon.sh start master" );
    }


    public static RequestBuilder getStopHMaster()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemon.sh stop master" );
    }


    public static RequestBuilder getStartBackupMaster()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemons.sh start master-backup" );
    }


    public static RequestBuilder getStopBackupMaster()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemons.sh stop master-backup" );
    }


    public static RequestBuilder getStartHquorum()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemons.sh start zookeeper" );
    }


    public static RequestBuilder getStopHquorum()
    {
        return new RequestBuilder( ". /etc/profile && hbase-daemons.sh stop zookeeper" );
    }


    public static RequestBuilder getUninstallCommand()
    {

        return new RequestBuilder( String.format( "apt-get purge %s -y --allow-unauthenticated", PACKAGE_NAME ) )
                .withTimeout( 360 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public static String addHbaseProperty( String cmd, String propFile, String property, String value )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "bash /opt/hbase/conf/hbase-property.sh " ).append( cmd ).append( " " );
        sb.append( propFile ).append( " " ).append( property );
        if ( value != null )
        {
            sb.append( " " ).append( value );
        }
        return sb.toString();
    }


    public static String setHbaseMasterInfoPort()
    {
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.master.info.port", "16010" );
    }


    public static String setHbaseRootDir( final String hmasterIp )
    {
        String uri = String.format( "hdfs://%s:8020/hbase", hmasterIp );
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.rootdir", uri );
    }


    public static String setHbaseClusterDistributed()
    {
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.cluster.distributed", "true" );
    }


    public static String setHbaseZookeeperQuorum( final String hmasterIp )
    {
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.zookeeper.quorum", hmasterIp );
    }


    public static String setHbaseZookeeperDataDir( final String hmasterIp )
    {
        String uri = String.format( "hdfs://%s:8020/zookeeper", hmasterIp );
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.zookeeper.property.dataDir", uri );
    }


    public static String setHbaseZookeeperClientPort()
    {
        return addHbaseProperty( "add", "hbase/conf/hbase-site.xml", "hbase.zookeeper.property.clientPort", "2181" );
    }


    public static String setRegionServers( final String slavesHostnames )
    {
        return String.format( "rm /opt/hbase/conf/regionservers ; echo -e \"%s\" > /opt/hbase/conf/regionservers",
                slavesHostnames );
    }
}
