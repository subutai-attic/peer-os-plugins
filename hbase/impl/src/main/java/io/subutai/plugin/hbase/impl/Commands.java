package io.subutai.plugin.hbase.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.hbase.api.HBaseConfig;


public class Commands
{

    public final static String PACKAGE_NAME = Common.PACKAGE_PREFIX + HBaseConfig.PRODUCT_KEY.toLowerCase();


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 5000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
    }


    public static RequestBuilder getInstallCommand()
    {

        return new RequestBuilder( "apt-get --assume-yes --force-yes install " + PACKAGE_NAME ).withTimeout( 5000 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public static RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "service hbase start" ).daemon();
    }


    public static RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "service hbase stop" ).withTimeout( 360 );
    }


    public static RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "service hbase status" );
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
        return new RequestBuilder( String.format( ". /etc/profile && region.sh remove %s", regionServer ) );
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

        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 360 )
                                                                                             .withStdOutRedirection(
                                                                                                     OutputRedirection.NO );
    }
}
