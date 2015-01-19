package org.safehaus.subutai.plugin.hbase.impl;


import org.safehaus.subutai.common.command.OutputRedirection;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;


public class Commands
{

    public final static String PACKAGE_NAME = Common.PACKAGE_PREFIX + HBaseConfig.PRODUCT_KEY.toLowerCase();


    public RequestBuilder getInstallDialogCommand()
    {

        return new RequestBuilder( "apt-get --assume-yes --force-yes install dialog" ).withTimeout( 360 )
                                                                                      .withStdOutRedirection(
                                                                                              OutputRedirection.NO );
    }


    public static RequestBuilder getInstallCommand()
    {

        return new RequestBuilder( "apt-get --assume-yes --force-yes install " + PACKAGE_NAME ).withTimeout( 360 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {

        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 360 )
                                                                                             .withStdOutRedirection(
                                                                                                     OutputRedirection.NO );
    }


    public static RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "service hbase start &" );
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
        return new RequestBuilder( String.format( ". /etc/profile && backUpMasters.sh %s", backUpMasters ) );
    }


    public static RequestBuilder getConfigQuorumCommand( String quorumPeers )
    {
        return new RequestBuilder( String.format( ". /etc/profile && quorum.sh %s", quorumPeers ) );
    }


    public static RequestBuilder getConfigRegionCommand( String regionServers )
    {
        return new RequestBuilder( String.format( ". /etc/profile && region.sh %s", regionServers ) );
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


    public static RequestBuilder getStartRegionServer(){
        return new RequestBuilder( ". /etc/profile && hbase-daemon.sh start regionserver " );
    }

    public static RequestBuilder getStopRegionServer(){
        return new RequestBuilder( ". /etc/profile && hbase-daemon.sh stop regionserver " );
    }
}
