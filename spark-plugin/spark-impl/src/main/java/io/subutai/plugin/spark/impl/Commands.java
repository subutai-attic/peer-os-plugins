package io.subutai.plugin.spark.impl;


import java.util.Set;

import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + SparkClusterConfig.PRODUCT_KEY.toLowerCase();


    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 600000 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 600 );
    }


    public RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }


    public RequestBuilder getStartAllCommand()
    {
        return new RequestBuilder( "service spark-all start" ).daemon();
    }


    public RequestBuilder getStopAllCommand()
    {
        return new RequestBuilder( "service spark-all stop" ).daemon();
    }


    public RequestBuilder getStatusAllCommand()
    {
        return new RequestBuilder( "service spark-all status" ).daemon();
    }


    public RequestBuilder getStartMasterCommand()
    {
        return new RequestBuilder( "service spark-master start" ).daemon();
    }


    public RequestBuilder getRestartMasterCommand()
    {
        return new RequestBuilder( "service spark-master stop && service spark-master start" ).daemon();
    }


    public RequestBuilder getObtainMasterPidCommand()
    {
        return new RequestBuilder( "service spark-master status" );
    }


    public RequestBuilder getObtainSlavePidCommand()
    {
        return new RequestBuilder( "service spark-slave status" );
    }


    public RequestBuilder getStopMasterCommand()
    {
        return new RequestBuilder( "service spark-master stop" );
    }


    public RequestBuilder getStatusMasterCommand()
    {
        return new RequestBuilder( "service spark-master status" );
    }


    public RequestBuilder getStartSlaveCommand()
    {
        return new RequestBuilder( "service spark-slave start" ).daemon();
    }


    public RequestBuilder getStatusSlaveCommand()
    {
        return new RequestBuilder( "service spark-slave status" );
    }


    public RequestBuilder getStopSlaveCommand()
    {
        return new RequestBuilder( "service spark-slave stop" );
    }


    public RequestBuilder getSetMasterIPCommand( String masterHostname )
    {
        return new RequestBuilder(
                String.format( "/opt/spark*/bin/sparkMasterConf.sh clear ; /opt/spark*/bin/sparkMasterConf.sh %s", masterHostname ) );
    }


    public RequestBuilder getClearSlavesCommand()
    {
        return new RequestBuilder( "/opt/spark*/bin/sparkSlaveConf.sh clear" );
    }


    public RequestBuilder getClearSlaveCommand( String slaveHostname )
    {
        return new RequestBuilder( String.format( "/opt/spark*/bin/sparkSlaveConf.sh clear %s", slaveHostname ) )
                .withTimeout( 60 );
    }


    public RequestBuilder getAddSlaveCommand( String slaveHostname )
    {
        return new RequestBuilder( String.format( "/opt/spark*/bin/sparkSlaveConf.sh %s", slaveHostname ) )
                .withTimeout( 60 );
    }


    public RequestBuilder getAddSlavesCommand( Set<String> slaveNodeHostnames )
    {
        StringBuilder slaves = new StringBuilder();
        for ( String slaveNode : slaveNodeHostnames )
        {
            slaves.append( slaveNode ).append( " " );
        }

        return new RequestBuilder(
                String.format( "/opt/spark*/bin/sparkSlaveConf.sh clear ; /opt/spark*/bin/sparkSlaveConf.sh %s", slaves ) )
                .withTimeout( 60 );
    }
}
