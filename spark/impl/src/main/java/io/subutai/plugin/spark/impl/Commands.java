package io.subutai.plugin.spark.impl;


import java.util.Set;

import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME = "subutai-spark2";


    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 2000 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
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


    //    public RequestBuilder getStartAllCommand()
    //    {
    //        return new RequestBuilder( "service spark-all start" ).daemon();
    //    }


    //    public RequestBuilder getStopAllCommand()
    //    {
    //        return new RequestBuilder( "service spark-all stop" ).daemon();
    //    }


    //    public RequestBuilder getStatusAllCommand()
    //    {
    //        return new RequestBuilder( "service spark-all status" ).daemon();
    //    }


    //    public RequestBuilder getStartMasterCommand()
    //    {
    //        return new RequestBuilder( "service spark-master start" ).daemon();
    //    }


    //    public RequestBuilder getRestartMasterCommand()
    //    {
    //        return new RequestBuilder( "service spark-master stop && service spark-master start" ).daemon();
    //    }


    //    public RequestBuilder getObtainMasterPidCommand()
    //    {
    //        return new RequestBuilder( "service spark-master status" );
    //    }


    //    public RequestBuilder getObtainSlavePidCommand()
    //    {
    //        return new RequestBuilder( "service spark-slave status" );
    //    }


    //    public RequestBuilder getStopMasterCommand()
    //    {
    //        return new RequestBuilder( "service spark-master stop" );
    //    }


    //    public RequestBuilder getStatusMasterCommand()
    //    {
    //        return new RequestBuilder( "service spark-master status" );
    //    }


    //    public RequestBuilder getStartSlaveCommand()
    //    {
    //        return new RequestBuilder( "service spark-slave start" ).daemon();
    //    }


    //    public RequestBuilder getStatusSlaveCommand()
    //    {
    //        return new RequestBuilder( "service spark-slave status" );
    //    }


    //    public RequestBuilder getStopSlaveCommand()
    //    {
    //        return new RequestBuilder( "service spark-slave stop" );
    //    }


    public RequestBuilder getSetMasterIPCommand( String masterHostname )
    {
        return new RequestBuilder(
                String.format( "/opt/spark*/bin/sparkMasterConf.sh clear ; /opt/spark*/bin/sparkMasterConf.sh %s",
                        masterHostname ) );
    }


    public RequestBuilder getClearSlavesCommand()
    {
        return new RequestBuilder( "/opt/spark*/bin/sparkSlaveConf.sh clear" );
    }


    public RequestBuilder getClearSlaveCommand( String slaveIP )
    {
        return new RequestBuilder( String.format( "sed -i -e \"/%s/d\" /opt/spark/conf/slaves", slaveIP ) );
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
                String.format( "/opt/spark*/bin/sparkSlaveConf.sh clear ; /opt/spark*/bin/sparkSlaveConf.sh %s",
                        slaves ) ).withTimeout( 60 );
    }


    public static RequestBuilder getSetEnvVariablesCommand()
    {
        return new RequestBuilder( "source /etc/profile" );
    }


    public static RequestBuilder getSetIpCommand( String ip )
    {
        return new RequestBuilder( String.format( "bash /opt/spark/conf/spark-conf.sh ip %s", ip ) );
    }


    public static RequestBuilder getSetWorkerCoreCommand( final String worker )
    {
        return new RequestBuilder( String.format( "bash /opt/spark/conf/spark-conf.sh worker.core %s", worker ) );
    }


    public static RequestBuilder getSetSlavesCommand( final String slaveIPs )
    {
        return new RequestBuilder( String.format( "echo -e \"%s\" > /opt/spark/conf/slaves", slaveIPs ) );
    }


    public static RequestBuilder getStartAllCommand()
    {
        return new RequestBuilder( "bash /opt/spark/sbin/start-all.sh" );
    }


    public static RequestBuilder getNodeStatusCommand()
    {
        return new RequestBuilder( "source /etc/profile ; jps" );
    }


    public RequestBuilder getStartMasterCommand()
    {
        return new RequestBuilder( "bash /opt/spark/sbin/start-master.sh" );
    }


    public RequestBuilder getStartSlaveCommand( final String hostname )
    {
        return new RequestBuilder( String.format( "bash /opt/spark/sbin/start-slave.sh %s:7077", hostname ) );
    }


    public RequestBuilder getStopMasterCommand()
    {
        return new RequestBuilder( "bash /opt/spark/sbin/stop-master.sh" );
    }


    public RequestBuilder getStopSlaveCommand()
    {
        return new RequestBuilder( "bash /opt/spark/sbin/stop-slave.sh" );
    }
}
