package io.subutai.plugin.flume.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.flume.api.FlumeConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + "flume2";


    public static String make( CommandType type )
    {
        switch ( type )
        {
            case STATUS:
                return "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
            case INSTALL:
            case PURGE:
                return "apt-get --force-yes --assume-yes " + type.toString().toLowerCase() + " " + PACKAGE_NAME;
            case START:
                return "start-stop-daemon --start --background  --exec /opt/flume/bin/flume-ng -- agent -n "
                        + "myagentname -c /opt/flume/conf/ -f /opt/flume/conf/flume.properties";
            case STOP:
                return "kill `jps | grep \"Application\" | cut -d \" \" -f 1` ; sleep 5";
            case SERVICE_STATUS:
                return "ps axu | grep [f]lume";
            default:
                throw new AssertionError( type.name() );
        }
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
    }


    public static RequestBuilder getConfigurePropertiesCommand( final String ip )
    {
        return new RequestBuilder(
                String.format( "sed -i -e \"s/hdfs:\\/\\/localhost/hdfs:\\/\\/%s/g\" /opt/flume/conf/flume.properties",
                        ip ) );
    }


    public static RequestBuilder getCreatePropertiesCommand()
    {
        return new RequestBuilder( "cp /opt/flume/conf/hdfs.properties /opt/flume/conf/flume.properties" );
    }
}
