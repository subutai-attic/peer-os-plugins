package io.subutai.plugin.hive.impl;


import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;


public class Commands
{
    public static final String PRODUCT_KEY = "hive2";

    public static final String INSTALL_COMMAND = "apt-get --force-yes --assume-yes install ";

    public static final String UPDATE_COMMAND = "apt-get --force-yes --assume-yes update";

    public static final String UNINSTALL_COMMAND = "apt-get --force-yes --assume-yes purge ";

    public static final String START_COMMAND = "source /etc/profile ; start-stop-daemon --start --background --exec /opt/hive/bin/hive -- --service hiveserver2";

    public static final String STOP_COMMAND = "kill `jps | grep \"RunJar\" | cut -d \" \" -f 1`";

    public static final String RESTART_COMMAND = "service hive-thrift restart";

    public static final String STATUS_COMMAND = "jps";

    public static final String START_NAMENODE_COMMAND = "source /etc/profile ; start-dfs.sh ; start-yarn.sh";

    public static final String START_DERBY_COMMAND = "start-stop-daemon --start --background  --exec /opt/derby/bin/startNetworkServer -- -h 0.0.0.0 -p 50000";

    public static final String STOP_DERBY_COMMAND = "kill `jps | grep \"NetworkServerControl\" | cut -d \" \" -f 1`";

    public static final String STOP_NAMENODE_COMMAND = "source /etc/profile ; stop-dfs.sh ; stop-yarn.sh";

    public static final String ENVIRONMENT_VARIABLES_COMMAND = "source /etc/profile";

    public static final String INITIALIZE_SCHEMA = "/opt/hive/bin/schematool -initSchema -dbType derby";

    public static final String CREATE_HDFS_DIRECTORIES =
            "source /etc/profile ; hdfs dfs -mkdir /tmp ; hdfs dfs -mkdir -p /user/hive/warehouse ; hdfs dfs -chmod g+w /tmp ; hdfs dfs "
                    + "-chmod g+w /user/hive/warehouse";

    public static final String copyDerbyJarsCommand =
            "cp /opt/derby/lib/derbyclient.jar /opt/hive/lib/ ; cp /opt/derby/lib/derbytools.jar /opt/hive/lib/";

    public static final String checkIfInstalled = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;


    public static String configureHiveServer( String ip )
    {
        return "bash /opt/hive/conf/hive-configure.sh " + ip;
    }


    public static String configureClient( ContainerHost server )
    {
        String uri = "thrift://" + server.getInterfaceByName( "eth0" ).getIp() + ":10000";
        return Commands.addHiveProperty( "add", "hive-site.xml", "hive.metastore.uris", uri );
    }


    public static String addHiveProperty( String cmd, String propFile, String property, String value )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "bash /opt/hive/conf/hive-property.sh " ).append( cmd ).append( " " );
        sb.append( propFile ).append( " " ).append( property );
        if ( value != null )
        {
            sb.append( " " ).append( value );
        }
        return sb.toString();
    }


    public static String addConfigHostsCoreSite()
    {
        return Commands.addHiveProperty( "add", "hadoop/etc/hadoop/core-site.xml", "hadoop.proxyuser.root.hosts", "*" );
    }


    public static String addConfigGroupsCoreSite()
    {
        return Commands
                .addHiveProperty( "add", "hadoop/etc/hadoop/core-site.xml", "hadoop.proxyuser.root.groups", "*" );
    }
}
