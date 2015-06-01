package org.safehaus.subutai.plugin.mysqlc.api;


import java.util.List;
import java.util.Set;
import java.util.UUID;


/**
 * Created by tkila on 5/11/15.
 */
public class MySQLManagerNodeConfig
{

    //@formatter:off

    //Options affecting ndbd process on all data nodes
    private static final String DEFAULT_OP                 = "[ndbd==default]";

    //Amount of memory to allocate for data storage
    private static final String DEFAULT_DATA_MEMORY        = "80M";
    //Amount of memory to allocate for index storage
    private static final String DEFAULT_INDEX_MEMORY       = "18M";

    //TCP/IP Process Options
    private static final String TCP_IP_OPTIONS             = "[tcp==default]";
    private static final int    DEFAULT_PORT_NUMBER        = 2202;

    //Management Process Options
    private static final String DEFAULT_MGM_PROC_OP        = "[ndb_mgmd]";
    //IP Address or Host name of manager node
    private              List<String> managerNodeHost         ;
    public               String dataDir                    = "/usr/local/mysql/mysql-cluster";
    //Data nodes
    private static final String       DATA_NODE_OP         = "[ndbd]";
    private              Set<UUID>    dataNodes                        ;
    private              List<String> dataNodesIPAddresses              ;
    //MySQL Server configurations
    private static final String MYSQL_SERVER_OP            = "[mysqld]";
    private              String mySqlServerHostName        = "";
    private              String dataNodeDataDir                ;
    //@formatter:on


    public String getDataDir()
    {
        return dataDir;
    }


    public void setDataDir( final String dataDir )
    {
        this.dataDir = dataDir;
    }


    public String getDataNodeDataDir()
    {
        return dataNodeDataDir;
    }


    public void setDataNodeDataDir( final String dataNodeDataDir )
    {
        this.dataNodeDataDir = dataNodeDataDir;
    }


    public void setDataNodes( final Set<UUID> dataNodes )
    {
        this.dataNodes = dataNodes;
    }


    public List<String> getManagerNodeHost()
    {
        return managerNodeHost;
    }


    public void setManagerNodeHost( final List<String> managerNodeHost )
    {
        this.managerNodeHost = managerNodeHost;
    }


    public String getMySqlServerHostName()
    {
        return mySqlServerHostName;
    }


    public void setMySqlServerHostName( final String mySqlServerHostName )
    {
        this.mySqlServerHostName = mySqlServerHostName;
    }


    public static String getDefaultOp()
    {
        return DEFAULT_OP;
    }


    public String getDataNodeOp()
    {
        return DATA_NODE_OP;
    }


    public String getMysqlServerHostname()
    {
        return mySqlServerHostName;
    }


    public static String getMysqlServerOp()
    {
        return MYSQL_SERVER_OP;
    }


    public static String getDefaultDataMemory()
    {
        return DEFAULT_DATA_MEMORY;
    }


    public static String getDefaultIndexMemory()
    {
        return DEFAULT_INDEX_MEMORY;
    }


    public static String getTcpIpOptions()
    {
        return TCP_IP_OPTIONS;
    }


    public static int getDefaultPortNumber()
    {
        return DEFAULT_PORT_NUMBER;
    }


    public static String getDefaultMgmProcOp()
    {
        return DEFAULT_MGM_PROC_OP;
    }


    public Set<UUID> getDataNodes()
    {
        return dataNodes;
    }


    //configuration for config.ini file
    public String exportConfing()
    {
        StringBuilder sb = new StringBuilder();
        int nodeId = 1;
        sb.append( DEFAULT_OP + "\n" );
        sb.append( "NoOfReplicas=1" + "\n" );
        sb.append( "DataMemory=" + DEFAULT_DATA_MEMORY + "\n" );
        sb.append( "IndexMemory=" + DEFAULT_INDEX_MEMORY + "\n" );
        sb.append( TCP_IP_OPTIONS + "\n" );
        sb.append( "portnumber=" + DEFAULT_PORT_NUMBER + "\n" );
        for ( String ip : managerNodeHost )
        {
            sb.append( DEFAULT_MGM_PROC_OP + "\n" );
            sb.append( "NodeId=" + nodeId + "\n" );
            sb.append( "HostName=" + ip + "\n" );

            nodeId++;
        }
        sb.append( "DataDir=" + getDataDir() + "\n" );
        for ( String node : dataNodesIPAddresses )
        {
            sb.append( DATA_NODE_OP + "\n" );
            sb.append( "HostName=" + node + "\n" );
            sb.append( "DataDir=" + getDataNodeDataDir() + "\n" );
        }
        for ( String node : dataNodesIPAddresses )
        {
            sb.append( MYSQL_SERVER_OP + "\n" );
            sb.append( "hostname=" + node + "\n" );
        }

        return sb.toString();
    }


    public void setDataNodesIPAddresses( final List<String> dataNodesIPAddresses )
    {
        this.dataNodesIPAddresses = dataNodesIPAddresses;
    }


    public List<String> getDataNodesIPAddresses()
    {
        return dataNodesIPAddresses;
    }
}
