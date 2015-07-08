package io.subutai.plugin.mysql.impl.common;


/**
 * Created by tkila on 5/13/15.
 */
public class Commands
{

    //@formatter:off
    public static String sqlDir              ="/usr/local/mysql/";
    public static String sqlScriptDir        =sqlDir+"support-files/";

    public static String stopAllCommand      ="ndb_mgm -e shutdown";
    public static String createDir           ="mkdir -p ";
    public static String confFile            =sqlDir+"mysql-cluster/config.ini";
    public static String chmod644            ="chmod 644 ";
    public static String recreateConfFile    ="rm %s && touch %s";
    public static String writeToConfFile  =sqlScriptDir+"editconf.sh ";
    public static String startManagementNode ="ndb_mgmd -f "+confFile;
    public static String startInitManageNode ="ndb_mgmd --initial --config-file="+confFile;
    public static String getPidCommand       ="ps aux | grep 'ndbd ' | awk  -F ' ' '{print $2}' ";
    public static String startCommand        ="/usr/local/mysql/bin/ndbd --defaults-file=%s";
    public static String stopNodeCommand     ="/usr/bin/pkill -15 ndbd";
    public static String statusDataNode      ="ps aux | grep \"ndbd\"";
    public static String statusManagerNode   ="ps aux | grep \"ndb_mgmd\"";
    public static String stopManagerNode     ="pkill -f ndb_mgmd";
    public static String ndbdInit            ="/usr/local/mysql/bin/ndbd --initial";
    public static String initMySQLServer     =sqlDir+"scripts/mysql_install_db --user=mysql --datadir=%s"
            + " --defaults-file=%s";

    public static String startMySQLServer    =sqlScriptDir+"mysql.server start";
    public static String statusMySQLServer   =sqlScriptDir+"mysql.server status";
    public static String stopMySQLServer     =sqlScriptDir+"mysql.server stop";

    //@formatter:on
}
