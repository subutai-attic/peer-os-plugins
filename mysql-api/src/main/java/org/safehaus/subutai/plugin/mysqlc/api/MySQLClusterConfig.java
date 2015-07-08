package org.safehaus.subutai.plugin.mysqlc.api;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ConfigBase;

import com.google.common.collect.Sets;


/**
 * Created by tkila on 5/7/15.
 */

public class MySQLClusterConfig implements ConfigBase
{

    //@formatter:off
    public static final String PRODUCT_KEY                        ="MySQLCluster";
    public static final String PRODUCT_NAME                       ="MySQLCluster";

    public static final String TEMPLATE_NAME                      ="mysqlcluster";
    public static final String PACKAGE_NAME                       =( Common.PACKAGE_PREFIX + PRODUCT_NAME )
            .toLowerCase();
    private             String domainName                         =  Common.DEFAULT_DOMAIN_NAME;
    private             String clusterName                        = "";

    private             String dataNodeDataDir                    ="/usr/local/mysql/data";
    private             String confNodeDataFile                   ="/usr/local/mysql/my.cnf";
    private             String dataManNodeDir                     ="/usr/local/mysql/mysql-cluster";
    private             String confManNodeFile                    ="/usr/local/mysql/mysql-cluster/config.ini";
    private             boolean isAutoScaling                     =true;
    private             UUID                   environmentId;
    private             Set<UUID>              dataNodes;
    private             Set<UUID>              managerNodes;
    private             MySQLManagerNodeConfig managerNodeConfig;
    private             MySQLDataNodeConfig    dataNodeConfig;

    private Map<String,Boolean> isSqlInstalled = new HashMap<>();
    private Map<String,Boolean> requiresReloadConf = new HashMap<>();
    private Map<String,Boolean> isInitialStart = new HashMap<>();

    //@formatter:on


    public boolean isAutoScaling()
    {
        return isAutoScaling;
    }


    public void setAutoScaling( final boolean isAutoScaling )
    {
        this.isAutoScaling = isAutoScaling;
    }



    public String getDataNodeDataDir()
    {
        return dataNodeDataDir;
    }


    public void setDataNodeDataDir( final String dataNodeDataDir )
    {
        this.dataNodeDataDir = dataNodeDataDir;
    }


    public String getConfNodeDataFile()
    {
        return confNodeDataFile;
    }


    public void setConfNodeDataFile( final String confNodeDataFile )
    {
        this.confNodeDataFile = confNodeDataFile;
    }


    public String getDataManNodeDir()
    {
        return dataManNodeDir;
    }


    public void setDataManNodeDir( final String dataManNodeDir )
    {
        this.dataManNodeDir = dataManNodeDir;
    }


    public String getConfManNodeFile()
    {
        return confManNodeFile;
    }


    public void setConfManNodeFile( final String confManNodeFile )
    {
        this.confManNodeFile = confManNodeFile;
    }


    public Map<String, Boolean> getIsInitialStart()
    {
        return isInitialStart;
    }


    public void setIsInitialStart( final Map<String, Boolean> isInitialStart )
    {
        this.isInitialStart = isInitialStart;
    }


    public Map<String, Boolean> getRequiresReloadConf()
    {
        return requiresReloadConf;
    }


    public void setRequiresReloadConf( final Map<String, Boolean> requiresReloadConf )
    {
        this.requiresReloadConf = requiresReloadConf;
    }


    public Map<String, Boolean> getIsSqlInstalled()
    {
        return isSqlInstalled;
    }


    public void setIsSqlInstalled( final Map<String, Boolean> isSqlInstalled )
    {
        this.isSqlInstalled = isSqlInstalled;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }


    public Set<UUID> getDataNodes()
    {
        return dataNodes;
    }


    public Set<UUID> getManagerNodes()
    {
        return managerNodes;
    }


    public MySQLManagerNodeConfig getManagerNodeConfig()
    {
        return managerNodeConfig;
    }


    public void setManagerNodeConfig( final MySQLManagerNodeConfig managerNodeConfig )
    {
        this.managerNodeConfig = managerNodeConfig;
    }


    public MySQLDataNodeConfig getDataNodeConfig()
    {
        return dataNodeConfig;
    }


    public void setDataNodeConfig( final MySQLDataNodeConfig dataNodeConfig )
    {
        this.dataNodeConfig = dataNodeConfig;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    @Override
    public String getClusterName()
    {
        return clusterName;
    }


    @Override
    public String getProductName()
    {
        return PRODUCT_NAME;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }


    public static String getPackageName()
    {
        return PACKAGE_NAME;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDataNodes( final Set<UUID> dataNodes )
    {
        this.dataNodes = dataNodes;
    }


    public void setManagerNodes( final Set<UUID> managerNodes )
    {
        this.managerNodes = managerNodes;
    }


    public Set<UUID> getAllNodes()
    {
        Set<UUID> allNodes = Sets.newHashSet();
        if ( !CollectionUtil.isCollectionEmpty( managerNodes ) )
        {
            allNodes.addAll( managerNodes );
        }
        if ( !CollectionUtil.isCollectionEmpty( dataNodes ) )
        {
            allNodes.addAll( dataNodes );
        }

        return allNodes;
    }

}
