package io.subutai.plugin.oozie.api;


import java.util.HashSet;
import java.util.Set;

import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.ConfigBase;


/**
 * @author dilshat
 */
public class OozieClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Oozie";
    public static final String TEMPLATE_NAME = "oozie";

    public static final String PRODUCT_NAME_CLIENT = "hadoopOozieClient";
    private String templateNameClient = PRODUCT_NAME_CLIENT;
    public static final String PRODUCT_NAME_SERVER = "hadoopOozieServer";
    private String templateNameServer = PRODUCT_NAME_SERVER;
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private String hadoopClusterName;
    private String id;
    private String server;
    private Set<String> clients;
    private String clusterName = "";
    private SetupType setupType;
    private String environmentId;
    private Set<String> nodes = new HashSet<>();
    private boolean autoScaling;


    public OozieClusterConfig()
    {
    }


    public SetupType getSetupType()
    {
        return setupType;
    }


    public void setSetupType( final SetupType setupType )
    {
        this.setupType = setupType;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( final String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
    }


    public String getId()
    {
        return id;
    }


    public void setId( String id )
    {
        this.id = id;
    }


    public String getServer()
    {
        return server;
    }


    public void setServer( String server )
    {
        this.server = server;
    }


    public Set<String> getClients()
    {
        return clients;
    }


    public void setClients( Set<String> clients )
    {
        this.clients = clients;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    @Override
    public String getProductName()
    {
        return PRODUCT_KEY;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    @Override
    public String toString()
    {
        return "OozieConfig{" +
                "domainName='" + domainName + '\'' +
                ", id=" + id +
                ", server='" + server + '\'' +
                ", clients=" + clients +
                //                ", hadoopNodes=" + hadoopNodes +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = new HashSet<>();
        if ( clients != null )
        {
            allNodes.addAll( clients );
        }
        if ( server != null )
        {
            allNodes.add( server );
        }
        return allNodes;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }
}
