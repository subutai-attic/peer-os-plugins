package org.safehaus.subutai.plugin.oozie.api;


import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.common.api.ConfigBase;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


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
    private UUID uuid;
    private UUID server;
    private Set<UUID> clients;
    private String clusterName = "";
    private SetupType setupType;
    private UUID environmentId;
    private Set<UUID> nodes = new HashSet();


    public OozieClusterConfig()
    {
    }


    public String getTemplateNameServer()
    {
        return templateNameServer;
    }


    public void setTemplateNameServer( final String templateNameServer )
    {
        this.templateNameServer = templateNameServer;
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


    public UUID getUuid()
    {
        return uuid;
    }


    public void setUuid( UUID uuid )
    {
        this.uuid = uuid;
    }


    public void reset()
    {
        this.server = null;
        this.clients = null;
        this.domainName = "";
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( String domainName )
    {
        this.domainName = domainName;
    }


    public UUID getServer()
    {
        return server;
    }


    public void setServer( UUID server )
    {
        this.server = server;
    }


    public Set<UUID> getClients()
    {
        return clients;
    }


    public void setClients( Set<UUID> clients )
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


    /*public Set<String> getHadoopNodes() {
        return hadoopNodes;
    }


    public void setHadoopNodes( Set<String> hadoopNodes ) {
        this.hadoopNodes = hadoopNodes;
    }*/


    @Override
    public String toString()
    {
        return "OozieConfig{" +
                "domainName='" + domainName + '\'' +
                ", uuid=" + uuid +
                ", server='" + server + '\'' +
                ", clients=" + clients +
                //                ", hadoopNodes=" + hadoopNodes +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }


    public String getTemplateNameClient()
    {
        return templateNameClient;
    }


    public void setTemplateNameClient( final String templateNameClient )
    {
        this.templateNameClient = templateNameClient;
    }


    public Set<UUID> getAllNodes()
    {
        Set<UUID> allNodes = new HashSet<>();
        if ( clients != null)
        {
            allNodes.addAll( clients );
        }
        if ( server != null)
        {
            allNodes.add( server );
        }
        return allNodes;
    }

    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }

    public Set<UUID> getNodes()
    {
        return nodes;
    }

    public void setNodes( Set<UUID> nodes )
    {
        this.nodes = nodes;
    }


    public void removeClient( final UUID node )
    {
        clients.remove( node );
    }
}
