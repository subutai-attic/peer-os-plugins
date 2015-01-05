package org.safehaus.subutai.plugin.mahout.api;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.protocol.ConfigBase;
import org.safehaus.subutai.common.settings.Common;


public class MahoutClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Mahout";
    public static final String PRODUCT_NAME = "Mahout";
    private String templateName = PRODUCT_NAME;
    public static final String TEMPLATE_NAME = "hadoopmahout";
    public static final String PRODUCT_PACKAGE = ( Common.PACKAGE_PREFIX + PRODUCT_KEY ).toLowerCase();
    private String clusterName = "";
    private SetupType setupType;
    private String hadoopClusterName;
    private Set<UUID> nodes = new HashSet<>();
    private Set<UUID> hadoopNodes = new HashSet<>();
    private UUID environmentId;



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


    public Set<UUID> getNodes()
    {
        return nodes;
    }


    public void setNodes( Set<UUID> nodes )
    {
        this.nodes = nodes;
    }


    public String getTemplateName()
    {
        return templateName;
    }


    public void setTemplateName( final String templateName )
    {
        this.templateName = templateName;
    }


    @Override
    public String toString()
    {
        return "Config{" + "clusterName=" + clusterName + ", nodes=" + nodes + '}';
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
        this.clusterName = hadoopClusterName;

    }

    public Set<UUID> getHadoopNodes()
    {
        return hadoopNodes;
    }


    public void setHadoopNodes( final Set<UUID> hadoopNodes )
    {
        this.hadoopNodes = hadoopNodes;
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }

    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }
}
