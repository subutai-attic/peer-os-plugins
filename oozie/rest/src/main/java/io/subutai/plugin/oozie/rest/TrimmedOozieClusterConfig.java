package io.subutai.plugin.oozie.rest;


import java.util.Set;


public class TrimmedOozieClusterConfig
{

    private String hadoopClusterName;
    private String serverHostname;
    private Set<String> clientHostNames;
    private String clusterName;


    public String getClusterName()
    {
        return clusterName;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public String getServerHostname()
    {
        return serverHostname;
    }


    public Set<String> getClientHostNames()
    {
        return clientHostNames;
    }
}
