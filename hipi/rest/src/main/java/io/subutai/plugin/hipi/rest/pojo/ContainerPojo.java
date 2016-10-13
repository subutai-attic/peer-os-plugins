package io.subutai.plugin.hipi.rest.pojo;


public class ContainerPojo
{
    private String hostname;
    private String ip;
    private String id;


    public String getId()
    {
        return id;
    }


    public void setId( final String id )
    {
        this.id = id;
    }


    public ContainerPojo()
    {
        hostname = "";
    }


    public ContainerPojo( final String hostname, final String ip, final String id )
    {
        this.hostname = hostname;
        this.ip = ip;
        this.id = id;
    }


    public String getHostname()
    {
        return hostname;
    }


    public void setHostname( final String hostname )
    {
        this.hostname = hostname;
    }


    public String getIp()
    {
        return ip;
    }


    public void setIp( final String ip )
    {
        this.ip = ip;
    }
}
