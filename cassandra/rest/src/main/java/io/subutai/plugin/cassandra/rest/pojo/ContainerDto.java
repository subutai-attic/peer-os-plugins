package io.subutai.plugin.cassandra.rest.pojo;


public class ContainerDto
{
    private String id;
    private String ip;
    private String hostname;
    private String status;


    public ContainerDto()
    {
        ip = "";
        status = "";
        id = "";
    }


    public String getIp()
    {
        return ip;
    }


    public void setIp( final String ip )
    {
        this.ip = ip;
    }


    public String getStatus()
    {
        return status;
    }


    public void setStatus( final String status )
    {
        this.status = status;
    }


    public String getId()
    {
        return id;
    }


    public void setId( final String id )
    {
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
}
