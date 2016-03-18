package io.subutai.plugin.elasticsearch.rest.dto;


public class ContainerDto
{
    private String hostname;
    private String ip;
    private String status;
    private String id;


    public ContainerDto() {}

    public ContainerDto( final String hostname, final String ip, final String status )
    {
        this.hostname = hostname;
        this.ip = ip;
        this.status = status;
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


    public String getStatus()
    {
        return status;
    }


    public void setStatus( final String status )
    {
        this.status = status;
    }


    public void setId( final String id )
    {
        this.id = id;
    }
}
