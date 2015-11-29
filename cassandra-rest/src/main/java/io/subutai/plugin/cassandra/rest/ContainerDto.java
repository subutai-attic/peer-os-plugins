package io.subutai.plugin.cassandra.rest;


public class ContainerDto
{
    private String ip;
    private String status;


    public ContainerDto()
    {
        ip = "";
        status = "";
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
}
