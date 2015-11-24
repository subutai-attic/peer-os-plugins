package io.subutai.plugin.hadoop.rest;


public class ContainerInfoJson
{
    private String ip;
    private String status;


    public ContainerInfoJson()
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
