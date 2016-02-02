package io.subutai.plugin.hbase.rest.pojo;


public class ContainerPojo
{
    private String hostname;
    private String ip;
    private String id;
    private String status;


    public ContainerPojo( final String hostname, final String id, final String ip, final String status )
    {
        this.hostname = hostname;
        this.ip = ip;
        this.status = status;
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
}
