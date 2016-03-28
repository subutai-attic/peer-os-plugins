package io.subutai.plugin.oozie.rest.pojo;


/**
 * Created by ermek on 11/27/15.
 */
public class ContainerPojo
{
    private String hostname;
    private String ip;
    private String status;


    public ContainerPojo( final String hostname, final String ip )
    {
        this.hostname = hostname;
        this.ip = ip;
    }


    public ContainerPojo ( final String hostname, final String ip, final String status )
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
}
