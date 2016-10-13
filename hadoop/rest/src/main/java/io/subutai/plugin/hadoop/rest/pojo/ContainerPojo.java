package io.subutai.plugin.hadoop.rest.pojo;


import java.io.Serializable;


public class ContainerPojo implements Serializable
{
    private String uuid;
    private String hostname;
    private String status;
    private String ip;


    public ContainerPojo( final String uuid, final String hostname, final String ip, final String status )
    {
        this.uuid = uuid;
        this.hostname = hostname;
        this.ip = ip;
        this.status = status;
    }


    public String getIp()
    {
        return ip;
    }


    public void setIp( final String ip )
    {
        this.ip = ip;
    }


    public String getHostname()
    {
        return hostname;
    }


    public void setHostname( final String hostname )
    {
        this.hostname = hostname;
    }


    public String getStatus()
    {
        return status;
    }


    public void setStatus( final String status )
    {
        this.status = status;
    }


    public String getUuid()
    {
        return uuid;
    }


    public void setUuid( final String uuid )
    {
        this.uuid = uuid;
    }


    @Override
    public boolean equals( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final ContainerPojo that = ( ContainerPojo ) o;

        if ( !getUuid().equals( that.getUuid() ) )
        {
            return false;
        }
        if ( getHostname() != null ? !getHostname().equals( that.getHostname() ) : that.getHostname() != null )
        {
            return false;
        }
        if ( getStatus() != null ? !getStatus().equals( that.getStatus() ) : that.getStatus() != null )
        {
            return false;
        }
        return !( getIp() != null ? !getIp().equals( that.getIp() ) : that.getIp() != null );
    }


    @Override
    public int hashCode()
    {
        int result = getUuid().hashCode();
        result = 31 * result + ( getHostname() != null ? getHostname().hashCode() : 0 );
        result = 31 * result + ( getStatus() != null ? getStatus().hashCode() : 0 );
        result = 31 * result + ( getIp() != null ? getIp().hashCode() : 0 );
        return result;
    }


    @Override
    public String toString()
    {
        return "ContainerPojo{" +
                "uuid='" + uuid + '\'' +
                ", hostname='" + hostname + '\'' +
                ", status='" + status + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}
