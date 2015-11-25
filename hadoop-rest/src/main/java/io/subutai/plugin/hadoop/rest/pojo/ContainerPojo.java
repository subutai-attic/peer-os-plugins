package io.subutai.plugin.hadoop.rest.pojo;


import java.io.Serializable;


public class ContainerPojo implements Serializable
{
    private String uuid;
    private String hostname;
    private String status;


    public ContainerPojo()
    {
        hostname = "";
        status = "";
        uuid = "";
    }


    public ContainerPojo( final String uuid, final String hostname, final String status )
    {
        this.uuid = uuid;
        this.hostname = hostname;
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
    public String toString()
    {
        return "ContainerPojo{" +
                "uuid='" + uuid + '\'' +
                ", hostname='" + hostname + '\'' +
                ", status='" + status + '\'' +
                '}';
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

        return ( !getUuid().equals( that.getUuid() ) );
    }


    @Override
    public int hashCode()
    {
        int result = getUuid().hashCode();
        result = 31 * result + ( getHostname() != null ? getHostname().hashCode() : 0 );
        result = 31 * result + ( getStatus() != null ? getStatus().hashCode() : 0 );
        return result;
    }
}
