package io.subutai.plugin.hive.rest.pojo;


import java.io.Serializable;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;


public class NodePojo implements Serializable
{
    private String uuid;
    private String hostname;
    private String status;
    private String ip;


    public NodePojo()
    {
    }


    public NodePojo( final String uuid, Environment environment )
    {
        this.uuid = uuid;
        try
        {
            ContainerHost container = environment.getContainerHostById( uuid );
            this.hostname = container.getHostname();
            this.ip = container.getIpByInterfaceName( "eth0" );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    public String getIp()
    {
        return ip;
    }


    public String getHostname()
    {
        return hostname;
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

        final NodePojo nodePojo = ( NodePojo ) o;

        if ( !getUuid().equals( nodePojo.getUuid() ) )
        {
            return false;
        }
        if ( getHostname() != null ? !getHostname().equals( nodePojo.getHostname() ) : nodePojo.getHostname() != null )
        {
            return false;
        }
        if ( getStatus() != null ? !getStatus().equals( nodePojo.getStatus() ) : nodePojo.getStatus() != null )
        {
            return false;
        }
        return !( getIp() != null ? !getIp().equals( nodePojo.getIp() ) : nodePojo.getIp() != null );
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
        return "NodePojo{" +
                "uuid='" + uuid + '\'' +
                ", hostname='" + hostname + '\'' +
                ", status='" + status + '\'' +
                ", ip='" + ip + '\'' +
                '}';
    }
}
