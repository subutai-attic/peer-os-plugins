package org.safehaus.subutai.plugin.common.api;


import org.safehaus.subutai.common.exception.SubutaiException;


public class ClusterConfigurationException extends SubutaiException
{

    public ClusterConfigurationException( final String message )
    {
        super( message );
    }


    public ClusterConfigurationException( final Throwable cause )
    {
        super( cause );
    }
}
