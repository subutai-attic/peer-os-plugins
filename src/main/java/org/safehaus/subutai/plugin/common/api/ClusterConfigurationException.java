package org.safehaus.subutai.plugin.common.api;


public class ClusterConfigurationException extends Exception
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
