package org.safehaus.subutai.plugin.common.api;


import org.safehaus.subutai.common.exception.SubutaiException;


public class ClusterSetupException extends SubutaiException
{

    public ClusterSetupException( final String message )
    {
        super( message );
    }


    public ClusterSetupException( final Throwable cause )
    {
        super( cause );
    }
}
