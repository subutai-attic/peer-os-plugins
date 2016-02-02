package io.subutai.plugin.common.api;


public class ClusterSetupException extends Exception
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
