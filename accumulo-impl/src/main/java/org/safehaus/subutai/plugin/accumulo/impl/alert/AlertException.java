package org.safehaus.subutai.plugin.accumulo.impl.alert;


public class AlertException extends Exception
{
    public AlertException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}