package io.subutai.plugin.backup.api;


/**
 * Created by ermek on 2/25/16.
 */
public class BackupException extends Exception
{
    private String description = "";


    public BackupException( final Throwable cause )
    {
        super( cause );
    }


    public BackupException( final String message )
    {
        super( message );
    }


    public BackupException( final String message, String description )
    {
        super( message );
        this.description = description;
    }


    public BackupException( final String message, final Throwable cause )
    {
        super( message, cause );
    }


    public String toString()
    {
        return super.toString() + " (" + this.description + ")";
    }

}
