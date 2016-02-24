package io.subutai.plugin.backup.rest;


import io.subutai.plugin.backup.api.Backup;


/**
 * Created by ermek on 2/24/16.
 */
public class RestServiceImpl /*implements Backup*/
{
    private Backup backup;


    public RestServiceImpl( Backup backup )
    {
        this.backup = backup;
    }

    public void setBackup( final Backup backup )
    {
        this.backup = backup;
    }
}
