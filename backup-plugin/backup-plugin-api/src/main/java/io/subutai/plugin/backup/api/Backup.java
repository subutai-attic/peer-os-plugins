package io.subutai.plugin.backup.api;


/**
 * Created by ermek on 2/24/16.
 */
public interface Backup
{
    boolean executeBackup( String lxcHostName ) throws BackupException;
}
