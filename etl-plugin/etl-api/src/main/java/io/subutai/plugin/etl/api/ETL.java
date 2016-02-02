package io.subutai.plugin.etl.api;


import java.util.UUID;


public interface ETL
{
    public UUID isInstalled( String clusterName, String hostname );
}
