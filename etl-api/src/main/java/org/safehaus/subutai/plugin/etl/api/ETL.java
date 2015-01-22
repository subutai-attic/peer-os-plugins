package org.safehaus.subutai.plugin.etl.api;


import java.util.UUID;

import org.safehaus.subutai.plugin.etl.api.setting.ExportSetting;
import org.safehaus.subutai.plugin.etl.api.setting.ImportSetting;


public interface ETL
{

    public UUID isInstalled( String clusterName, String hostname );

    public UUID exportData( ExportSetting settings );

    public UUID importData( ImportSetting settings );

}
