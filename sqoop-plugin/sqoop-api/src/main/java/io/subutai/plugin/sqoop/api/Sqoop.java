package io.subutai.plugin.sqoop.api;


import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.sqoop.api.setting.ExportSetting;
import io.subutai.plugin.sqoop.api.setting.ImportSetting;


public interface Sqoop extends ApiBase<SqoopConfig>
{

    public UUID isInstalled( String clusterName, String hostname );

    public UUID installCluster( SqoopConfig config, HadoopClusterConfig hadoopConfig );

    public UUID destroyNode( String clusterName, String hostname );

    public UUID exportData( ExportSetting settings );

    public UUID importData( ImportSetting settings );

    public String fetchDatabases( ImportSetting setting );

    public String fetchTables( ImportSetting setting );

    public String reviewExportQuery( ExportSetting settings );

    public String reviewImportQuery( ImportSetting settings );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, SqoopConfig config, TrackerOperation po );

}
