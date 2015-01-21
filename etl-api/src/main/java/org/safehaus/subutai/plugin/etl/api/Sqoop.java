package org.safehaus.subutai.plugin.etl.api;


import java.util.UUID;

import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.etl.api.setting.ExportSetting;
import org.safehaus.subutai.plugin.etl.api.setting.ImportSetting;


public interface Sqoop extends ApiBase<SqoopConfig>
{

    public UUID isInstalled( String clusterName, String hostname );

    public UUID installCluster( SqoopConfig config, HadoopClusterConfig hadoopConfig );

    public UUID destroyNode( String clusterName, String hostname );

    public UUID exportData( ExportSetting settings );

    public UUID importData( ImportSetting settings );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment env, SqoopConfig config, TrackerOperation po );
}
