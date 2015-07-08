package io.subutai.plugin.etl.impl;


import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import io.subutai.plugin.common.api.NodeOperationType;
import io.subutai.plugin.etl.impl.CommandFactory;
import io.subutai.plugin.sqoop.api.DataSourceType;
import io.subutai.plugin.sqoop.api.setting.CommonSetting;
import io.subutai.plugin.sqoop.api.setting.ExportSetting;
import io.subutai.plugin.sqoop.api.setting.ImportSetting;


public class CommandFactoryTest
{

    @Test
    public void testBuild()
    {
        // all needed operation types
        List<NodeOperationType> types = new ArrayList<>();
        types.add( NodeOperationType.STATUS );
        types.add( NodeOperationType.INSTALL );
        types.add( NodeOperationType.UNINSTALL );
        types.add( NodeOperationType.EXPORT );
        types.add( NodeOperationType.IMPORT );

        for ( NodeOperationType type : types )
        {
            CommonSetting settings = null;
            if ( type == NodeOperationType.EXPORT )
            {
                settings = new ExportSetting();
            }
            else if ( type == NodeOperationType.IMPORT )
            {
                settings = new ImportSetting();
                ( ( ImportSetting ) settings ).setType( DataSourceType.HDFS );
            }
            String cmd = CommandFactory.build( type, settings );
            Assert.assertNotNull( "Command should not be null for " + type, cmd );
        }
    }

}

