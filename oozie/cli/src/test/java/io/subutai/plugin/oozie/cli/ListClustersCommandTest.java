package io.subutai.plugin.oozie.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.cli.ListClustersCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Oozie oozie;
    @Mock
    OozieClusterConfig oozieClusterConfig;

    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setOozieManager( oozie );
    }

    @Test
    public void testGetPrestoManager() throws Exception
    {
        listClustersCommand.getOozieManager();
        
        // assertions
        assertNotNull(listClustersCommand.getOozieManager());
        assertEquals( oozie,listClustersCommand.getOozieManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        myList.add(oozieClusterConfig);
        when(oozie.getClusters()).thenReturn(myList);
        when(oozieClusterConfig.getClusterName()).thenReturn("testPresto");

        listClustersCommand.doExecute();

        // assertions
        assertNotNull( oozie.getClusters() );
    }

    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        when(oozie.getClusters()).thenReturn(myList);

        listClustersCommand.doExecute();

        // assertions
        assertTrue( oozie.getClusters().isEmpty() );
    }
}