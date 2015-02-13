package org.safehaus.subutai.plugin.hipi.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.hipi.api.Hipi;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Hipi hipi;
    @Mock
    HipiConfig hipiConfig;

    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setHipiManager( hipi );
    }

    @Test
    public void testGetPrestoManager() throws Exception
    {
        listClustersCommand.getHipiManager();

        // assertions
        assertNotNull(listClustersCommand.getHipiManager());
        assertEquals( hipi,listClustersCommand.getHipiManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add(hipiConfig);
        when(hipi.getClusters()).thenReturn(myList);
        when(hipiConfig.getClusterName()).thenReturn("testPresto");

        listClustersCommand.doExecute();

        // assertions
        assertNotNull( hipi.getClusters() );
    }

    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        when(hipi.getClusters()).thenReturn(myList);

        listClustersCommand.doExecute();

        // assertions
        assertTrue( hipi.getClusters().isEmpty() );
    }
}