package io.subutai.plugin.hipi.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.plugin.hipi.api.Hipi;
import io.subutai.plugin.hipi.api.HipiConfig;
import io.subutai.plugin.hipi.cli.ListClustersCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ListClustersCommandTest
{
    @Mock Hipi hipi;
    @Mock HipiConfig hipiConfig;
    private ListClustersCommand listClustersCommand;


    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setHipiManager( hipi );
    }


    @Test
    public void testGetHipiManager() throws Exception
    {
        listClustersCommand.getHipiManager();

        // assertions
        assertNotNull( listClustersCommand.getHipiManager() );
        assertEquals( hipi, listClustersCommand.getHipiManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        myList.add( hipiConfig );
        when( hipi.getClusters() ).thenReturn( myList );
        when( hipiConfig.getClusterName() ).thenReturn( "testPresto" );

        listClustersCommand.doExecute();

        // assertions
        assertNotNull( hipi.getClusters() );
    }


    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<HipiConfig> myList = new ArrayList<>();
        when( hipi.getClusters() ).thenReturn( myList );

        listClustersCommand.doExecute();

        // assertions
        assertTrue( hipi.getClusters().isEmpty() );
    }
}