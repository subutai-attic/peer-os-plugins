package io.subutai.plugin.mahout.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.cli.ListClustersCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Mahout mahout;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;


    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setMahoutManager( mahout );
    }


    @Test
    public void testGetMahoutManager() throws Exception
    {
        listClustersCommand.getMahoutManager();

        // assertions
        assertNotNull( listClustersCommand.getMahoutManager() );
        assertEquals( mahout, listClustersCommand.getMahoutManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<MahoutClusterConfig> myList = new ArrayList<>();
        myList.add( mahoutClusterConfig );
        when( mahout.getClusters() ).thenReturn( myList );
        when( mahoutClusterConfig.getClusterName() ).thenReturn( "test" );

        listClustersCommand.doExecute();
    }


    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<MahoutClusterConfig> myList = new ArrayList<>();
        when( mahout.getClusters() ).thenReturn( myList );

        listClustersCommand.doExecute();
    }
}