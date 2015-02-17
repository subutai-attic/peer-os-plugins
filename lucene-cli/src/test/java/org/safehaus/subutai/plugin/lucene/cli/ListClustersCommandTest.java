package org.safehaus.subutai.plugin.lucene.cli;


import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.lucene.api.Lucene;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Lucene lucene;
    @Mock
    LuceneConfig luceneConfig;


    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setLuceneManager( lucene );
    }


    @Test
    public void testGetPrestoManager() throws Exception
    {
        listClustersCommand.getLuceneManager();

        // assertions
        assertNotNull( listClustersCommand.getLuceneManager() );
        assertEquals( lucene, listClustersCommand.getLuceneManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        myList.add( luceneConfig );
        when( lucene.getClusters() ).thenReturn( myList );
        when( luceneConfig.getClusterName() ).thenReturn( "testPresto" );

        listClustersCommand.doExecute();

        // assertions
        assertNotNull( lucene.getClusters() );
    }


    @Test
    public void testDoExecuteNoConfigs() throws Exception
    {
        List<LuceneConfig> myList = new ArrayList<>();
        when( lucene.getClusters() ).thenReturn( myList );

        listClustersCommand.doExecute();

        // assertions
        assertTrue( lucene.getClusters().isEmpty() );
    }
}