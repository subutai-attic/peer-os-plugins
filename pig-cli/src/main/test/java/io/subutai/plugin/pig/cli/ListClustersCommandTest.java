package io.subutai.plugin.pig.cli;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.plugin.pig.api.Pig;
import io.subutai.plugin.pig.api.PigConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ListClustersCommandTest
{
    private ListClustersCommand listClustersCommand;
    @Mock
    Pig pig;
    @Mock
    PigConfig pigConfig;

    @Before
    public void setUp() throws Exception
    {
        listClustersCommand = new ListClustersCommand();
        listClustersCommand.setPigManager(pig);
    }

    @Test
    public void testGetPigManager() throws Exception
    {
        listClustersCommand.getPigManager();

        // assertions
        assertEquals(pig,listClustersCommand.getPigManager());
        assertNotNull(listClustersCommand.getPigManager());
    }

    @Test
    public void testDoExecute() throws Exception
    {
        List<PigConfig> myList = new ArrayList<>();
        myList.add(pigConfig);
        when(pig.getClusters()).thenReturn(myList);

        listClustersCommand.doExecute();

        // assertions
        assertNotNull(pig.getClusters());
    }

    @Test
    public void testDoExecuteNoFlumeClusters()
    {
        List<PigConfig> myList = new ArrayList<>();
        when(pig.getClusters()).thenReturn(myList);

        listClustersCommand.doExecute();

    }
}