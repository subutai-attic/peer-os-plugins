package org.safehaus.subutai.plugin.etl.ui.transform;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.api.ETLConfig;
import org.safehaus.subutai.plugin.etl.ui.SqoopComponent;
import org.safehaus.subutai.plugin.etl.ui.manager.ExportPanel;
import org.safehaus.subutai.plugin.etl.ui.manager.ImportPanel;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;

import com.vaadin.ui.Component;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;


public class ETLTransformManager
{
    protected static final String AVAILABLE_OPERATIONS_COLUMN_CAPTION = "AVAILABLE_OPERATIONS";
    protected static final String REFRESH_CLUSTERS_CAPTION = "Refresh Clusters";
    protected static final String DESTROY_ALL_INSTALLATIONS_BUTTON_CAPTION = "Destroy All Installations";
    protected static final String ADD_NODE_BUTTON_CAPTION = "Add Node";
    protected static final String HOST_COLUMN_CAPTION = "Host";
    protected static final String IP_COLUMN_CAPTION = "IP List";
    protected static final String BUTTON_STYLE_NAME = "default";

    private final GridLayout contentRoot;
    private final ImportPanel importPanel;
    private final ExportPanel exportPanel;
    private final ETL sqoop;
    private final ExecutorService executorService;
    private final Tracker tracker;
    private final EnvironmentManager environmentManager;
    private final SqoopComponent sqoopComponent;

    private ETLConfig config;
    private Environment environment;
    private Hadoop hadoop;


    public ETLTransformManager( ExecutorService executorService, ETL sqoop, Hadoop hadoop, Tracker tracker,
                                EnvironmentManager environmentManager, SqoopComponent sqoopComponent )
            throws NamingException
    {

        this.executorService = executorService;
        this.sqoopComponent = sqoopComponent;
        this.sqoop = sqoop;
        this.hadoop = hadoop;
        this.tracker = tracker;
        this.environmentManager = environmentManager;


        contentRoot = new GridLayout();
        contentRoot.setSpacing( true );
        contentRoot.setMargin( true );
        contentRoot.setSizeFull();
        contentRoot.setRows( 10 );
        contentRoot.setColumns( 1 );

        HorizontalLayout controlsContent = new HorizontalLayout();
        controlsContent.setSpacing( true );

        Label clusterNameLabel = new Label( "Select Sqoop installation:" );
        controlsContent.addComponent( clusterNameLabel );

        contentRoot.addComponent( controlsContent, 0, 0 );
        importPanel = new ImportPanel( sqoop, executorService, tracker );
        exportPanel = new ExportPanel( sqoop, executorService, tracker );
    }

    public Component getContent()
    {
        return contentRoot;
    }
}
