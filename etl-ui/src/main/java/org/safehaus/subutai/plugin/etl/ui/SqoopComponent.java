package org.safehaus.subutai.plugin.etl.ui;


import java.util.concurrent.ExecutorService;

import javax.naming.NamingException;

import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.etl.api.ETL;
import org.safehaus.subutai.plugin.etl.ui.extract.ETLExtractManager;
import org.safehaus.subutai.plugin.etl.ui.load.ETLLoadManager;
import org.safehaus.subutai.plugin.etl.ui.manager.ExportPanel;
import org.safehaus.subutai.plugin.etl.ui.manager.ImportExportBase;
import org.safehaus.subutai.plugin.etl.ui.manager.ImportPanel;
import org.safehaus.subutai.plugin.etl.ui.transform.ETLTransformManager;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;

import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;


public class SqoopComponent extends CustomComponent
{

    private final ETLExtractManager etlManager;
    private final ETLLoadManager etlLoadManager;
    private final ETLTransformManager etlTransformManager;

    private final TabSheet sheet;


    public SqoopComponent( ExecutorService executorService, ETL sqoop, Hadoop hadoop, Tracker tracker, EnvironmentManager environmentManager ) throws NamingException
    {
        etlManager = new ETLExtractManager( executorService, sqoop, hadoop, tracker, environmentManager, this );
        etlLoadManager = new ETLLoadManager( executorService, sqoop, hadoop, tracker, environmentManager, this );
        etlTransformManager = new ETLTransformManager( executorService, sqoop, hadoop, tracker, environmentManager, this );


        setSizeFull();
        VerticalLayout verticalLayout = new VerticalLayout();
        verticalLayout.setSpacing( true );
        verticalLayout.setSizeFull();

        sheet = new TabSheet();
        sheet.setSizeFull();
        sheet.addTab( etlManager.getContent(), "Extract" );
        sheet.getTab( 0 ).setId( "etlExtractManagerTab" );

        sheet.addTab( etlTransformManager.getContent(), "Transform" );
        sheet.getTab( 1 ).setId( "etlTransformManagerTab" );

        sheet.addTab( etlLoadManager.getContent(), "Load" );
        sheet.getTab( 2 ).setId( "etlLoadManagerTab" );

        sheet.addSelectedTabChangeListener( new TabSheet.SelectedTabChangeListener()
        {
            @Override
            public void selectedTabChange( TabSheet.SelectedTabChangeEvent event )
            {
                TabSheet tabsheet = event.getTabSheet();
                String caption = tabsheet.getTab( event.getTabSheet().getSelectedTab() ).getCaption();
                if ( caption.equals( "ETL" ) )
                {
                }
            }
        } );

        verticalLayout.addComponent( sheet );
        setCompositionRoot( verticalLayout );
    }


    public void addTab( ImportExportBase component )
    {
        TabSheet.Tab tab = sheet.addTab( component );
        if ( component instanceof ExportPanel )
        {
            tab.setCaption( "Export" );
        }
        else if ( component instanceof ImportPanel )
        {
            tab.setCaption( "Import" );
        }
        sheet.setSelectedTab( component );
    }
}
