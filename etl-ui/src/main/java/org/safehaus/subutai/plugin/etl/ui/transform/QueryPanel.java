package org.safehaus.subutai.plugin.etl.ui.transform;


import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.server.ui.component.QuestionDialog;

import com.vaadin.data.Property;
import com.vaadin.event.Action;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Layout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.Upload;
import com.vaadin.ui.VerticalLayout;


public class QueryPanel extends VerticalLayout
{

    private static final Action CREATE_FILE_ACTION = new Action( "Create new file" );
    private ContainerHost containerHost;
    private String clusterName;
    private QueryType type;
    ETLTransformManager etlTransformManager;
    private final int rowSize = 30;
    private static final String JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk-amd64/bin/";



    public QueryPanel( ETLTransformManager etlTransformManager )
    {
        this.etlTransformManager = etlTransformManager;
    }

    public void init( final QueryType type ){

        final GridLayout upperGrid = new GridLayout();
        upperGrid.setRows( 1 );
        upperGrid.setColumns( 2 );
        upperGrid.setSpacing( true );
        upperGrid.setSizeFull();

        VerticalLayout left = new VerticalLayout();
        left.setSpacing( true );
        left.setSizeFull();

        VerticalLayout right = new VerticalLayout();
        right.setSpacing( true );
        right.setSizeFull();

        final ProgressBar progressBar; progressBar = new ProgressBar();
        progressBar.setId( "indicator" );
        progressBar.setIndeterminate( true );
        progressBar.setVisible( false );


        // Show uploaded file in this placeholder
        final Embedded uploadedFile = new Embedded("Uploaded Query File");

        uploadedFile.setVisible( false );

        final QueryFileUploader receiver = new QueryFileUploader( uploadedFile );

        final TextArea contentOfQueryFile = new TextArea( "Content of query file:" );
        contentOfQueryFile.setSizeFull();
        contentOfQueryFile.setRows( 10 );
        contentOfQueryFile.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                receiver.saveChanges( contentOfQueryFile, receiver.getFile() );
            }
        } );


        // Create the upload with a caption and set receiver later
        Upload upload = new Upload("", receiver);
        upload.setButtonCaption("Start Upload");
        upload.addStyleName( "default" );
        upload.addSucceededListener(receiver);
        upload.addSucceededListener( new Upload.SucceededListener()
        {
            @Override
            public void uploadSucceeded( final Upload.SucceededEvent succeededEvent )
            {
                receiver.showUploadedText( contentOfQueryFile, receiver.getFile() );
            }
        } );

        // Put the components in a panel
        Panel panel = new Panel("Upload Panel");
        Layout panelContent = new VerticalLayout();
        panelContent.addComponents( upload, uploadedFile );
        panel.setContent( panelContent );

        Button refresh = new Button( "Refresh" );
        refresh.addStyleName( "default" );
        refresh.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                receiver.showUploadedText( contentOfQueryFile, receiver.getFile() );
                Notification.show( "refreshed..." );
            }
        } );


        Button saveButton = new Button( "Save Changes" );
        saveButton.addStyleName( "default" );
        saveButton.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                receiver.saveChanges( contentOfQueryFile, receiver.getFile() );
                Notification.show( "saved..." );
            }
        } );


        Button newFileButton = new Button( "Create Query File" );
        newFileButton.addStyleName( "default" );
        newFileButton.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                final QuestionDialog questionDialog =
                        new QuestionDialog<String>( CREATE_FILE_ACTION, "Name your file...",
                                String.class, "Create", "Cancel" );
                questionDialog.getOk().addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( final Button.ClickEvent clickEvent )
                    {
                        if ( questionDialog.getInputField().getValue() != null ){
                            String fileName = questionDialog.getInputField().getValue();
                            File newFile = receiver.createNewFile( fileName );
                            Notification.show( fileName + " is created under /tmp/uploads." );
                            receiver.setFile( newFile );
                            receiver.showUploadedText( contentOfQueryFile, newFile );
                            contentOfQueryFile.setCaption( "Content of \"" + newFile.getName() + "\" query file:" );

                        }
                    }
                } );
                getUI().addWindow( questionDialog.getAlert() );
            }
        } );


        final TextArea std_logs = new TextArea();
        std_logs.setSizeFull();
        std_logs.setRows( rowSize );


        final TextArea std_err_logs = new TextArea();
        std_err_logs.setSizeFull();
        std_err_logs.setRows( rowSize );
        std_err_logs.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( final Property.ValueChangeEvent valueChangeEvent )
            {
                Notification.show( "Error !!!" );
            }
        } );


        final QueryType queryType = type;
        Button runQueryButton = new Button( "Run Query File" );
        runQueryButton.addStyleName( "default" );
        runQueryButton.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                if ( containerHost == null )
                {
                    Notification.show( "Please select node !!!" );
                    return;
                }

                progressBar.setVisible( true );
                String queryFileName = "query_file";
                File newFile = receiver.createNewFile( queryFileName );
                receiver.setFile( newFile );
                receiver.saveChanges( contentOfQueryFile, newFile );
                receiver.showUploadedText( contentOfQueryFile, newFile );

                try
                {
                    receiver.copyFile( containerHost, receiver.getFile() );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
                switch ( queryType ){
                    case HIVE:
                        etlTransformManager.executorService.execute( new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                CommandResult result = executeCommand( containerHost, ". /etc/profile && hive -f " +
                                        receiver.getFile().getAbsolutePath() );
                                if ( result.hasSucceeded() ){
                                    std_logs.setValue( std_logs.getValue() +  getCurrentTime() + result.getStdOut() + "\n" );
                                    std_err_logs.setValue( std_err_logs.getValue() + "\n" + result.getStdErr() );
                                }
                                progressBar.setVisible( false );
                            }
                        } );
                        break;
                    case PIG:
                        etlTransformManager.executorService.execute( new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                CommandResult result = executeCommand( containerHost, ". /etc/profile && "
                                                                        + "export JAVA_HOME=" + JAVA_HOME + " && "
                                                                        + "pig -x mapreduce " +
                                                                          receiver.getFile().getAbsolutePath() );
                                if ( result.hasSucceeded() ){
                                    std_logs.setValue( std_logs.getValue() +  getCurrentTime() + result.getStdOut() + "\n" );
                                    std_err_logs.setValue( std_err_logs.getValue() + getCurrentTime() + result.getStdErr() + "\n");
                                }
                                progressBar.setVisible( false );
                            }
                        } );
                        break;
                }


            }
        } );

        HorizontalLayout runButtonLayout = new HorizontalLayout();
        runButtonLayout.setSpacing( true );
        runButtonLayout.addComponent( runQueryButton );
        runButtonLayout.setComponentAlignment( runQueryButton, Alignment.MIDDLE_LEFT );
        runButtonLayout.addComponent( progressBar );

        HorizontalLayout buttonLayout = new HorizontalLayout();
        buttonLayout.setSpacing( true );
        buttonLayout.addComponent( refresh );
//        buttonLayout.addComponent( newFileButton );
//        buttonLayout.addComponent( saveButton );
        buttonLayout.addComponent( runButtonLayout );

        final TabSheet tabsheet = new TabSheet();
        tabsheet.setCaption( "Query Results" );
        tabsheet.setSizeFull();


        final VerticalLayout tab1 = new VerticalLayout();
        tab1.setSizeFull();
        tab1.setSpacing( true );
        tab1.setCaption( "std_out" );
        tabsheet.addTab(tab1);
        tab1.addComponent( std_logs );

        final VerticalLayout tab2 = new VerticalLayout();
        tab2.setSizeFull();
        tab2.setSpacing( true );
        tab2.setCaption( "std_err");
        tabsheet.addTab(tab2);
        tab2.addComponent( std_err_logs );


        Button clearLogs = new Button( "Clear Logs" );
        clearLogs.addStyleName( "default" );
        clearLogs.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                std_logs.setValue( "" );
                std_err_logs.setValue( "" );
            }
        } );


        left.addComponent( panel );
        left.addComponent( contentOfQueryFile );
        left.addComponent( buttonLayout );
        left.setComponentAlignment( buttonLayout, Alignment.MIDDLE_CENTER );

        right.addComponent( tabsheet );
        right.addComponent( clearLogs );
        right.setComponentAlignment( clearLogs, Alignment.MIDDLE_CENTER );

        upperGrid.addComponent( left, 0, 0 );
        upperGrid.addComponent( right, 1, 0 );
        addComponent( upperGrid );
    }


    public String getCurrentTime(){
        Calendar cal = Calendar.getInstance();
        cal.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        return  "Query start time  : " + sdf.format( cal.getTime() ) + "\n";
    }

    public ContainerHost getContainerHost()
    {
        return containerHost;
    }


    public void setContainerHost( final ContainerHost containerHost )
    {
        this.containerHost = containerHost;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public QueryType getType()
    {
        return type;
    }


    public void setType( final QueryType type )
    {
        this.type = type;
    }


    private CommandResult executeCommand( ContainerHost containerHost, String command ){
        CommandResult result = null;
        try
        {
            // TODO: maximum command timeout is 360000 sec which is 100 hours.
            result = containerHost.execute( new RequestBuilder( command ).withTimeout( 360000 ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return result;
    }
}
