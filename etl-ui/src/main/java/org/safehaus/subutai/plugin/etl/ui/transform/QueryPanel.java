package org.safehaus.subutai.plugin.etl.ui.transform;


import java.io.File;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.server.ui.component.QuestionDialog;

import com.vaadin.event.Action;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Layout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.Upload;
import com.vaadin.ui.VerticalLayout;


public class QueryPanel extends VerticalLayout
{

    private static final Action CREATE_FILE_ACTION = new Action( "Create new file" );
    private ContainerHost containerHost;
    private QueryType type;
    ETLTransformManager etlTransformManager;



    public QueryPanel( ETLTransformManager etlTransformManager )
    {
        this.etlTransformManager = etlTransformManager;
        init( QueryType.HIVE );
    }

    public void init( QueryType type ){

        final GridLayout gridLayout = new GridLayout();
        gridLayout.setSpacing( true );
        gridLayout.setMargin( true );
        gridLayout.setSizeFull();
        gridLayout.setRows( 4 );
        gridLayout.setColumns( 12 );

        // Show uploaded file in this placeholder
        final Embedded uploadedFile = new Embedded("Uploaded Query File");

        uploadedFile.setVisible( false );

        final QueryFileUploader receiver = new QueryFileUploader( uploadedFile );

        final TextArea contentOfQueryFile = new TextArea( "Content of query file:" );
        contentOfQueryFile.setWidth( "600px" );
        contentOfQueryFile.setHeight( "400px" );

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

        //
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
                        }
                    }
                } );
                getUI().addWindow( questionDialog.getAlert() );
            }
        } );


        TextArea logs = new TextArea( "Logs" );
        logs.setWidth( "600px" );
        logs.setHeight( "400px" );

        HorizontalLayout buttonLayout = new HorizontalLayout();
        buttonLayout.setSpacing( true );
        buttonLayout.addComponent( refresh );
        buttonLayout.addComponent( newFileButton );
        buttonLayout.addComponent( saveButton );

        gridLayout.addComponent( panel, 0, 0, 11, 0 );
        gridLayout.addComponent( contentOfQueryFile, 0, 1, 5, 1 );
        gridLayout.addComponent( buttonLayout, 0, 2, 4, 2 );
        gridLayout.setComponentAlignment( buttonLayout, Alignment.BOTTOM_CENTER );

        final QueryType queryType = type;
        Button runQueryButton = new Button( "Run" );
        runQueryButton.addStyleName( "default" );
        runQueryButton.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent event )
            {
                switch ( queryType ){
                    case HIVE:
                        executeCommand( containerHost, "hive -f " + receiver.getFile().getAbsolutePath() );
                        break;
                    case PIG:
                        executeCommand( containerHost, "pig -x mapreduce " + receiver.getFile().getAbsolutePath() );
                        break;
                }
            }
        } );

        gridLayout.addComponent( runQueryButton, 6, 1 );
        gridLayout.setComponentAlignment( runQueryButton, Alignment.MIDDLE_LEFT );
        gridLayout.addComponent( logs, 7, 1 );
        addComponent( gridLayout );
    }


    public ContainerHost getContainerHost()
    {
        return containerHost;
    }


    public void setContainerHost( final ContainerHost containerHost )
    {
        this.containerHost = containerHost;
    }


    public QueryType getType()
    {
        return type;
    }


    public void setType( final QueryType type )
    {
        this.type = type;
    }


    private void executeCommand( ContainerHost containerHost, String command ){

        try
        {
            containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }
}
