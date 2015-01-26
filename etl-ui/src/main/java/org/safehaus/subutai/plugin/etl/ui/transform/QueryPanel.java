package org.safehaus.subutai.plugin.etl.ui.transform;

import java.io.File;

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


public class QueryPanel extends GridLayout
{

    private static final Action CREATE_FILE_ACTION = new Action( "Create new file" );

    public QueryPanel( )
    {
        init();
    }

    public void init(){

        setSpacing( true );
        setImmediate( true );
        setSizeFull();
        setRows( 4 );
        setColumns( 12 );

        // Show uploaded file in this placeholder
        final Embedded uploadedFile = new Embedded("Uploaded Query File");

        uploadedFile.setVisible( false );

        final QueryFileUploader receiver = new QueryFileUploader( uploadedFile );

        final TextArea contentOfQueryFile = new TextArea( "Content of uploaded query file:" );
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

        addComponent( panel, 1, 0 );
        addComponent( contentOfQueryFile, 1, 1 );
        addComponent( buttonLayout, 1, 2 );
        setComponentAlignment( buttonLayout, Alignment.BOTTOM_CENTER );

        Button runQuery = new Button( "Run" );
        runQuery.addStyleName( "default" );

        addComponent( runQuery, 6, 1  );
        setComponentAlignment( runQuery, Alignment.MIDDLE_LEFT );
        addComponent( logs, 7, 1 );
    }

}
