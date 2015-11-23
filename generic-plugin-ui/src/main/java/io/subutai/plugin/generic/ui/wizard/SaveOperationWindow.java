package io.subutai.plugin.generic.ui.wizard;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;

import com.vaadin.server.Page;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.Upload;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Operation;


/**
 * Created by ermek on 11/23/15.
 */
public class SaveOperationWindow extends Window
{
    protected static final String BUTTON_STYLE_NAME = "default";
    private Button uploadScript;
    private TextField cwd = new TextField( "CWD" );
    private TextField timeOut = new TextField( "Timeout" );
    private CheckBox daemon = new CheckBox( "Daemon" );

    private TextField newCommand;
    private TextField newName;
    private Button addOperation;
    private CheckBox scriptCheck;
    private TextArea commandArea;
    private Window uploadWindow;
    private boolean fromFile = false;
    private VerticalLayout subContent;


    public SaveOperationWindow( final GenericPlugin genericPlugin, final Long profileId, final Wizard wizard,
                                final boolean isSave )
    {
        super( "Operation info" );
        setModal( true );
        setClosable( true );
        addStyleName( "default" );
        center();
        setHeight( "50%" );
        setWidth( "50%" );

        subContent = new VerticalLayout();
        subContent.setSpacing( true );
        subContent.setMargin( true );

        newName = new TextField( "Operation name" );
        newName.setInputPrompt( "Enter new operation name" );
        newName.setRequired( true );
        newName.setWidth( "30%" );

        newCommand = new TextField( "Command" );
        newCommand.setRequired( true );
        newCommand.setInputPrompt( "Enter new command" );

        commandArea = new TextArea( "Command" );
        commandArea.setRequired( true );
        commandArea.setInputPrompt( "Enter new command" );
        commandArea.setWidth( "100%" );

        cwd.setValue( "/" );
        cwd.setData( "/" );
        cwd.setRequired( true );
        timeOut.setValue( "30" );
        timeOut.setData( "30" );
        timeOut.setRequired( true );
        daemon.setValue( false );
        daemon.setData( false );
        daemon.setRequired( true );

        scriptCheck = new CheckBox( "Script" );
        scriptCheck.setEnabled( false );
        scriptCheck.setValue( false );

        uploadScript = new Button( "Upload script" );
        uploadScript.addStyleName( "default" );
        uploadScript.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                uploadWindow = createWindowTemplate( "Upload Script" );

                VerticalLayout content = new VerticalLayout();
                content.setSpacing( true );
                content.setMargin( true );

                MyUploader uploader = new MyUploader();
                Upload upload = new Upload( "", uploader );
                upload.setButtonCaption( "Start Upload" );
                upload.addSucceededListener( uploader );
                upload.addFailedListener( uploader );

                Button cancel = new Button( "Cancel" );
                cancel.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent event )
                    {
                        uploadWindow.close();
                    }
                } );

                content.addComponent( upload );
                content.addComponent( cancel );
                content.setComponentAlignment( upload, Alignment.BOTTOM_CENTER );
                content.setComponentAlignment( cancel, Alignment.BOTTOM_CENTER );
                uploadWindow.setContent( content );
                UI.getCurrent().addWindow( uploadWindow );
            }
        } );


        HorizontalLayout scriptLayout = new HorizontalLayout();
        scriptLayout.setSpacing( true );
        scriptLayout.setWidth( "100%" );

        VerticalLayout scriptItems = new VerticalLayout();
        scriptItems.setSpacing( true );

        scriptItems.addComponent( uploadScript );
        scriptItems.addComponent( scriptCheck );

        if ( !isSave )
        {
            Operation operation = wizard.getCurrentOperation();
            byte[] decodedBytes = Base64.decodeBase64( operation.getCommandName() );

            commandArea.setValue( new String( decodedBytes ) );
            cwd.setValue( operation.getCwd() );
            timeOut.setValue( operation.getTimeout() );
            daemon.setValue( operation.getDaemon() );
            newName.setValue( operation.getOperationName() );
        }


        addOperation = new Button( "Save" );
        addOperation.addStyleName( BUTTON_STYLE_NAME );
        addOperation.addClickListener( new Button.ClickListener()
        {
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( newName.getValue().isEmpty() )
                {
                    show( "Please enter operation name" );
                }
                else if ( commandArea.getValue().isEmpty() )
                {
                    show( "Please enter command" );
                }
                else
                {
                    if ( isSave )
                    {
                        boolean exist = false;
                        for ( final Operation operation : genericPlugin.getProfileOperations( profileId ) )
                        {
                            if ( newName.getValue().equals( operation.getOperationName() ) )
                            {
                                show( "Operation with such name already exists" );
                                exist = true;
                            }
                        }

                        if ( !exist )
                        {
                            genericPlugin.saveOperation( profileId, newName.getValue(), commandArea.getValue(),
                                    cwd.getValue(), timeOut.getValue(), daemon.getValue(), fromFile );
                            cleanTextBoxes();
                            wizard.getConfigureOperationStep().refreshUI();
                            show( "Operation saved successfully!" );
                            close();
                        }
                    }
                    else
                    {

                        genericPlugin
                                .updateOperation( wizard.getCurrentOperation().getOperationId(), commandArea.getValue(),
                                        cwd.getValue(), timeOut.getValue(), daemon.getValue(), fromFile,
                                        newName.getValue() );
                        show( "Operation edited successfully!" );
                        fromFile = false;
                        close();
                        wizard.getConfigureOperationStep().refreshUI();
                    }
                }
            }
        } );

        scriptLayout.addComponent( commandArea );
        scriptLayout.addComponent( scriptItems );
        scriptLayout.setComponentAlignment( scriptItems, Alignment.MIDDLE_LEFT );

        subContent.addComponent( newName );
        subContent.addComponent( cwd );
        subContent.addComponent( timeOut );
        subContent.addComponent( daemon );
        subContent.addComponent( scriptLayout );
        subContent.addComponent( addOperation );

        setContent( subContent );
    }


    class MyUploader implements Upload.Receiver, Upload.SucceededListener, Upload.FailedListener
    {
        private File file;
        private String path;


        @Override
        public OutputStream receiveUpload( String filename, String MIMEType )
        {
            if ( filename != null && !filename.isEmpty() )
            {
                FileOutputStream fos;
                new File( "/tmp/uploads" ).mkdirs();
                this.file = new File( "/tmp/uploads/" + filename );
                this.path = new String( "/tmp/uploads/" + filename );
                try
                {
                    fos = new FileOutputStream( this.file );
                }
                catch ( final java.io.FileNotFoundException e )
                {
                    new Notification( "Could not open file<br/>", e.getMessage(), Notification.Type.ERROR_MESSAGE )
                            .show( Page.getCurrent() );
                    return null;
                }

                return fos;
            }
            else
            {
                show( "Please select file" );
                return null;
            }
        }


        @Override
        public void uploadSucceeded( Upload.SucceededEvent event )
        {
            try
            {
                byte[] encoded = Files.readAllBytes( Paths.get( this.path ) );
                commandArea.setValue( new String( encoded ) );
                FileUtils.deleteDirectory( new File( "/tmp" ) );
                fromFile = true;
                uploadWindow.close();
                scriptCheck.setValue( true );
                newCommand.setEnabled( false );
                show( "File is uploaded" );
            }
            catch ( IOException e )
            {
                show( "Server is busy, please try again" );
                e.printStackTrace();
            }
        }


        @Override
        public void uploadFailed( Upload.FailedEvent event )
        {
            show( "Upload failed, please try again" );
            // TODO: Log the failure on screen.
        }
    }


    private void show( String notification )
    {
        Notification notif = new Notification( notification );
        notif.setDelayMsec( 2000 );
        notif.show( Page.getCurrent() );
    }


    private void cleanTextBoxes()
    {
        newName.setValue( "" );
        commandArea.setValue( "" );
        scriptCheck.setRequired( false );
    }


    private Window createWindowTemplate( final String caption )
    {
        Window subWindow = new Window( caption );
        subWindow.setClosable( true );
        subWindow.setModal( true );
        subWindow.addStyleName( "default" );
        subWindow.center();

        return subWindow;
    }
}
