package io.subutai.plugin.generic.ui.wizard;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;

import com.vaadin.data.Property;
import com.vaadin.server.Page;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Table;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.Upload;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public class ConfigureOperationStep extends Panel
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigureOperationStep.class.getName() );
    protected static final String BUTTON_STYLE_NAME = "default";
    private TextField cwd = new TextField( "CWD" );
    private TextField timeOut = new TextField( "Timeout" );
    private CheckBox daemon = new CheckBox( "Daemon" );

    private GenericPlugin genericPlugin;
    private VerticalLayout content;
    private Label title;
    private ComboBox profileSelect;
    private HorizontalLayout fieldGrid;
    private Profile profile;

    private Table operationTable;
    private TextField newCommand;
    private TextField newName;
    private Window uploadWindow;
    private Wizard wizard;
    private boolean fromFile = false;
    private CheckBox scriptCheck;


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
                    // Error while opening the file
                    // TODO: notify that file is not opened
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
                newCommand.setValue( new String( encoded ) );
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
                // TODO: file is not saved
            }
        }


        @Override
        public void uploadFailed( Upload.FailedEvent event )
        {
            show( "Upload failed, please try again" );
            // TODO: Log the failure on screen.
        }
    }


    public ConfigureOperationStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        this.genericPlugin = genericPlugin;
        this.wizard = wizard;

        operationTable = createTableTemplate( "Operations" );

        content = new VerticalLayout();
        title = new Label( "<h2>Configure operations</h2>" );
        title.setContentMode( ContentMode.HTML );

        profileSelect = new ComboBox( "Choose Profile" );
        profileSelect.setNullSelectionAllowed( false );
        profileSelect.setTextInputAllowed( false );
        profileSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                profile = ( Profile ) event.getProperty().getValue();
                refreshUI();
            }
        } );
        refreshProfileInfo();

        fieldGrid = new HorizontalLayout();
        fieldGrid.setSpacing( true );
        fieldGrid.setWidth( "100%" );

        newName = new TextField( "Operation name" );
        newName.setInputPrompt( "Enter new operation name" );
        newName.setRequired( true );

        newCommand = new TextField( "Command" );
        newCommand.setRequired( true );
        newCommand.setInputPrompt( "Enter new command" );

        //
        /*final ComboBox templates = new ComboBox ("Template");
        templates.setNullSelectionAllowed (false);
		templates.setTextInputAllowed (false);
		for (Template t : wizard.getRegistry().getAllTemplates())
		{
			templates.addItem (t.getTemplateName());
		}
		templates.setValue (wizard.getRegistry().getAllTemplates().get (0).getTemplateName());*/
        //


        cwd.setValue( "/" );
        cwd.setData( "/" );
        timeOut.setValue( "30" );
        timeOut.setData( "30" );
        daemon.setValue( false );
        daemon.setData( false );

        scriptCheck = new CheckBox( "Script" );
        scriptCheck.setEnabled( false );
        scriptCheck.setValue( false );

        Button options = new Button( "Options" );
        options.addStyleName( BUTTON_STYLE_NAME );

        Button addOperation = new Button( "Add operation" );
        addOperation.addStyleName( BUTTON_STYLE_NAME );


        Button uploadScript = new Button( "Upload script" );
        uploadScript.addStyleName( "default" );
        uploadScript.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                uploadWindow = new Window( "Upload Script" );
                uploadWindow.center();
                uploadWindow.setClosable( false );
                uploadWindow.addStyleName( "default" );
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


        fieldGrid.addComponent( newName );
        fieldGrid.addComponent( newCommand );
        fieldGrid.addComponent( cwd );
        fieldGrid.addComponent( timeOut );
        fieldGrid.addComponent( daemon );
        // fieldGrid.addComponent (templates);
        //        fieldGrid.addComponent( options );
        fieldGrid.addComponent( addOperation );
        fieldGrid.addComponent( scriptCheck );
        fieldGrid.addComponent( uploadScript );
        fieldGrid.setComponentAlignment( daemon, Alignment.BOTTOM_LEFT );
        fieldGrid.setComponentAlignment( scriptCheck, Alignment.BOTTOM_RIGHT );
        //        fieldGrid.setComponentAlignment( newCommand, Alignment.BOTTOM_LEFT );
        //fieldGrid.setComponentAlignment (templates, Alignment.BOTTOM_LEFT);
        //        fieldGrid.setComponentAlignment( options, Alignment.BOTTOM_LEFT );
        fieldGrid.setComponentAlignment( addOperation, Alignment.BOTTOM_LEFT );
        fieldGrid.setComponentAlignment( uploadScript, Alignment.BOTTOM_LEFT );

        content.addComponent( title );
        content.addComponent( profileSelect );
        content.addComponent( fieldGrid );
        content.addComponent( operationTable );
        content.setSpacing( true );

        Button back = new Button( "Back" );
        back.addStyleName( BUTTON_STYLE_NAME );
        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.changeWindow( 0 );
                wizard.putForm();
            }
        } );
        content.addComponent( back );


        options.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                final Window subWindow = new Window( "Edit operation" );
                subWindow.setClosable( false );
                subWindow.addStyleName( "default" );
                subWindow.center();
                VerticalLayout subContent = new VerticalLayout();
                subContent.setSpacing( true );
                subContent.setMargin( true );
                HorizontalLayout fields = new HorizontalLayout();
                fields.setSpacing( true );
                fields.addComponent( cwd );
                fields.addComponent( timeOut );
                fields.setComponentAlignment( cwd, Alignment.BOTTOM_LEFT );
                fields.setComponentAlignment( timeOut, Alignment.BOTTOM_LEFT );

                HorizontalLayout buttonGrid = new HorizontalLayout();
                buttonGrid.setSpacing( true );

                Button back = new Button( "Back" );
                back.setSizeFull();
                back.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent event )
                    {
                        cwd.setValue( ( String ) cwd.getData() );
                        timeOut.setValue( ( String ) timeOut.getData() );
                        daemon.setValue( ( boolean ) daemon.getData() );
                        subWindow.close();
                    }
                } );

                Button save = new Button( "Save" );
                save.setSizeFull();
                save.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent event )
                    {
                        cwd.setData( cwd.getValue() );
                        timeOut.setData( timeOut.getValue() );
                        daemon.setData( daemon.getValue() );
                        subWindow.close();
                    }
                } );

                buttonGrid.addComponent( back );
                buttonGrid.addComponent( save );
                buttonGrid.setComponentAlignment( back, Alignment.BOTTOM_CENTER );
                buttonGrid.setComponentAlignment( save, Alignment.BOTTOM_CENTER );

                subContent.addComponent( fields );
                subContent.addComponent( daemon );
                subContent.addComponent( buttonGrid );
                subContent.setComponentAlignment( daemon, Alignment.BOTTOM_LEFT );
                subContent.setComponentAlignment( buttonGrid, Alignment.BOTTOM_CENTER );
                subWindow.setContent( subContent );
                UI.getCurrent().addWindow( subWindow );
            }
        } );


        addOperation.addClickListener( new Button.ClickListener()
        {
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( newName.getValue().isEmpty() )
                {
                    show( "Please enter operation name" );
                }
                else if ( newCommand.getValue().isEmpty() )
                {
                    show( "Please enter command" );
                }
                else
                {


                    Profile current = ( Profile ) profileSelect.getValue();
                    boolean exist = false;
                    for ( final Operation operation : genericPlugin.getProfileOperations( current.getId() ) )
                    {
                        if ( newName.getValue().equals( operation.getOperationName() ) )
                        {
                            show( "Operation with such name already exists" );
                            exist = true;
                        }
                    }

                    if ( !exist )
                    {
                        genericPlugin.saveOperation( current.getId(), newName.getValue(), newCommand.getValue(),
                                cwd.getValue(), timeOut.getValue(), daemon.getValue(), fromFile );
                        newCommand.setEnabled( true );
                        cleanTextBoxes();
                        show( "Operation saved successfully!" );
                        refreshUI();
                    }
                }
            }
        } );
        this.setContent( content );
    }


    private Table createTableTemplate( final String caption )
    {
        final Table table = new Table( caption );
        table.addContainerProperty( "Operation name", String.class, null );
        table.addContainerProperty( "Command", String.class, null );
        // table.addContainerProperty ("Template", String.class, null);
        table.addContainerProperty( "Actions", HorizontalLayout.class, null );
        table.setSizeFull();
        table.setPageLength( 10 );
        table.setSelectable( false );
        table.setImmediate( true );

        return table;
    }


    private void refreshUI()
    {
        if ( profile != null )
        {
            List<Operation> operations = genericPlugin.getProfileOperations( profile.getId() );
            populateTable( operationTable, operations );
        }
    }


    private void populateTable( final Table sampleTable, final List<Operation> operations )
    {
        sampleTable.removeAllItems();

        for ( Operation operation : operations )
        {
            final Button viewBtn = new Button( "View" );
            final Button editBtn = new Button( "Edit" );
            final Button deleteBtn = new Button( "Delete" );

            addStyleNameToButtons( viewBtn, editBtn, deleteBtn );

            final HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.addStyleName( "default" );
            availableOperations.setSpacing( true );

            addGivenComponents( availableOperations, viewBtn, editBtn, deleteBtn );
            sampleTable.addItem(
                    new Object[] { operation.getOperationName(), operation.getCommandName(), availableOperations },
                    null );

            addClickListenerToViewButton( viewBtn, operation );
            addClickListenerToEditButton( editBtn, operation );
            addClickListenerToDeleteButton( deleteBtn, operation );
        }
    }


    private void addClickListenerToDeleteButton( final Button deleteBtn, final Operation operation )
    {
        deleteBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                genericPlugin.deleteOperation( operation.getOperationId() );
                show( "Operation deleted successfully!" );
                refreshUI();
            }
        } );
    }


    private void addClickListenerToViewButton( Button viewBtn, final Operation operation )
    {
        viewBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                final Window subWindow = new Window( "Edit operation" );
                subWindow.setClosable( false );
                subWindow.addStyleName( "default" );
                subWindow.center();

                VerticalLayout subContent = new VerticalLayout();
                subContent.setSpacing( true );
                subContent.setMargin( true );


                Label operationLbl = new Label( "Operation name: " + operation.getOperationName() );
                Label command = new Label( "Command: " + operation.getCommandName() );
                Label cwd = new Label( "Cwd: " + operation.getCwd() );
                Label timeout = new Label( "Timeout: " + operation.getTimeout() );
                Label daemon = new Label( "Daemon: " );
                if ( operation.getDaemon() )
                {
                    daemon.setValue( daemon.getValue() + "yes" );
                }
                else
                {
                    daemon.setValue( daemon.getValue() + "no" );
                }

                Button close = new Button( "Close" );
                close.addStyleName( BUTTON_STYLE_NAME );
                close.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent event )
                    {
                        subWindow.close();
                    }
                } );

                subContent.addComponent( operationLbl );
                subContent.addComponent( command );
                subContent.addComponent( cwd );
                subContent.addComponent( timeout );
                subContent.addComponent( daemon );
                subContent.addComponent( close );
                subContent.setComponentAlignment( close, Alignment.BOTTOM_CENTER );

                subWindow.setContent( subContent );
                UI.getCurrent().addWindow( subWindow );
            }
        } );
    }


    private void addClickListenerToEditButton( Button editBtn, final Operation operation )
    {
        editBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                final Window subWindow = new Window( "Edit operation" );
                subWindow.setClosable( false );
                subWindow.addStyleName( "default" );
                subWindow.center();

                VerticalLayout subContent = new VerticalLayout();
                subContent.setSpacing( true );
                subContent.setMargin( true );

                HorizontalLayout editInfo = new HorizontalLayout();
                editInfo.setSpacing( true );
                editInfo.setWidth( "100%" );

                final TextField editCommand = new TextField( "Edit command" );
                editCommand.setValue( operation.getCommandName() );

				/*final ComboBox editTemplate = new ComboBox ("Edit template");
                for (Template t : wizard.getRegistry().getAllTemplates())
				{
					editTemplate.addItem (t.getTemplateName());
				}
				editTemplate.setValue (operationTable.getItem (edit.getData()).getItemProperty ("Template").toString()
				);*/

                final TextField editCwd = new TextField( "Edit cwd" );
                editCwd.setValue( operation.getCwd() );

                final TextField editTimeout = new TextField( "Edit timeout" );
                editTimeout.setValue( operation.getTimeout() );

                final CheckBox editDaemon = new CheckBox( "Edit daemon" );
                editDaemon.setValue( operation.getDaemon() );


                HorizontalLayout buttons = new HorizontalLayout();
                buttons.setSpacing( true );
                buttons.setWidth( "100%" );

                Button cancel = new Button( "Cancel" );
                cancel.addStyleName( BUTTON_STYLE_NAME );
                cancel.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent clickEvent )
                    {
                        subWindow.close();
                    }
                } );


                Button finalEdit = new Button( "Edit" );
                finalEdit.addStyleName( BUTTON_STYLE_NAME );
                finalEdit.addClickListener( new Button.ClickListener()
                {
                    @Override
                    public void buttonClick( Button.ClickEvent clickEvent )
                    {
                        if ( editCommand.getValue().isEmpty() )
                        {
                            Notification notif = new Notification( "Please enter command" );
                            notif.setDelayMsec( 2000 );
                            notif.show( Page.getCurrent() );
                        }
                        else
                        {
                            genericPlugin.updateOperation( operation.getOperationId(), editCommand.getValue(),
                                    editCwd.getValue(), editTimeout.getValue(), editDaemon.getValue(), fromFile );
                            show( "Operation saved successfully!" );
                            fromFile = false;
                            subWindow.close();
                            refreshUI();
                        }
                    }
                } );


                editInfo.addComponent( editCommand );
                // editInfo.addComponent (editTemplate);
                editInfo.addComponent( editCwd );
                editInfo.addComponent( editTimeout );
                editInfo.setComponentAlignment( editCommand, Alignment.BOTTOM_LEFT );
                // editInfo.setComponentAlignment (editTemplate, Alignment.BOTTOM_LEFT);
                editInfo.setComponentAlignment( editCwd, Alignment.BOTTOM_LEFT );
                editInfo.setComponentAlignment( editTimeout, Alignment.BOTTOM_LEFT );

                buttons.addComponent( cancel );
                buttons.addComponent( finalEdit );
                buttons.setComponentAlignment( cancel, Alignment.BOTTOM_CENTER );
                buttons.setComponentAlignment( finalEdit, Alignment.BOTTOM_CENTER );


                subContent.addComponent( editInfo );
                subContent.addComponent( editDaemon );
                subContent.setComponentAlignment( editDaemon, Alignment.BOTTOM_CENTER );
                subContent.addComponent( buttons );


                subWindow.setContent( subContent );
                UI.getCurrent().addWindow( subWindow );
            }
        } );
    }


    private void refreshProfileInfo()
    {
        profileSelect.removeAllItems();
        List<Profile> profileInfo = genericPlugin.getProfiles();
        if ( profileInfo != null && !profileInfo.isEmpty() )
        {
            for ( Profile pi : profileInfo )
            {
                profileSelect.addItem( pi );
                profileSelect.setItemCaption( pi, pi.getName() );
            }

            Long currentProfileId = wizard.getCurrentProfileId();
            if ( currentProfileId != null )
            {
                for ( final Profile pi : profileInfo )
                {
                    if ( pi.getId().equals( currentProfileId ) )
                    {
                        profileSelect.setValue( pi );
                        profileSelect.setItemCaption( pi, pi.getName() );
                    }
                }
            }
            else
            {
                Profile temp = profileInfo.get( 0 );
                profileSelect.setValue( temp );
                profileSelect.setItemCaption( temp, temp.getName() );
            }
        }
    }


    private void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    private void addGivenComponents( HorizontalLayout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
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
        newCommand.setValue( "" );
    }
}
