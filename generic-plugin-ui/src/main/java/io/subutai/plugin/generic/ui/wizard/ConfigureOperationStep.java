package io.subutai.plugin.generic.ui.wizard;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vaadin.data.Property;
import com.vaadin.server.Page;
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
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


public class ConfigureOperationStep extends Panel
{
    private static final Logger LOG = LoggerFactory.getLogger( ConfigureOperationStep.class.getName() );
    protected static final String BUTTON_STYLE_NAME = "default";
    private static final String VIEW_BUTTON_CAPTION = "View";
    private static final String EDIT_BUTTON_CAPTION = "Edit";
    private static final String DELETE_BUTTON_CAPTION = "Delete";
    private TextField cwd = new TextField( "cwd" );
    private TextField timeOut = new TextField( "Timeout" );
    private CheckBox daemon = new CheckBox( "Daemon" );
    private Wizard wizard;

    private GenericPlugin genericPlugin;
    private VerticalLayout content;
    private Label title;
    private ComboBox profileSelect;
    private HorizontalLayout fieldGrid;
    private Profile profile;

    private final Table operationTable;


    public ConfigureOperationStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        this.wizard = wizard;
        this.genericPlugin = genericPlugin;

        operationTable = createTableTemplate( "Operations" );

        content = new VerticalLayout();
        title = new Label( "Configure operations" );

        profileSelect = new ComboBox( "Profile" );
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

        final TextField newName = new TextField( "New operation name (maximum 10 characters)" );
        newName.setInputPrompt( "Enter new operation name" );
        newName.setRequired( true );

        final TextField newCommand = new TextField( "New command" );
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

        Button options = new Button( "Options" );
        options.addStyleName( BUTTON_STYLE_NAME );

        Button addOperation = new Button( "Add operation" );
        addOperation.addStyleName( BUTTON_STYLE_NAME );

        fieldGrid.addComponent( newName );
        fieldGrid.addComponent( newCommand );
        //fieldGrid.addComponent (templates);
        fieldGrid.addComponent( options );
        fieldGrid.addComponent( addOperation );
        fieldGrid.setComponentAlignment( newName, Alignment.BOTTOM_LEFT );
        fieldGrid.setComponentAlignment( newCommand, Alignment.BOTTOM_LEFT );
        //fieldGrid.setComponentAlignment (templates, Alignment.BOTTOM_LEFT);
        fieldGrid.setComponentAlignment( options, Alignment.BOTTOM_LEFT );
        fieldGrid.setComponentAlignment( addOperation, Alignment.BOTTOM_LEFT );

        content.addComponent( title );
        content.addComponent( profileSelect );
        content.addComponent( fieldGrid );
        content.addComponent( operationTable );

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
                else if ( profileSelect.getValue() == null )
                {
                    show( "Please choose profile" );
                }
                else
                {
                    Profile current = ( Profile ) profileSelect.getValue();
                    if ( genericPlugin.IsOperationRegistered( newName.getValue() ) )
                    {
                        show( "Operation with such name already exist!" );
                    }
                    else
                    {
                        genericPlugin.saveOperation( current.getId(), newName.getValue(), newCommand.getValue(),
                                cwd.getValue(), timeOut.getValue(), daemon.getValue() );
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
        //operationTable.addContainerProperty ("Template", String.class, null);
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
            final Button viewBtn = new Button( VIEW_BUTTON_CAPTION );
            final Button editBtn = new Button( EDIT_BUTTON_CAPTION );
            final Button deleteBtn = new Button( DELETE_BUTTON_CAPTION );

            addStyleNameToButtons( viewBtn, editBtn, deleteBtn );

            final HorizontalLayout availableOperations = new HorizontalLayout();
            availableOperations.addStyleName( "default" );
            availableOperations.setSpacing( true );

            addGivenComponents( availableOperations, viewBtn, editBtn, deleteBtn );
            sampleTable.addItem( new Object[] {
                    operation.getOperationName(), operation.getCommandName(), availableOperations
            }, null );

            addClickListenerToViewButton( viewBtn, operation );
            addClickListenerToEditButton( editBtn, operation );
            addClickListenerToDeleteButton( deleteBtn, operation );
        }
    }


    private void addClickListenerToDeleteButton( final Button deleteBtn, final Operation operation )
    {
        getButton( DELETE_BUTTON_CAPTION, deleteBtn ).addClickListener( new Button.ClickListener()
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
        getButton( VIEW_BUTTON_CAPTION, viewBtn ).addClickListener( new Button.ClickListener()
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


    private void addClickListenerToEditButton( Button viewBtn, final Operation operation )
    {
        getButton( EDIT_BUTTON_CAPTION, viewBtn ).addClickListener( new Button.ClickListener()
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
					editTemplate.setValue (operationTable.getItem (edit.getData()).getItemProperty ("Template")
					.toString());*/

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
                            genericPlugin.updateOperation( operation, editCommand.getValue(), editCwd.getValue(),
                                    editTimeout.getValue(), editDaemon.getValue() );
                            show( "Operation saved successfully!" );
                            subWindow.close();
                            refreshUI();
                        }
                    }
                } );


                editInfo.addComponent( editCommand );
                //editInfo.addComponent (editTemplate);
                editInfo.addComponent( editCwd );
                editInfo.addComponent( editTimeout );
                editInfo.setComponentAlignment( editCommand, Alignment.BOTTOM_LEFT );
                //editInfo.setComponentAlignment (editTemplate, Alignment.BOTTOM_LEFT);
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
        Notification.show( notification );
    }


    private Button getButton( String caption, Button... buttons )
    {
        for ( Button b : buttons )
        {
            if ( b.getCaption().equals( caption ) )
            {
                return b;
            }
        }
        return null;
    }
}
