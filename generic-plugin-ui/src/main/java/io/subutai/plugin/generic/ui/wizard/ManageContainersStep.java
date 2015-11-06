package io.subutai.plugin.generic.ui.wizard;


import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.vaadin.annotations.Theme;
import com.vaadin.data.Property;
import com.vaadin.server.Page;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Table;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.VerticalLayout;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;


@Theme( "valo" )
public class ManageContainersStep extends Panel
{
    private static final Logger LOG = LoggerFactory.getLogger( Wizard.class.getName() );
    private static final String EXECUTE_BUTTON_CAPTION = "Execute";
    private static final String BUTTON_STYLE_NAME = "default";
    private Environment currentEnvironment;
    private Profile currentProfile;
    private String currentTemplate;
    private Table containerTable;
    private Operation currentOperation;
    private Wizard wizard;
    private TextArea output = new TextArea( "Output" );

    private GenericPlugin genericPlugin;
    private VerticalLayout content;
    private HorizontalLayout comboGrid;
    private ComboBox envSelect;
    private ComboBox profileSelect;
    private ComboBox templates;


    public ManageContainersStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        this.wizard = wizard;
        this.genericPlugin = genericPlugin;
        this.setSizeFull();

        content = new VerticalLayout();
        Label title = new Label( "Manage containers" );

        comboGrid = new HorizontalLayout();
        comboGrid.setWidth( "100%" );
        comboGrid.setSpacing( true );

        envSelect = new ComboBox( "Environment" );
        envSelect.setNullSelectionAllowed( false );
        envSelect.setTextInputAllowed( false );

        profileSelect = new ComboBox( "Profile" );
        profileSelect.setNullSelectionAllowed( false );
        profileSelect.setTextInputAllowed( false );

        templates = new ComboBox( "Template" );
        templates.setNullSelectionAllowed( false );
        templates.setTextInputAllowed( false );

        comboGrid.addComponent( envSelect );
        comboGrid.addComponent( templates );
        comboGrid.addComponent( profileSelect );

        containerTable = createTableTemplate( "Containers" );
        containerTable.setHeight( "200px" );
        content.addComponent( title );
        content.addComponent( comboGrid );
        content.addComponent( containerTable );

        templates.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                currentTemplate = ( String ) event.getProperty().getValue();
                refreshUI();
            }
        } );

        profileSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                currentProfile = ( Profile ) event.getProperty().getValue();
                refreshUI();
            }
        } );

        envSelect.addValueChangeListener( new Property.ValueChangeListener()
        {
            @Override
            public void valueChange( Property.ValueChangeEvent event )
            {
                currentEnvironment = ( Environment ) event.getProperty().getValue();
                refreshTemplates( currentEnvironment );
                refreshUI();
            }
        } );

        refreshPluginInfo();

        Button back = new Button( "Back" );
        addStyleNameToButtons( back );
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
        output.setSizeFull();
        output.setRows( 15 );
        // output.setReadOnly (true); TODO: set output readonly without making its design like a label
        content.addComponent( output );
        this.setContent( content );
    }


    protected void addOutput( String output )
    {
        if ( !Strings.isNullOrEmpty( output ) )
        {
            this.output.setValue( output );
            this.output.setCursorPosition( this.output.getValue().length() - 1 );
        }
    }


    private void refreshPluginInfo()
    {
        Profile currentProfile = ( Profile ) profileSelect.getValue();
        profileSelect.removeAllItems();
        List<Profile> profileInfo = genericPlugin.getProfiles();
        if ( profileInfo != null && !profileInfo.isEmpty() )
        {
            for ( Profile pi : profileInfo )
            {
                profileSelect.addItem( pi );
                profileSelect.setItemCaption( pi, pi.getName() );
            }
            if ( currentProfile != null )
            {
                for ( Profile pi : profileInfo )
                {
                    if ( pi.getName().equals( currentProfile.getName() ) )
                    {
                        profileSelect.setValue( pi );
                    }
                }
            }
            else
            {
                profileSelect.setValue( profileInfo.get( 0 ) );
            }
        }

        Environment currentEvn = ( Environment ) envSelect.getValue();
        envSelect.removeAllItems();
        Set<Environment> environments = wizard.getManager().getEnvironments();
        if ( environments != null && !environments.isEmpty() )
        {
            for ( Environment environment : environments )
            {
                envSelect.addItem( environment );
                envSelect.setItemCaption( environment, environment.getName() );
            }
            if ( currentEvn != null )
            {
                for ( Environment env : environments )
                {
                    if ( env.getName().equals( currentEvn.getName() ) )
                    {
                        envSelect.setValue( env );
                    }
                }
            }
            else
            {
                for ( Environment env : environments )
                {
                    envSelect.setValue( env );
                    envSelect.setItemCaption( env, env.getName() );
                    break;
                }
            }
        }
    }


    private void refreshTemplates( Environment environment )
    {
        templates.removeAllItems();
        Set<String> tempSet = new HashSet<>();
        if ( environment != null )
        {
            for ( ContainerHost c : environment.getContainerHosts() )
            {
                tempSet.add( c.getTemplateName() );
            }
            for ( String s : tempSet )
            {
                templates.addItem( s );
            }
        }
    }


    private Table createTableTemplate( final String caption )
    {
        final Table table = new Table( caption );
        table.addContainerProperty( "Container name", String.class, null );
        table.addContainerProperty( "Operation", ComboBox.class, null );
        table.addContainerProperty( "Action", Button.class, null );
        // table.addContainerProperty ("Template", String.class, null);
        // table.addContainerProperty ("Action", HorizontalLayout.class, null);
        table.setSizeFull();
        table.setPageLength( 10 );
        table.setSelectable( false );
        table.setImmediate( true );

        return table;
    }


    private void refreshUI()
    {
        if ( currentEnvironment != null && currentProfile != null )
        {
            Set<EnvironmentContainerHost> hosts = currentEnvironment.getContainerHosts();

            if ( currentTemplate == null )
            {
                populateTable( containerTable, hosts, currentProfile );
            }
            else
            {
                Set<EnvironmentContainerHost> sortedHosts = Sets.newHashSet();
                for ( final EnvironmentContainerHost host : hosts )
                {
                    if ( Objects.equals( host.getTemplateName(), currentTemplate ) )
                    {
                        sortedHosts.add( host );
                    }
                }

                populateTable( containerTable, sortedHosts, currentProfile );
            }
        }
    }


    private void populateTable( final Table containerTable, final Set<EnvironmentContainerHost> hosts,
                                final Profile currentProfile )
    {
        containerTable.removeAllItems();

        for ( final ContainerHost host : hosts )
        {
            final Button executeBtn = new Button( EXECUTE_BUTTON_CAPTION );
            addStyleNameToButtons( executeBtn );

            final ComboBox operationSelect = new ComboBox();
            operationSelect.setNullSelectionAllowed( false );
            operationSelect.setTextInputAllowed( false );

            List<Operation> operations = genericPlugin.getProfileOperations( currentProfile.getId() );

            if ( operations != null && !operations.isEmpty() )
            {
                for ( Operation operation : operations )
                {
                    operationSelect.addItem( operation );
                    operationSelect.setItemCaption( operation, operation.getOperationName() );
                }
                operationSelect.select( operations.get( 0 ) );
            }

            operationSelect.addValueChangeListener( new Property.ValueChangeListener()
            {
                @Override
                public void valueChange( Property.ValueChangeEvent event )
                {
                    currentOperation = ( Operation ) event.getProperty().getValue();
                    operationSelect.setValue( currentOperation );
                }
            } );

            containerTable.addItem( new Object[] { host.getHostname(), operationSelect, executeBtn }, null );

            executeBtn.addClickListener( new Button.ClickListener()
            {
                @Override
                public void buttonClick( final Button.ClickEvent clickEvent )
                {
                    addOutput(
                            genericPlugin.executeCommandOnContainer( host, ( Operation ) operationSelect.getValue() ) );
                }
            } );
            // addClickListenerToExecuteButton( executeBtn, host, ( Operation ) operationSelect.getValue() );
        }
    }


    private void addClickListenerToExecuteButton( final Button executeBtn, final ContainerHost host,
                                                  final Operation operation )
    {
        executeBtn.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                addOutput( genericPlugin.executeCommandOnContainer( host, operation ) );
            }
        } );
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


    private void addStyleNameToButtons( Button... buttons )
    {
        for ( Button b : buttons )
        {
            b.addStyleName( BUTTON_STYLE_NAME );
        }
    }


    private void show( String notification )
    {
        Notification notif = new Notification( notification );
        notif.setDelayMsec( 2000 );
        notif.show( Page.getCurrent() );
    }
}
