package io.subutai.plugin.generic.ui.wizard;


import com.vaadin.annotations.Theme;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.server.Page;
import com.vaadin.ui.*;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.Profile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashSet;
import java.util.Set;

@Theme( "valo" )
public class ManageContainersStep extends Panel
{
	private static final Logger LOG = LoggerFactory.getLogger(Wizard.class.getName());
	private Profile currentProfile;
	private Environment currentEnvironment;
	private String currentTemplate;
	private Table containerTable = new Table ("Containers");


	private void populateTable()
	{
		for (ContainerHost c : this.currentEnvironment.getContainerHosts())
		{
			if (c.getTemplateName() == this.currentTemplate)
			{
				ComboBox operations = new ComboBox();
				operations.setNullSelectionAllowed(false);
				operations.setTextInputAllowed(false);
				for (String o : this.currentProfile.getOperations())
				{
					operations.addItem(o);
				}
				operations.setValue (this.currentProfile.getOperations().get(0));
				Button execute = new Button ("Execute");
				execute.addClickListener (new Button.ClickListener()
				{
					@Override
					public void buttonClick (final Button.ClickEvent clickEvent)
					{
						// TODO: execute command defined by "operations" ComboBox
					}
				});
				Object newItemId = containerTable.addItem();
				Item row = containerTable.getItem (newItemId);
				LOG.info ("Adding container: " + c.getHostname());
				row.getItemProperty ("Container name").setValue (c.getHostname());
				row.getItemProperty ("Operation").setValue (operations);
				row.getItemProperty ("Action").setValue (execute);
			}
		}
	}


	private void changeProfile()
	{
		if (currentProfile != null && !currentProfile.getOperations().isEmpty())
		{
			for (Object o : containerTable.getItemIds())
			{
				ComboBox operations = new ComboBox();
				operations.setNullSelectionAllowed(false);
				operations.setTextInputAllowed(false);
				for (String op : currentProfile.getOperations())
				{
					operations.addItem(op);
				}
				operations.setValue (currentProfile.getOperations().get(0));
				if (containerTable.getItem (o).getItemProperty("Operation").getValue() != null)
				{
					containerTable.getItem(o).getItemProperty("Operation").setValue(operations);
				}
			}
		}
		else
		{
			Notification notif = new Notification("Please select profile");
			notif.setDelayMsec(2000);
			notif.show(Page.getCurrent());
		}
	}





	public ManageContainersStep (final Wizard wizard)
	{
		this.setSizeFull();
		VerticalLayout content = new VerticalLayout();
		Label title = new Label ("Manage containers");
		HorizontalLayout comboGrid = new HorizontalLayout();
		comboGrid.setWidth ("100%");
		comboGrid.setSpacing(true);
		final ComboBox envSelect = new ComboBox ("Environment");
		envSelect.setNullSelectionAllowed(false);
		envSelect.setTextInputAllowed(false);
		boolean assigned = false;
		for (Environment e : wizard.getManager().getEnvironments())
		{
			LOG.info (e.getName());
			envSelect.addItem(e.getName());
			if (!assigned)
			{
				currentEnvironment = e;
				envSelect.setValue (e.getName());
				assigned = true;
			}
		}
		final ComboBox profileSelect = new ComboBox ("Profile");
		profileSelect.setNullSelectionAllowed(false);
		profileSelect.setTextInputAllowed(false);
		assigned = false;
		this.currentProfile = null;
		for (Profile p : wizard.getConfig().getProfiles())
		{
			if (!p.getOperations().isEmpty())
			{
				if (!assigned)
				{
					this.currentProfile = p;
					assigned = true;
				}
				profileSelect.addItem(p.getName());
			}
		}
		profileSelect.setValue (wizard.getConfig().getProfiles().get(0).getName());

		final ComboBox templates = new ComboBox ("Template");
		templates.setNullSelectionAllowed (false);
		templates.setTextInputAllowed(false);
		final Set <String> tempSet = new HashSet<>();
		for (ContainerHost c : currentEnvironment.getContainerHosts())
		{
			LOG.info ("Template: " + c.getTemplateName());
			tempSet.add(c.getTemplateName());
		}
		assigned = false;
		for (String s : tempSet)
		{
			templates.addItem (s);
			if (!assigned)
			{
				templates.setValue (s);
				currentTemplate = s;
				assigned = true;
			}
		}

		comboGrid.addComponent(envSelect);
		comboGrid.addComponent (templates);
		comboGrid.addComponent (profileSelect);
		containerTable.setWidth ("100%");
		containerTable.setHeight ("200px");
		containerTable.addContainerProperty ("Container name", String.class, null);
		containerTable.addContainerProperty ("Operation", ComboBox.class, null);
		containerTable.addContainerProperty ("Action", Button.class, null);
		content.addComponent (title);
		content.addComponent (comboGrid);
		content.addComponent (containerTable);
		if (currentProfile != null)
		{
			this.populateTable();
		}
		templates.addValueChangeListener (new Property.ValueChangeListener()
		{
			@Override
			public void valueChange (Property.ValueChangeEvent event)
			{
				currentTemplate = event.getProperty().toString();
				containerTable.removeAllItems();
				populateTable();
			}
		});
		profileSelect.addValueChangeListener (new Property.ValueChangeListener()
		{
			@Override
			public void valueChange (Property.ValueChangeEvent event)
			{
				String profileName = event.getProperty().toString();
				currentProfile = wizard.getConfig().findProfile(profileName);
				changeProfile();
			}
		});
		envSelect.addValueChangeListener (new Property.ValueChangeListener()
		{
			@Override
			public void valueChange(Property.ValueChangeEvent event)
			{
				boolean found = false;
				String envName = event.getProperty().toString();
				for (Environment e : wizard.getManager().getEnvironments())
				{
					if (e.getName() == envName)
					{
						found = true;
						currentEnvironment = e;
						break;
					}
				}
				if (found)
				{
					templates.removeAllItems();
					tempSet.clear();
					for (ContainerHost c : currentEnvironment.getContainerHosts())
					{
						LOG.info ("Template: " + c.getTemplateName());
						tempSet.add (c.getTemplateName());
					}
					found = false;
					for (String s : tempSet)
					{
						templates.addItem (s);
						if (!found)
						{
							templates.setValue (s);
							currentTemplate = s;
							found = true;
						}
					}
					containerTable.removeAllItems();
					populateTable();
				}
				else
				{
					Notification notif = new Notification ("Please select environment");
					notif.setDelayMsec(2000);
					notif.show(Page.getCurrent());
				}
			}
		});
		Button back = new Button ("Back");
		back.addClickListener (new Button.ClickListener()
		{
			@Override
			public void buttonClick (final Button.ClickEvent clickEvent)
			{
				wizard.changeWindow (0);
				wizard.putForm();
			}
		});
		content.addComponent (back);
		TextArea output = new TextArea ("Output");
		output.setSizeFull();
		output.setRows (15);
		// output.setReadOnly (true); TODO: set output readonly without making its design like a label
		content.addComponent (output);
		this.setContent(content);
	}

}
