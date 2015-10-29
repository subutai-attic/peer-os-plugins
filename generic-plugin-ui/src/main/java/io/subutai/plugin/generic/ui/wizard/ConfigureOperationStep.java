package io.subutai.plugin.generic.ui.wizard;


import com.vaadin.annotations.Theme;
import com.vaadin.data.Item;
import com.vaadin.data.Property;
import com.vaadin.event.FieldEvents;
import com.vaadin.server.Page;
import com.vaadin.ui.*;
//import io.subutai.common.protocol.Template;
import io.subutai.plugin.generic.api.Profile;


@Theme ("valo")
public class ConfigureOperationStep extends Panel
{
	private Profile currentProfile;
	private TextField cwd = new TextField ("cwd");
	private FloatField timeOut = new FloatField ("Timeout");
	private CheckBox daemon = new CheckBox ("Daemon");
	private Table operationTable = new Table ("Operations");
	private Wizard wizard;

	class FloatField extends TextField implements FieldEvents.TextChangeListener
	{
		private String lastValue;

		public FloatField (String caption)
		{
			super (caption);
			this.setImmediate (true);
			this.setTextChangeEventMode (TextChangeEventMode.EAGER);
			this.addTextChangeListener (this);
		}

		@Override
		public void textChange (FieldEvents.TextChangeEvent event) {
			String text = event.getText();
			try {
				new Float (text);
				this.lastValue = text;
			} catch (NumberFormatException e) {
				this.setValue (lastValue);
			}
		}
	}

	class EditButton extends Button
	{
		private TextField cwd = new TextField ("cwd");
		private FloatField timeOut = new FloatField ("Timeout");
		private CheckBox daemon = new CheckBox ("Daemon");
		public EditButton (String caption, Object itemId)
		{
			super (caption);
			cwd.setValue ("/");
			cwd.setData ("/");
			timeOut.setValue ("30");
			timeOut.setData ("30");
			daemon.setValue (false);
			daemon.setData (false);
			this.setData (itemId);
			this.addClickListener (new Button.ClickListener()
			{
				@Override
				public void buttonClick (final Button.ClickEvent clickEvent)
				{
					final Window subWindow = new Window ("Edit operation");
					subWindow.setClosable (false);
					subWindow.addStyleName ("default");
					subWindow.center();



					VerticalLayout subContent = new VerticalLayout();
					subContent.setSpacing (true);
					subContent.setMargin (true);


					HorizontalLayout editInfo = new HorizontalLayout();
					editInfo.setSpacing (true);
					editInfo.setWidth ("100%");


					final TextField editName = new TextField ("Edit operation name");
					editName.setValue (operationTable.getItem (getData()).getItemProperty ("Operation name").toString());


					final TextField editCommand = new TextField ("Edit command");
					editCommand.setValue (operationTable.getItem (getData()).getItemProperty ("Command").toString());


					/*final ComboBox editTemplate = new ComboBox ("Edit template");
					for (Template t : wizard.getRegistry().getAllTemplates())
					{
						editTemplate.addItem (t.getTemplateName());
					}
					editTemplate.setValue (operationTable.getItem (edit.getData()).getItemProperty ("Template").toString());*/


					HorizontalLayout buttons = new HorizontalLayout();
					buttons.setSpacing (true);
					buttons.setWidth ("100%");


					Button options = new Button ("Options");
					options.addClickListener (new Button.ClickListener()
					{
						@Override
						public void buttonClick (Button.ClickEvent event)
						{
							final Window subWindow = new Window("Edit operation");
							subWindow.setClosable(false);
							subWindow.addStyleName("default");
							subWindow.center();
							VerticalLayout subContent = new VerticalLayout();
							subContent.setSpacing(true);
							subContent.setMargin(true);
							HorizontalLayout fields = new HorizontalLayout();
							fields.setSpacing(true);
							fields.addComponent (cwd);
							fields.addComponent (timeOut);
							fields.setComponentAlignment (cwd, Alignment.BOTTOM_LEFT);
							fields.setComponentAlignment (timeOut, Alignment.BOTTOM_LEFT);
							HorizontalLayout buttonGrid = new HorizontalLayout();
							buttonGrid.setSpacing (true);
							Button back = new Button ("Back");
							back.setSizeFull();
							back.addClickListener (new Button.ClickListener()
							{
								@Override
								public void buttonClick (Button.ClickEvent event)
								{
									cwd.setValue ((String)cwd.getData());
									timeOut.setValue ((String)timeOut.getData());
									daemon.setValue ((boolean)daemon.getData());
									subWindow.close();
								}
							});
							Button save = new Button("Save");
							save.setSizeFull();
							save.addClickListener(new Button.ClickListener()
							{
								@Override
								public void buttonClick(Button.ClickEvent event)
								{
									cwd.setData (cwd.getValue());
									timeOut.setData (timeOut.getValue());
									daemon.setData (daemon.getValue());
									subWindow.close();
								}
							});
							buttonGrid.addComponent (back);
							buttonGrid.addComponent (save);
							buttonGrid.setComponentAlignment (back, Alignment.BOTTOM_CENTER);
							buttonGrid.setComponentAlignment (save, Alignment.BOTTOM_CENTER);
							subContent.addComponent (fields);
							subContent.addComponent (daemon);
							subContent.addComponent (buttonGrid);
							subContent.setComponentAlignment (daemon, Alignment.BOTTOM_LEFT);
							subContent.setComponentAlignment (buttonGrid, Alignment.BOTTOM_CENTER);
							subWindow.setContent (subContent);
							UI.getCurrent().addWindow(subWindow);
						}
					});

					Button cancel = new Button ("Cancel");
					cancel.addClickListener (new Button.ClickListener()
					{
						@Override
						public void buttonClick (Button.ClickEvent clickEvent)
						{
							subWindow.close();
						}
					});


					Button finalEdit = new Button ("Edit");
					finalEdit.addClickListener (new Button.ClickListener()
					{
						@Override
						public void buttonClick (Button.ClickEvent clickEvent)
						{
							if (editName.getValue().isEmpty())
							{
								Notification notif = new Notification ("Please enter operation name");
								notif.setDelayMsec (2000);
								notif.show (Page.getCurrent());
							}
							else if (editCommand.getValue().isEmpty())
							{
								Notification notif = new Notification ("Please enter command");
								notif.setDelayMsec (2000);
								notif.show (Page.getCurrent());
							}
							else
							{
								boolean exists = false;
								for (int i = 0; i < currentProfile.getOperations().size(); ++i)
								{
									if (currentProfile.getOperations().get(i).equals (editName.getValue()))
									{
										Notification notif = new Notification ("Command with such operation name already exists");
										notif.setDelayMsec (2000);
										notif.show (Page.getCurrent());
										exists = true;
										break;
									} else if (currentProfile.getCommands().get(i).equals (editCommand.getValue()))
									{
										Notification notif = new Notification ("Operation with such command already exists");
										notif.setDelayMsec (2000);
										notif.show (Page.getCurrent());
										exists = true;
										break;
									}
								}
								if (!exists)
								{
									currentProfile.deleteOperation (operationTable.getItem (getData()).getItemProperty ("Operation name").getValue().toString());
									currentProfile.addOperation (editName.getValue(), editCommand.getValue()/*, editTemplate.getValue().toString()*/, cwd.getValue(), new Float (timeOut.getValue()), new Boolean (daemon.getValue()));
									wizard.getConfig().replaceProfile (currentProfile);
									operationTable.getItem (getData()).getItemProperty ("Operation name").setValue (editName.getValue());
									operationTable.getItem (getData()).getItemProperty ("Command").setValue (editCommand.getValue());
									//operationTable.getItem (edit.getData()).getItemProperty ("Template").setValue (editTemplate.getValue().toString());
									subWindow.close();
									Notification notif = new Notification ("Operation was changed successfully");
									notif.setDelayMsec (2000);
									notif.show (Page.getCurrent());
								}
							}
						}
					});


					editInfo.addComponent (editName);
					editInfo.addComponent (editCommand);
					//editInfo.addComponent (editTemplate);


					buttons.addComponent (cancel);
					buttons.addComponent (finalEdit);
					buttons.setComponentAlignment (cancel, Alignment.BOTTOM_CENTER);
					buttons.setComponentAlignment (finalEdit, Alignment.BOTTOM_CENTER);


					subContent.addComponent (editInfo);
					subContent.addComponent (buttons);


					subWindow.setContent (subContent);
					UI.getCurrent().addWindow (subWindow);
				}
			});
		}
	}




	private void addRow (final String newOperation, String newCommand/*, String newTemplate*/)
	{
		Object newItemId = operationTable.addItem();
		Item row = operationTable.getItem (newItemId);
		row.getItemProperty ("Operation name").setValue (newOperation);
		row.getItemProperty ("Command").setValue (newCommand);
		//row.getItemProperty ("Template").setValue (newTemplate);
		HorizontalLayout buttonGrid = new HorizontalLayout();
		EditButton edit = new EditButton ("Edit", newItemId);
		final Button delete = new Button ("Delete");
		delete.setData (newItemId);
		delete.addClickListener (new Button.ClickListener()
		{
			@Override
			public void buttonClick (final Button.ClickEvent clickEvent)
			{
				currentProfile.deleteOperation (newOperation);
				wizard.getConfig().replaceProfile (currentProfile);
				operationTable.removeItem(delete.getData());
			}
		});
		buttonGrid.addComponent (edit);
		buttonGrid.addComponent (delete);
		row.getItemProperty ("Actions").setValue (buttonGrid);
	}


	public ConfigureOperationStep (final Wizard wizard)
	{
		this.wizard = wizard;
		VerticalLayout content = new VerticalLayout();
		Label title = new Label ("Configure operations");
		final ComboBox profileSelect = new ComboBox ("Profile");
		profileSelect.setNullSelectionAllowed (false);
		profileSelect.setTextInputAllowed (false);
		boolean assigned = false;
		this.currentProfile = null;
		for (Profile p : wizard.getConfig().getProfiles())
		{
			if (!assigned)
			{
				this.currentProfile = p;
				assigned = true;
			}
			profileSelect.addItem (p.getName());
		}
		profileSelect.setValue (wizard.getConfig().getProfiles().get (0).getName());
		HorizontalLayout fieldGrid = new HorizontalLayout();
		fieldGrid.setSpacing (true);
		fieldGrid.setWidth ("100%");
		final TextField newName = new TextField ("New operation name (maximum 10 characters)");
		newName.setInputPrompt ("Enter new operation name");
		newName.setRequired (true);
		final TextField newCommand = new TextField ("New command");
		newCommand.setRequired (true);
		newCommand.setInputPrompt ("Enter new command");

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


		cwd.setValue ("/");
		cwd.setData ("/");
		timeOut.setValue ("30");
		timeOut.setData ("30");
		daemon.setValue (false);
		daemon.setData (false);
		Button options = new Button ("Options");


		Button addOperation = new Button ("Add operation");
		fieldGrid.addComponent (newName);
		fieldGrid.addComponent (newCommand);
		//fieldGrid.addComponent (templates);
		fieldGrid.addComponent (options);
		fieldGrid.addComponent (addOperation);
		fieldGrid.setComponentAlignment (newName, Alignment.BOTTOM_LEFT);
		fieldGrid.setComponentAlignment (newCommand, Alignment.BOTTOM_LEFT);
		//fieldGrid.setComponentAlignment (templates, Alignment.BOTTOM_LEFT);
		fieldGrid.setComponentAlignment (options, Alignment.BOTTOM_LEFT);
		fieldGrid.setComponentAlignment (addOperation, Alignment.BOTTOM_LEFT);

		operationTable.setWidth ("100%");
		operationTable.setHeight ("200px");
		operationTable.addContainerProperty ("Operation name", String.class, null);
		operationTable.addContainerProperty ("Command", String.class, null);
		//operationTable.addContainerProperty ("Template", String.class, null);
		operationTable.addContainerProperty ("Actions", HorizontalLayout.class, null);
		content.addComponent (title);
		content.addComponent (profileSelect);
		content.addComponent (fieldGrid);
		content.addComponent (operationTable);
		if (currentProfile != null)
		{
			for (int i = 0; i < currentProfile.getOperations().size(); ++i)
			{
				this.addRow (currentProfile.getOperations().get (i), currentProfile.getCommands().get (i)/*, currentProfile.getTemplates().get (i)*/);
			}
		}
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
		content.addComponent(back);
		profileSelect.addValueChangeListener (new Property.ValueChangeListener()
		{
			@Override
			public void valueChange (Property.ValueChangeEvent event)
			{
				String profileName = event.getProperty().toString();
				currentProfile = wizard.getConfig().findProfile (profileName);
				operationTable.removeAllItems();
				if (currentProfile != null)
				{
					for (int i = 0; i < currentProfile.getOperations().size(); ++i)
					{
						addRow (currentProfile.getOperations().get (i), currentProfile.getCommands().get (i)/*, currentProfile.getTemplates().get (i)*/);
					}
				}
			}
		});


		options.addClickListener (new Button.ClickListener()
		{
			@Override
			public void buttonClick (Button.ClickEvent event)
			{
				final Window subWindow = new Window("Edit operation");
				subWindow.setClosable(false);
				subWindow.addStyleName("default");
				subWindow.center();
				VerticalLayout subContent = new VerticalLayout();
				subContent.setSpacing(true);
				subContent.setMargin(true);
				HorizontalLayout fields = new HorizontalLayout();
				fields.setSpacing(true);
				fields.addComponent (cwd);
				fields.addComponent (timeOut);
				fields.setComponentAlignment (cwd, Alignment.BOTTOM_LEFT);
				fields.setComponentAlignment (timeOut, Alignment.BOTTOM_LEFT);
				HorizontalLayout buttonGrid = new HorizontalLayout();
				buttonGrid.setSpacing (true);
				Button back = new Button ("Back");
				back.setSizeFull();
				back.addClickListener (new Button.ClickListener()
				{
					@Override
					public void buttonClick (Button.ClickEvent event)
					{
						cwd.setValue ((String)cwd.getData());
						timeOut.setValue ((String)timeOut.getData());
						daemon.setValue ((boolean)daemon.getData());
						subWindow.close();
					}
				});
				Button save = new Button("Save");
				save.setSizeFull();
				save.addClickListener(new Button.ClickListener()
				{
					@Override
					public void buttonClick(Button.ClickEvent event)
					{
						cwd.setData (cwd.getValue());
						timeOut.setData (timeOut.getValue());
						daemon.setData (daemon.getValue());
						subWindow.close();
					}
				});
				buttonGrid.addComponent (back);
				buttonGrid.addComponent (save);
				buttonGrid.setComponentAlignment (back, Alignment.BOTTOM_CENTER);
				buttonGrid.setComponentAlignment (save, Alignment.BOTTOM_CENTER);
				subContent.addComponent (fields);
				subContent.addComponent (daemon);
				subContent.addComponent (buttonGrid);
				subContent.setComponentAlignment (daemon, Alignment.BOTTOM_LEFT);
				subContent.setComponentAlignment (buttonGrid, Alignment.BOTTOM_CENTER);
				subWindow.setContent (subContent);
				UI.getCurrent().addWindow(subWindow);
			}
		});


		addOperation.addClickListener (new Button.ClickListener()
		{
			public void buttonClick (final Button.ClickEvent clickEvent)
			{
				if (currentProfile != null)
				{
					if (newName.getValue().isEmpty())
					{
						Notification notif = new Notification ("Please enter operation name");
						notif.setDelayMsec (2000);
						notif.show (Page.getCurrent());
					}
					else if (newCommand.getValue().isEmpty())
					{
						Notification notif = new Notification ("Please enter command");
						notif.setDelayMsec (2000);
						notif.show (Page.getCurrent());
					}
					else
					{
						boolean exists = false;
						for (int i = 0; i < currentProfile.getOperations().size(); ++i)
						{
							if (currentProfile.getOperations().get (i).equals (newName.getValue()))
							{
								Notification notif = new Notification ("Command with such operation name already exists");
								notif.setDelayMsec (2000);
								notif.show (Page.getCurrent());
								exists = true;
								break;
							}
							else if (currentProfile.getCommands().get (i).equals (newCommand.getValue()))
							{
								Notification notif = new Notification ("Operation with such command already exists");
								notif.setDelayMsec (2000);
								notif.show (Page.getCurrent());
								exists = true;
								break;
							}
						}
						if (!exists)
						{
							currentProfile.addOperation (newName.getValue(), newCommand.getValue()/*, newTemplateString*/, cwd.getValue(), new Float (timeOut.getValue()), new Boolean (daemon.getValue()));
							wizard.getConfig().replaceProfile (currentProfile);
							addRow(newName.getValue(), newCommand.getValue()/*, templates.getValue().toString()*/);
							newName.setValue ("");
							newCommand.setValue ("");
							cwd.setValue ("/");
							timeOut.setValue ("30");
							daemon.setValue (false);
						}
					}
				}
				else
				{
					Notification notif = new Notification ("Please select profile");
					notif.setDelayMsec (2000);
					notif.show (Page.getCurrent());
				}
			}
		});
		this.setContent (content);
	}
}
