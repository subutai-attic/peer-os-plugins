# Subutai Social Plugins repository

This repository contains source code of plugins for Subutai Social Console.
This is a multi-module Maven Java project.

These plugins are available in Subutai Social Console's Bazaar and can be installed directly from there.

## Building the project

###Prerequisites

- Latest Subutai Social Console

  [Installation](https://github.com/subutai-io/base/edit/dev/management)
  
###Build steps

- Clone the project by using:

    `git clone https://github.com/subutai-io/plugins.git`

- Start maven build:

    ```bash
    cd plugins
    mvn clean install
    ```
    
- You can pull all kar files into `kar` folder by executing script

    ```bash
    bash karPuller.sh
    ```
- Deploy file using Bazaar's Advanced tab.

    - Go to Bazaar -> Advanced on your Subutai Social Console and press "Upload new plugin"
    
    ![alt tag](http://i.imgur.com/h1ebTfm.png)
    
    - Fill all fields and upload
    
    ![alt tag](http://i.imgur.com/bEYjMCb.png)
