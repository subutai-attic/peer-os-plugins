# Subutai Social Plugins repository

This repository contains source code of plugins for Subutai Social Console.
This is a multi-module Maven Java project.

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
