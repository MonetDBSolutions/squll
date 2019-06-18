# SQULL

## Description
Squll.py is a generic driver program for performance experiments against a DBMS.

## Installation
Setting up the environment involves a few simple steps.
First make sure you have pipenv installed on your machine and that the pipenv command can be found in your $PATH.
If you do a user installation, you will need to add the right folder to your PATH variable.

```
  pip3 install --user pipenv
  PYTHON_BIN_PATH="$(python3 -m site --user-base)/bin"
  export PATH="$PATH:$PYTHON_BIN_PATH"
  which pipenv
```
Thereafter you can checkout the Squll repository in a local directory.
```
  git clone git@github.com:sqalpel/squll.git
  cd squll
  pipenv install --ignore-pipfile
  pipenv shell
```

## Usage
The driver program reads a configuration file 'sqalpel.yaml' from the directory in which
it is started. A sample is included for inspiration and documentation. Make sure that the
global parameters are set correctly.

Squll is started with a section name in the configuration file. That section may contain DBMS specific
parameter settings to establish a client connection.

Once started, squll.py continuously requests queries from the Sqalpel
server to contribute with execution times (in ms) of a number of runs.

## Drivers section
This contains the core programs squll.py and the supportive files
repository.py and sqalpel.py. The remainder are drivers
for some well-knonw database. The may require additional arguments.

## Policy
Recall that all experiments should be ran in good faith with
the intentions provided by the project owner. In particular,
the parameter setting of the DBMS and OS should comply with
those recorded for them in the SQALPEL website. The project
owner can discard any suspicious result or decline contributions.

## TODO
The squll.py drivers should be upgraded. It is in flux.

## Credits
squll.py is orginally developed by Martin Kersten.

## License

This Source Code is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
repository, you can obtain one at http://mozilla.org/MPL/2.0/.

&copy; 2019- Stichting Sqalpel
