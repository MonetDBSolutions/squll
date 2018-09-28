# Sqalpel

## Description
Sqalpel is the collection of driver programs for performance experiments against a DBMS.
For background information see the paper [Discrimative Performance Benchmarking](https://www.cwi.nl/~mk/scalpel.pdf).

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
Thereafter you can checkout the Sqalpel repository in a local directory.
```
  git clone git@github.com:MonetDBSolutions/sqalpel.git
  cd sqalpel
  pipenv install --ignore-pipfile
  pipenv shell
```

## Usage
The driver program reads a configuration file 'sqalpel.conf' from the directory in which
it is started. A sample is included for inspiration and documentation. Make sure that the
global parameters are set correctly.

Sqalpel is started with a target name, which corresponds to a section
in the configuration file. That section may contain DBMS specific
parameter settings.

It can also be used to limit the queries acceptable for execution. 
Either queries for a specific project are ran, or even specific experiments within a project.
The most generous way is to accept any task, as indicated with
the pseudo project '*' and pseudo experiment '*'.

Once started, it continuously requests queries from the SQLscalpel
server to contribute with execution times (in ms) of a number of runs.

## Policy
Recall that all experiments should be ran in good faith with
the intentions provided by the project owner. In particular,
the parameter setting of the DBMS and OS should comply with
those recorded for them in the SQLscalpel website. The project
owner can discard any suspicious result or decline contributions.


## Credits
Sqalpel.py and Sqalpel.io are orginally developed by Martin Kersten.

## License

This Source Code is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
repository, you can obtain one at http://mozilla.org/MPL/2.0/.

&copy; 2018- MonetDB Solutions B.V.
