# SQLprobes

## Description
SQLprobes is the collection of driver programs for performance experiments against a DBMS.
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
Thereafter you can checkout the SQLprobes repository in a local directory.
```
  git clone git@github.com:mlkersten/sqlprobes.git
  cd sqlprobes
  pipenv install --ignore-pipfile
  cd <path/to/Pipenv.lock>
  pipenv install --ignore-pipfile
  cd <path/to/Pipenv>
  pipenv shell
```

Alternatively, import the missing packages in a fresh Python virtual environment.
```
pip install --user requests
pip install --user pymonetdb
pip install --user configparser
pip install --user psycopg2-binary
```

## Usage
The driver program reads a configuration file from the directory in which
it is started. A sample is included for inspiration. Make sure that the
IP address for the SQLscalpel server is properly set.
Furthermore, the host name is a nickname by which is it known in the
SQLscalpel server.

SQLprobe is started with a target name, which corresponds to a section
in the configuration file. That section may contain DBMS specific
parameter settings.

It can also be used to limit
the queries acceptable for execution. Either queries for a specific
project are ran, or even specific experiments within a project.
The most generous way is to accept any task, as indicated with
the pseudo project 'all' and pseudo experiment 'all'.

Once started, it continuously requests queries from the SQLscalpel
server to contribute with execution times (in ms) of a number of runs.

## Policy
Recall that all experiments should be ran in good faith with
the intentions provided by the project owner. In particular,
the parameter setting of the DBMS and OS should comply with
those recorded for them in the SQLscalpel website. The project
owner can discard any suspicious result or decline contributions.


## Credits
SQLprobes and SQLscalpel are orginally developed by Martin Kersten.

## License

This Source Code is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
repository, you can obtain one at http://mozilla.org/MPL/2.0/.

&copy; 2018- MonetDB Solutions B.V.
