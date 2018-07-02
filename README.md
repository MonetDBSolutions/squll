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
  git clone git@github.com:MonetDBSolutions/sqlprobes.git
  cd sqlprobes
  pipenv install --ignore-pipfile
  cd <path/to/Pipenv.lock>
  pipenv install --ignore-pipfile
  cd <path/to/Pipenv>
  pipenv shell
```

Alternatively for anaconda users try:
export PYTHONPATH=$HOME/anaconda/lib/python3.6/site-packages:$PYTHONPATH
export PATH=$HOME/anaconda/bin:$PATH

Note, the IP address for the server should be configured in app/config.py
and in the scalpel.conf file.
The default is the localhost.

## Usage
The driver program reads a configuration file from the directory in which
it is started. A sample is included for inspiration. Make sure that the
IP address for the SQLscalpel server is properly set.
Furthermore, the host name is a nickname by which is it known in the
SQLscalpel server.

## Credits
SQLprobes is orginally developed by Martin Kersten.

## License

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
repository, you can obtain one at http://mozilla.org/MPL/2.0/.

&copy; 2018- MonetDB Solutions B.V.
