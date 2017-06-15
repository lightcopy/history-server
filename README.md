# history-server
Update for Spark history server

## Build instructions

### Requirements
- `python` == 2.7.x
- `pip` latest

You can download official Python distribution from https://www.python.org/downloads/. Note that `pip`
might already be included with Python, otherwise refer to the documentation on how to set it up.

### Install
If you do not have `virtualenv` installed, run this:
```
$ pip install virtualenv
```
Clone repository and set up `virtualenv`:
```shell
$ git clone https://github.com/lightcopy/history-server
$ cd history-server
$ virtualenv venv
```

Now you can use `bin/python` and `bin/pip` to access python and pip of virtual environment. Run
following commands to install dependencies.
```shell
$ bin/pip install -r requirements.txt --upgrade --target lib/
```

### Run linters
Run `bin/pylint` from project directory, you should provide `--python` flag for python binaries you
want to use.
```shell
# For example, use bin/python for 'virtualenv'
bin/pylint --python=bin/python
```

### Run tests
Run `bin/coverage` from project directory, you should also specify `--python` flag to provide link
to your python binaries. This will run Python tests and print overall coverage.
```shell
# For example, use bin/python for 'virtualenv'
$ bin/coverage --python=bin/python
```
