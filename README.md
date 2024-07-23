# Data retrieving ecommerce

Containers jobs that simplify commons task in Multivende website.

## Configuration

All containers/jobs must include a `config.py` file with configuration variables in order to work properly. An example configuration file named `test.py` is in each directory.

## First steps

1. Change the configurations files with current values
2. Move to the directory with the job
3. Run `python update_{job}.py`

## Other configurations

* Optionally, you can build docker's container to deploy the jobs. Each folder contains it's `Dockerfile` to deploy with a lightweight python version and system.

# Note

Currently, I am finished the basis of the App. It can download from multivende all the data to a desired DB, as explained.