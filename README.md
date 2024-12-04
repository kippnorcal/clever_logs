# clever_logs
Download Clever Student Participation, Resource Usage, and emails reports from Clever FTP.

At this time, student emails is the only report being downloaded from Clever. To start pulling in Student Participation and Resource Usage reports, uncomment the lines in DATA_REPORTS in main.py and setup tables in Big Query with the following naming convention `base_clever_[name_of_report]`

## Dependencies:

* Python3
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://docs.docker.com/docker-for-mac/install/)

## Getting Started

### Setup Environment

1. Clone this repo

```
$ git clone https://github.com/kippnorcal/clever_logs.git
```

2. Install Docker
* Mac: https://docs.docker.com/docker-for-mac/install/
* Linux: https://docs.docker.com/install/linux/docker-ce/debian/
* Windows: https://docs.docker.com/docker-for-windows/install/

3. Create .env file with project secrets

```
# Clever variables
CLEVER_USER=
CLEVER_PW=

# Clever FTP connection
FTP_HOST=
FTP_USER=
FTP_PW=
FTP_PORT=

# Mailgun & email notification variables
MG_DOMAIN=
MG_API_URL=
MG_API_KEY=
FROM_ADDRESS=
TO_ADDRESS=

# Google Cloud Credentials
GOOGLE_APPLICATION_CREDENTIALS=
GBQ_PROJECT=
BUCKET=

# dbt variables
DBT_ACCOUNT_ID=
DBT_JOB_ID=
DBT_BASE_URL=
DBT_PERSONAL_ACCESS_TOKEN=
```

4. Create table in database using definition file in sql folder

### Build the Docker image
```
docker build -t clever .
```

### Running the Job
```
docker run --rm -it clever
```

### Run with volume mapping
```
docker run --rm -it -v ${PWD}/:/code/ clever
```

## Maintenance

* No annual rollover is required
* This script can be paused during summer break and restarted when school starts.
