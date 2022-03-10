# clever_logs
Download Clever Student Participation, Resource Usage, and emails reports from Clever FTP.


## Dependencies:

* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)

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
# Database variables
DB_TYPE=
DB_SERVER=
DB=
DB_SCHEMA=
DB_USER=
DB_PWD=

# Clever variables
CLEVER_USER=
CLEVER_PW=

# Clever FTP connection
FTP_HOST=
FTP_USER=
FTP_PW=
FTP_PORT=

# Mailer config
SENDER_EMAIL=
SENDER_PWD=
EMAIL_SERVER=
EMAIL_PORT=
RECIPIENT_EMAIL=
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
