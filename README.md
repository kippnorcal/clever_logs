# clever_logs
Selenium script to get Clever login logs 


## Dependencies:

* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)

## Getting Started

### Setup Environment

1. Clone this repo

```
$ git clone https://github.com/kipp-bayarea/clever_logs.git
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

# Mailer config
SENDER_EMAIL=
SENDER_PWD=
EMAIL_SERVER=
EMAIL_PORT=
RECIPIENT_EMAIL=
```

4. Add credentials.json file from Google Developer console

5. Add settings.yaml file for connecting to Google Drive API

### Build the Docker image
```
docker build -t clever_logs .
```

### Running the Job
```
docker run -it clever_logs
```

### Run with volume mapping
```
docker run --rm -it -v ${PWD}/:/code/ clever_logs
```