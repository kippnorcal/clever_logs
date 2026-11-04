FROM --platform=linux/amd64 python:3.14
WORKDIR /code
RUN pip install pipenv
COPY . .
RUN pipenv install --skip-lock
ENTRYPOINT ["pipenv", "run", "python", "main.py"]