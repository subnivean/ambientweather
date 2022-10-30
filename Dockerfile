FROM python:3.10

# Tip from https://stackoverflow.com/questions/63892211
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive \
    apt-get install --no-install-recommends --assume-yes \
      sqlite3

COPY requirements.txt .
RUN python -mpip install --upgrade pip
RUN pip install --no-cache-dir -r ./requirements.txt

# Easiest to just do a `. ~/.bash_aliases to get handy shortcuts
COPY bash.bash_aliases .bash_aliases

WORKDIR /app
COPY ./src .

# Create storage directory for database
RUN mkdir /data

CMD ["python", "get_ambientweather_data.py"]
