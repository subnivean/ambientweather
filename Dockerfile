FROM py39-pandas134

WORKDIR /app

COPY requirements.txt ..
RUN python -mpip install --upgrade pip
RUN pip install --no-cache-dir -r ../requirements.txt

COPY ./src .

# Create storage directory for database
RUN mkdir /data

#ENV TZ=America/New_York
#RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ["python", "get_ambientweather_data.py"]
