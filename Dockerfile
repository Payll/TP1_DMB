FROM python

WORKDIR /usr/src/app
COPY . .
RUN pip install --upgrade pip
RUN pip install PySpark

CMD ["python", "Part5.py"]
