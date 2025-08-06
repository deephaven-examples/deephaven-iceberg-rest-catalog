FROM tabulario/spark-iceberg

COPY ./spark/iceberg.ipynb /home/iceberg/notebooks/
COPY ./entrypoint2.sh /home/entrypoint2.sh

ENTRYPOINT /home/entrypoint2.sh
