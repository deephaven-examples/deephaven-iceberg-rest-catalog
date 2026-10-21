FROM tabulario/spark-iceberg

# The base image points the demo catalog at a MinIO service. Point it at RustFS instead.
RUN sed -i 's|http://minio:9000|http://rustfs:9000|' /opt/spark/conf/spark-defaults.conf

COPY ./spark/iceberg.ipynb /home/iceberg/notebooks/
COPY ./entrypoint2.sh /home/entrypoint2.sh

ENTRYPOINT /home/entrypoint2.sh
