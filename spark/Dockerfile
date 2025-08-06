FROM tabulario/spark-iceberg

RUN sleep 3

WORKDIR /home/iceberg/notebooks

RUN jupyter nbconvert --to notebook --execute PyIceberg\ -\ Getting\ Started.ipynb
