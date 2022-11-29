FROM hysds/pge-base:develop-es1

LABEL description="Updates performance estimator model"

USER ops


COPY . /home/ops/verdi/ops/SOAMC-job-etc
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/SOAMC-job-etc
 
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]