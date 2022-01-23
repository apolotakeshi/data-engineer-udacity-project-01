FROM jupyter/scipy-notebook:latest

ENV NB_UID=1000
ENV NB_GID=100
ENV NB_USER=jovyan
ENV NB_HOME=/home/jovyan

COPY ./requirements.txt /usr/bin/make/requirements.txt

RUN pip install -r /usr/bin/make/requirements.txt --quiet

USER root
RUN apt-get update --yes && \
    apt-get upgrade --yes && \
    apt-get install --yes make

USER ${NB_USER}

COPY --chown=${NB_USER}:${NB_GID} ./data ${NB_HOME}/data

COPY --chown=${NB_USER}:${NB_GID} ./table_creation/ ${NB_HOME}/table_creation/

COPY --chown=${NB_USER}:${NB_GID} ./Makefile ${NB_HOME}/Makefile

COPY --chown=${NB_USER}:${NB_GID} ./scripts/templating.py ${NB_HOME}/scripts/templating.py

COPY --chown=${NB_USER}:${NB_GID} ./notebooks/ ${NB_HOME}/notebooks/

COPY --chown=${NB_USER}:${NB_GID} ./etl.py ${NB_HOME}/etl.py

COPY --chown=${NB_USER}:${NB_GID} ./README.md ${NB_HOME}/README.md
