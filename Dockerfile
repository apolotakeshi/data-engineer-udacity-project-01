FROM jupyter/scipy-notebook:latest

ENV NB_UID=1000
ENV NB_GID=100
ENV NB_USER=jovyan

COPY ./requirements.txt /usr/bin/make/requirements.txt

RUN pip install -r /usr/bin/make/requirements.txt --quiet

USER root
RUN apt-get update --yes && \
    apt-get upgrade --yes && \
    apt-get install --yes make

USER ${NB_USER}

COPY --chown=${NB_USER}:${NB_GID} ./data /home/jovyan/data

COPY --chown=${NB_USER}:${NB_GID} ./table_creation/ /home/jovyan/table_creation/

COPY --chown=${NB_USER}:${NB_GID} ./Makefile /home/jovyan/Makefile

COPY --chown=${NB_USER}:${NB_GID} ./scripts/templating.py /home/jovyan/scripts/templating.py

COPY --chown=${NB_USER}:${NB_GID} ./notebooks/ /home/jovyan/notebooks/

COPY --chown=${NB_USER}:${NB_GID} ./etl.py /home/jovyan/etl.py

COPY --chown=${NB_USER}:${NB_GID} ./README.md /home/jovyan/README.md
