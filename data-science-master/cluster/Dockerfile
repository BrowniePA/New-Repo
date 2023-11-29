ARG BASE_CONTAINER=continuumio/miniconda3

FROM $BASE_CONTAINER AS build

COPY ./dask_cluster_env.yaml ./

RUN conda config --set auto_update_conda False

RUN conda install -c conda-forge conda-pack

RUN conda env create -f ./dask_cluster_env.yaml -n dask_env

RUN conda-pack -n dask_env -o /tmp/env.tar \
	&& mkdir /venv \
	&& cd /venv \
	&& tar xf /tmp/env.tar \
	&& rm /tmp/env.tar
	
RUN /venv/bin/conda-unpack

FROM debian:stable-slim AS runtime

COPY --from=build /venv /venv

ENV TINI_VERSION v0.19.0

#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN apt-get update \
	&& apt-get install tini

#RUN chmod +x /tini

COPY ./prepare.sh /usr/bin/prepare.sh

RUN chmod +x /usr/bin/prepare.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/bin/prepare.sh"]