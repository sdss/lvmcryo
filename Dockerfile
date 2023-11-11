FROM python:3.12.0-slim-bookworm

MAINTAINER Jose Sanchez-Gallego, gallegoj@uw.edu
LABEL org.opencontainers.image.source https://github.com/sdss/lvm-ln2fill

WORKDIR /opt

COPY . ln2fill

RUN pip3 install -U pip setuptools wheel
RUN cd ln2fill && pip3 install .
RUN rm -Rf ln2fill
