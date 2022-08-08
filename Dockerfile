FROM quay.io/centos/centos:stream8

RUN dnf module -y install python39 && dnf install -y python39 python39-pip && dnf install -y git

RUN mkdir /plugin
RUN chmod 777 /plugin
ADD smallfile_plugin.py /plugin
ADD test_smallfile_plugin.py /plugin
ADD requirements.txt /plugin
ADD smallfile-example.yaml /plugin
ADD LICENSE /plugin
RUN chmod +x /plugin/smallfile_plugin.py /plugin/test_smallfile_plugin.py
WORKDIR /plugin

RUN pip3 install -r requirements.txt
#RUN /plugin/test_smallfile_plugin.py

USER 1000

RUN git clone -b 1.1 --single-branch https://github.com/distributed-system-analysis/smallfile.git

ENTRYPOINT ["/plugin/smallfile_plugin.py"]
CMD []

LABEL org.opencontainers.image.source="https://github.com/arcalot/arcaflow-plugins/tree/main/python/_smallfile_plugin"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.vendor="Arcalot project"
LABEL org.opencontainers.image.authors="Arcalot contributors"
LABEL org.opencontainers.image.title="Arcaflow Smallfile workload plugin"
LABEL io.github.arcalot.arcaflow.plugin.version="1"
