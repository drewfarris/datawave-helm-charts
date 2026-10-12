FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends bash curl ca-certificates debianutils && \
    rm -rf /var/lib/apt/lists/* && useradd --uid 1002 --create-home runner
COPY query.sh /opt/query-examples/bin/query.sh
COPY test-web/ /opt/query-examples/bin/services/datawave/test-web/
COPY env.sh /opt/query-examples/bin/env.sh
COPY bootstrap.sh /opt/query-examples/bin/services/datawave/bootstrap.sh
RUN chown -R runner:runner /opt/query-examples
USER 1002:1002
