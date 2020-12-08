FROM zjia/nightcore:asplos-ae as nightcore
FROM python:3.7-slim-buster as base

FROM base as builder

RUN apt-get -qq update \
    && apt-get install -y --no-install-recommends \
        build-essential cmake wget

COPY --from=nightcore /src/nightcore /usr/src/faas
RUN cd /usr/src/faas && ./build_deps.sh && make -j $(nproc)
RUN cd /usr/src/faas/worker/python && make -j $(nproc)

FROM base

# show python logs as they occur
ENV PYTHONUNBUFFERED=0
ENV PYTHONPATH=/nightcore/python

COPY --from=builder /usr/src/faas/worker/python/faas   /nightcore/python/faas
COPY --from=builder /usr/src/faas/bin/release/launcher /nightcore/launcher

# get packages
WORKDIR /recommendationservice
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# add files into working directory
COPY . .

ENTRYPOINT [ "/nightcore/launcher", \
             "--fprocess=python /recommendationservice/recommendation_server.py", \
             "--fprocess_working_dir=/recommendationservice", \
             "--fprocess_mode=python" ]
