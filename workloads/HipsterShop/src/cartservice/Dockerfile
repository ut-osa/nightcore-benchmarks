FROM zjia/nightcore:asplos-ae as nightcore
FROM node:10.22.0-buster-slim as base

FROM base as builder

RUN apt-get -qq update \
    && apt-get install -y --no-install-recommends \
        build-essential cmake wget python3

COPY --from=nightcore /src/nightcore /usr/src/faas
RUN cd /usr/src/faas && ./build_deps.sh && make -j $(nproc)

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --only=production

FROM base

COPY --from=builder /usr/src/app/node_modules          /usr/src/app/node_modules
COPY --from=builder /usr/src/faas/worker/nodejs        /usr/src/app/node_modules/faas
COPY --from=builder /usr/src/faas/bin/release/launcher /nightcore/launcher

COPY ./server.js /usr/src/app/server.js
COPY ./proto     /usr/src/app/proto

ENTRYPOINT [ "/nightcore/launcher", \
             "--fprocess=node server.js", \
             "--fprocess_working_dir=/usr/src/app", \
             "--fprocess_mode=nodejs" ]
