FROM node:19.5.0

# app
COPY executor.js /opt/executor/
COPY package.json /opt/executor/
WORKDIR /opt/executor
RUN npm install

ENTRYPOINT ["node", "executor.js"]
