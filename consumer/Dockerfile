FROM node:10.15-alpine
EXPOSE 3000
RUN apk add --no-cache tini
ENV NODE_ENV=production
WORKDIR /node
COPY consumer/package.json package-lock*.json ./
RUN npm install && npm cache clean --force
WORKDIR /node/app
COPY consumer/ .
ENTRYPOINT [ "/sbin/tini", "--" ]
CMD ["node", "consumer.js"]