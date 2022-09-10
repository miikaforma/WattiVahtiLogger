FROM node:16-alpine3.15

RUN apk add --no-cache \
    chromium \
  && rm -rf /var/cache/apk/* /tmp/*

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium-browser

WORKDIR /app

COPY package*.json /app/

RUN npm i

RUN addgroup pptruser \
    && adduser pptruser -D -G pptruser \
    && chown -R pptruser:pptruser /home/pptruser \
    && chown -R pptruser:pptruser /app

USER pptruser

COPY src/index.js /app

EXPOSE 3000

CMD ["node", "index.js"]
