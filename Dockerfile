FROM node:20-slim
WORKDIR /app
COPY package.json server.js ./
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates tar gzip && rm -rf /var/lib/apt/lists/* \
    && curl -L -o /tmp/sing-box.tar.gz https://github.com/SagerNet/sing-box/releases/download/v1.14.4/sing-box-1.14.4-linux-amd64.tar.gz \
    && tar -xzf /tmp/sing-box.tar.gz -C /tmp \
    && mv /tmp/sing-box-1.14.4-linux-amd64/sing-box /app/sing-box && chmod +x /app/sing-box \
    && rm -rf /tmp/sing-box.tar.gz /tmp/sing-box-1.14.4-linux-amd64
EXPOSE 8080
CMD ["node", "server.js"]
