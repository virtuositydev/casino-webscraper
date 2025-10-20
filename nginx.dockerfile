FROM nginx:alpine

# Remove default config and welcome page
RUN rm -rf /etc/nginx/conf.d/*
RUN rm -f /usr/share/nginx/html/index.html /usr/share/nginx/html/50x.html

# Copy custom config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Ensure directories exist

RUN mkdir -p /usr/share/nginx/html/output /usr/share/nginx/html/logs 
