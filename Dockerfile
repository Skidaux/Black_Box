FROM debian:stable-slim AS build

WORKDIR /BlackBox

COPY build/BlackBox /BlackBox/BlackBox
RUN chmod +x /BlackBox/BlackBox
EXPOSE 8080/tcp

CMD [ "./BlackBox" ]