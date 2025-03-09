FROM alpine:3.20

# Define the project name | 定义项目名称
ARG PROJECT=stocktask
# Define the config file name | 定义配置文件名
ARG CONFIG_FILE=stocktask.yaml
# Define the author | 定义作者
ARG AUTHOR="example@example.com"

LABEL org.opencontainers.image.authors=${AUTHOR}

WORKDIR /app
ENV PROJECT=${PROJECT}
ENV CONFIG_FILE=${CONFIG_FILE}

COPY ./${PROJECT}_api ./
COPY ./etc/${CONFIG_FILE} ./etc/

EXPOSE 9100

ENTRYPOINT ./${PROJECT}_api -f etc/${CONFIG_FILE}