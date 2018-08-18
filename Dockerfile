FROM python:3.7.0-slim-stretch

RUN apt-get update -y
RUN apt-get dist-upgrade -y

# Env vars
ENV USER=pyedj \
    HOME=/home/pyedj \
    TERM=xterm-256color \
    APP_ROOT_PATH=/opt/pyedj \
    APP_PATH=/opt/pyedj/pyedj \
    SETUP_PATH=/opt/pyedj/setup

# Setup box
RUN mkdir -p $SETUP_PATH
COPY ./requirements.txt $SETUP_PATH
COPY ./pyedj/ $APP_PATH

# Install libs
RUN pip install -r $SETUP_PATH/requirements.txt

# Setup user
RUN useradd -ms /bin/bash -G sudo $USER
RUN chown $USER:$USER -R $APP_ROOT_PATH

USER pyedj

# Finish
WORKDIR /opt/pyedj

# debug
CMD ["/bin/bash"]
