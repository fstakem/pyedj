# Run mosquitto broker
sudo docker run -it -p 1883:1883 -p 9001:9001 --rm --name broker eclipse-mosquitto