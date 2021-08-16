# Basic Examples of Message-Publishing and -Subscribing Services

This directory contains two scripts that demostrate very basic
services which use the pub/sub functionality of the
ghga_service_chassis_lib.

## Usage:
First start the subscribing service:
```bash
python3 ./subscriber.py
```

Thereafter, in another terminal, execute the publishing
service:
```bash
python3 ./publisher.py
```

You should now see the following in the stdout of the subscribing
service:
```
Received the message number: 0
Received the message number: 1
Received the message number: 2
Received the message number: 3
Received the message number: 4
Received the message number: 5
Received the message number: 6
Received the message number: 7
Received the message number: 8
Received the message number: 9
```
