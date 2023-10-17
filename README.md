# Crawler Orquestration - Mercado Livre

***
  This project aims to present some concepts about the creation and orquestration of web crawlers using a message broker (RabbitMQ).
  The crawlers refers to the e-commerce 'Mercado Livre' specifically to the department of sales of houses and apartments. In it we extract valorous informations of each announcement. The collected informations are formatted in a json and kept into a database of MongoDB.

***

To run the sender:
```shell
python sender.py --queue-name name_of_queue
```
To run the consumer:
```shell
python consumer.py --queue-name name_of_queue
```

***
To run the stream, it must to contains into the project's folder the file .env following the model below:
```
USER=your_user_of_mongodb
PASSWORD=your_password_of_mongodb
```


