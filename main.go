package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type user1 struct {
	Firstname string `json: "firstname"`
	Lastname  string `json: "lastname"`
}

type user2 struct {
	Fullname string `json: "fullname"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	var username2 user2
	var username1 user1
	username1.Firstname = "Waralee"
	username1.Lastname = "Sakaranurak"

	conn, err := amqp.Dial("amqp://root:tzrootroot@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello-queue", // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	buf := bytes.Buffer{}
	buf.WriteString(username1.Firstname)
	buf.WriteString(username1.Lastname)
	username2.Fullname = buf.String()

	data, err := json.Marshal(username2)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(data),
		})
	log.Printf(" [x] Sent %s", data)
	failOnError(err, "Failed to publish a message")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
