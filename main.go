package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

type userInput struct {
	Firstname string `json: "firstname"`
	Lastname  string `json: "lastname"`
}

type userOutput struct {
	Fullname string `json: "fullname"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://root:tzrootroot@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	chInput, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chInput.Close()

	chOutput, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer chOutput.Close()

	q, err := chInput.QueueDeclare(
		"queue:input", // name
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue input")
	msgs, err := chInput.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	qo, err := chOutput.QueueDeclare(
		"queue:output", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	failOnError(err, "Failed to declare a queue output")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var inputUser userInput
			err := json.Unmarshal(d.Body, &inputUser)
			if err != nil {
				fmt.Println("Convert string to json error", err)
			}
			outputUser := userOutput{
				Fullname: fmt.Sprintf("%s %s", inputUser.Firstname, inputUser.Lastname),
			}
			newJSONStringValue, err := json.Marshal(outputUser)
			if err != nil {
				fmt.Println("Convert struct to json string error", err)
			}
			err = chOutput.Publish(
				"",      // exchange
				qo.Name, // routing key
				false,   // mandatory
				false,   // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(newJSONStringValue),
				})
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
