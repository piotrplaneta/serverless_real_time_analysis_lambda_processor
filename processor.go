package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var db = dynamodb.New(session.New(), aws.NewConfig().WithRegion("us-east-1"))

type WildRydeKinesisStatus struct {
	Name            string `json:"Name"`
	StatusTime      string `json:"StatusTime"`
	Distance        int64  `json:"Distance"`
	MaxHealthPoints int64  `json:"MaxHealthPoints"`
	MinHealthPoints int64  `json:"MinHealthPoints"`
	MaxMagicPoints  int64  `json:"MaxMagicPoints"`
	MinMagicPoints  int64  `json:"MinMagicPoints"`
}

func main() {
	lambda.Start(handler)
}

func handler(ctx context.Context, kinesisEvent events.KinesisEvent) error {
	for _, record := range kinesisEvent.Records {
		kinesisRecord := record.Kinesis
		dataBytes := kinesisRecord.Data

		var parsedStatus WildRydeKinesisStatus
		jsonParsingErr := json.Unmarshal(dataBytes, &parsedStatus)
		if jsonParsingErr != nil {
			panic(jsonParsingErr)
		}

		fmt.Println(parsedStatus)
		saveToDynamoError := saveToDynamo(&parsedStatus)
		if saveToDynamoError != nil {
			panic(saveToDynamoError)
		}
	}

	return nil
}

func saveToDynamo(wildRydeStatus *WildRydeKinesisStatus) error {
	parsedToDynamoStatus, err := dynamodbattribute.MarshalMap(wildRydeStatus)
	if err != nil {
		panic(err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String("UnicornSensorData"),
		Item:      parsedToDynamoStatus,
	}

	_, dynamoErr := db.PutItem(input)
	return dynamoErr
}
