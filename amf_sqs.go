package mq

import (
"fmt"
//"encoding/base64"
"github.com/aws/aws-sdk-go/aws"
"github.com/aws/aws-sdk-go/aws/credentials"
"github.com/aws/aws-sdk-go/aws/session"
"github.com/aws/aws-sdk-go/service/sqs"
"github.com/google/uuid"
)

type SQSType struct {
	AccessKeyId string
	SecretKey string
	Region string
	QueueName string
	MessageGroupId string
	Handle []byte
	Client *sqs.SQS
}


func (s *SQSType) Init(accesskeyid, secretkey, region, msgGroupId string) {
	s.AccessKeyId = accesskeyid
	s.SecretKey = secretkey
	s.Region = region
	s.MessageGroupId = msgGroupId
	s.Client = nil
	s.Handle = nil
}

func (s *SQSType) SetHandle(handle []byte) error {
	s.Handle = handle
	return nil
}
func (s *SQSType) Stat(qname string) bool {
	// TODO: Ping
	return true
}

func (s *SQSType) Connect(shared bool) error {
	config := &aws.Config{
		Region:      aws.String(s.Region),
		Credentials: credentials.NewStaticCredentials(s.AccessKeyId, s.SecretKey, ""),
	}
	sess := session.Must(session.NewSession(config))
	s.Client = sqs.New(sess)
	return nil
}

func (s *SQSType) OpenQueue(qname string) error {
	s.QueueName = qname
	return nil
}

func (s *SQSType)  Put(qname string, data []byte) (err error) {
	fmt.Printf("Using Queue:%s\n",qname)
	//s.Handle = nil
	result, err := s.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(qname),
	})

	if err!=nil {
		fmt.Printf("Invalid Queue Name:%#v\n",err)
		return err
	}
	queueUrl := *result.QueueUrl
	fmt.Printf("Queue Url: %#v\n",queueUrl)
	u, _ := uuid.NewRandom()
	_, err = s.Client.SendMessage(&sqs.SendMessageInput{
		MessageBody: aws.String(string(data)),
		QueueUrl:    aws.String(queueUrl),
		MessageGroupId: aws.String(s.MessageGroupId),
		MessageDeduplicationId: aws.String(u.String()),
	})

	if err!=nil {
		fmt.Printf("Failed to send message:%#v\n",err)
		return err
	}

	return nil
}

func (s *SQSType) Get(wait int64) (data []byte, handle []byte, err error) {
	result, err := s.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &s.QueueName,
	})

	if err!=nil {
		fmt.Printf("Invalid Queue Name:%#v\n",err)
		return nil, nil, err
	}
	queueUrl := *result.QueueUrl

	msgResult, err := s.Client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(wait),
	})

	if err!=nil {
		return nil, nil, err
	}

	if len(msgResult.Messages) >0 {
		fmt.Println("Message Handle: " + *msgResult.Messages[0].ReceiptHandle)
		return []byte(*msgResult.Messages[0].Body),[]byte(*msgResult.Messages[0].ReceiptHandle), nil
	}
	s.Handle = nil
	return []byte(""), nil, nil
}

func (s *SQSType) Ping() bool  {
	return true
}

func (s *SQSType) Commit() error {
	if s.Handle == nil {
		return nil
	}
	result, err := s.Client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &s.QueueName,
	})

	if err!=nil {
		fmt.Printf("Invalid Queue Name:%#v",err)
		return err
	}
	queueUrl := *result.QueueUrl

	_, err = s.Client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueUrl),
		ReceiptHandle: aws.String(string(s.Handle)),
	})
	return err
}

func (s *SQSType) Backout() error {
	s.Handle = nil
	return nil
}

func (s *SQSType)  Disconnect() error {
	s.Client = nil
	return nil
}

func (s *SQSType) GetUUID() uuid.UUID {
	key,_ := uuid.NewRandom()
	return key
}


