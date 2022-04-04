/****************************************************************************
 *
 * Copyright (C) Agile Data, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by ADI TEAM <code@agiledatainc.com>, November 2018
 *
 ****************************************************************************/

package mq

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"strings"
	"time"
)



type NMQType2 struct {
	url string
	con1 *nats.Conn
	con2 *nats.Conn
	js1 nats.JetStreamContext
	js2 nats.JetStreamContext
	sub *nats.Subscription
	inputq string
	msg *nats.Msg
	ctx    context.Context
}

func (nmq *NMQType2) Init(name, host, port,channel string) {
	fmt.Printf("Connecting to MQ with NATS %v - %v - %v -%v\n",name,host,port,channel)
	nmq.url = channel
	nmq.con1 = nil
	nmq.con2 = nil
	nmq.js1 = nil
	nmq.js2  = nil
	nmq.sub = nil
	nmq.msg = nil
}

func (nmq *NMQType2) Stat(qname string) bool {
	return nmq.con1  != nil && nmq.con2 != nil
}

func (nmq *NMQType2) Connect(shared bool) error {
	var err error
	//fmt.Printf("Connecting to MQ with %v - %v - %v -%v\n",name,host,port,channel)
	nmq.con1, err = nats.Connect(nmq.url)
	if err!= nil {
		nmq.js1 = nil
		nmq.con1 = nil
		return err
	}
	nmq.js1, err = nmq.con1.JetStream(nats.PublishAsyncMaxPending(2048))
	if err!=nil {
		nmq.con1.Close()
		nmq.js1 = nil
		nmq.con1 = nil
		return err
	}
	nmq.con2, err = nats.Connect(nmq.url)
	if err!=nil {
		nmq.con1.Close()
		nmq.js1 = nil
		nmq.con1 = nil
		nmq.con2 = nil
		return err
	}
	nmq.js2, err = nmq.con2.JetStream(nats.PublishAsyncMaxPending(2048))
	if err!=nil {
		nmq.con1.Close()
		nmq.con2.Close()
		nmq.js1 = nil
		nmq.con1 = nil
		nmq.js2 = nil
		nmq.con2 = nil
		return err
	}
	return nil
}

func (nmq *NMQType2) OpenQueue(qname string) error {
	nmq.inputq = qname
	var err error
	//nmq.sub, err = nmq.con2.SubscribeSync(qname)
	nmq.sub, err = nmq.js2.SubscribeSync(qname+".*", nats.Durable("monitor"), nats.MaxDeliver(1))
	if err!=nil && strings.Contains(err.Error(),"nats: no stream matches subject") {
		nmq.js2.AddStream(&nats.StreamConfig{
			Name:     qname,
			Subjects: []string{qname+".*"},
		})

		nmq.sub, err = nmq.js2.SubscribeSync(qname+".*", nats.Durable("monitor"), nats.MaxDeliver(1))
	}
	if err!=nil && strings.Contains(err.Error(),"consumer is already bound to a subscription") {
		return nil
	}
	return err
}

func (nmq *NMQType2) Put(qname string, data []byte) (err error) {
	_, err = nmq.js1.PublishAsync(qname+".scratch", data)
	if err!=nil {
		if strings.Contains(err.Error(),"nats: no stream matches subject") {
			nmq.js1.AddStream(&nats.StreamConfig{
				Name:     qname,
				Subjects: []string{qname+".*"},
			})
			_, err = nmq.js1.PublishAsync(qname+".scratch", data)
		}
	}
	return err
}

func (nmq *NMQType2) Get(wait int64) (data []byte, err error) {
	msg, err := nmq.sub.NextMsg(time.Second*time.Duration(wait))
	if err!=nil {
		if strings.Contains(err.Error(),"nats: timeout") {
			return nil, nil
		}
		return nil, err
	}
	nmq.msg = msg
	//nmq.con2.Publish(msg.Reply,[]byte("Received"))
	return msg.Data, nil
}

func (nmq *NMQType2) Ping() bool  {
	return nmq.con1  != nil && nmq.con2 != nil
}

func (nmq *NMQType2) Commit() error {
	if nmq.msg != nil {
		nmq.con2.Publish(nmq.msg.Reply,[]byte("Received"))
	}

	return nil
}

func (nmq *NMQType2) Backout() error {
	//nmq.Put(nmq.inputq, nmq.msg.Data )
	return nil
}

func (nmq *NMQType2) Disconnect() error {
	if nmq.sub != nil {
		nmq.sub.Unsubscribe()
		nmq.sub.Drain()
	}
	if nmq.con1 != nil {
		nmq.con1.Close()
	}
	if nmq.con2 != nil {
		nmq.con2.Close()
	}
	nmq.sub = nil
	nmq.con1 = nil
	nmq.con2 = nil
	return nil
}

func (nmq *NMQType2) Close() error {
	if nmq.sub != nil {
		nmq.sub.Unsubscribe()
		nmq.sub.Drain()
	}
	if nmq.con1 != nil {
		nmq.con1.Close()
	}
	if nmq.con2 != nil {
		nmq.con2.Close()
	}
	nmq.sub = nil
	nmq.js1 = nil
	nmq.con1 = nil
	nmq.js2 = nil
	nmq.con2 = nil
	return nil
}

func  (nmq *NMQType2) GetUUID() uuid.UUID {
	key,_ := uuid.NewRandom()
	return key
}
