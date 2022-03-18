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
	"time"
)



type NMQType struct {
	url string
	con1 *nats.Conn
	con2 *nats.Conn
	sub *nats.Subscription
	inputq string
	msg *nats.Msg
	ctx    context.Context
}

func (nmq *NMQType) Init(name, host, port,channel string) {
	fmt.Printf("Connecting to MQ with NATS %v - %v - %v -%v\n",name,host,port,channel)
	nmq.url = channel
	nmq.con1 = nil
	nmq.con2 = nil
	nmq.sub = nil
	nmq.msg = nil
}

func (nmq *NMQType) Stat(qname string) bool {
	return nmq.con1  != nil && nmq.con2 != nil
}

func (nmq *NMQType) Connect(shared bool) error {
	var err error
	//fmt.Printf("Connecting to MQ with %v - %v - %v -%v\n",name,host,port,channel)
	nmq.con1, err = nats.Connect(nmq.url)
	if err!= nil {
		nmq.con1 = nil
		return err
	}
	nmq.con2, err = nats.Connect(nmq.url)
	if err!=nil {
		nmq.con2 = nil
		return err
	}
	return nil
}

func (nmq *NMQType) OpenQueue(qname string) error {
	nmq.inputq = qname
	var err error
	nmq.sub, err = nmq.con2.SubscribeSync(qname)
	return err
}

func (nmq *NMQType) Put(qname string, data []byte) (err error) {
	err = nmq.con1.Publish(qname, data)
	return err
}

func (nmq *NMQType) Get(wait int64) (data []byte, err error) {
	msg, err := nmq.sub.NextMsg(time.Second*time.Duration(wait))
	if err!=nil {
		return nil, err
	}
	nmq.msg = msg
	nmq.con2.Publish(msg.Reply,[]byte("Received"))
	return msg.Data, nil
}

func (nmq *NMQType) Ping() bool  {
	return nmq.con1  != nil && nmq.con2 != nil
}

func (nmq *NMQType) Commit() error {
	return nil
}

func (nmq *NMQType) Backout() error {
	nmq.Put(nmq.inputq, nmq.msg.Data )
	return nil
}

func (nmq *NMQType) Disconnect() error {
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

func (nmq *NMQType) Close() error {
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

func  (nmq *NMQType) GetUUID() uuid.UUID {
	key,_ := uuid.NewRandom()
	return key
}

