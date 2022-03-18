// +build postgres sqs nmq

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
	"fmt"
	"strings"
)

const (
	MQ_HOST    = "mq_host"
	MQ_PORT    = "mq_port"
	MQ_CHANNEL = "mq_channel"
	MQ_USER    = "mq_user"
	MQ_MANAGER = "mq_manager"
)

type MQType struct {
	name    string
	host    string
	port    string
	channel string
	dbq     bool
	sqs     bool
	nats    bool
	nats2   bool
	pmq    *PMQType
	smq    *SQSType
	nmq    *NMQType
	nmq2   *NMQType2
}

func (mq *MQType) Init(name, host, port, channel string) {
	mq.name = name
	mq.host = host
	mq.port = port
	mq.channel = channel
	if name == "sqs" {
		mq.sqs = true
		mq.smq = &SQSType{}
		fmt.Printf("Channel Received:%s\n",channel)
		params := strings.Split(channel,":")
		mq.smq.Init(params[0],params[1],params[2],params[3])
	}
	if name == "postgres" {
		mq.dbq = true
		mq.pmq = &PMQType{}
		mq.pmq.Init(name,host,port,channel)
	}
	if name == "nats" {
		mq.nats = true
		mq.nmq = &NMQType{}
		fmt.Printf("Connecting to MQ with %v - %v - %v -%v\n",name,host,port,channel)
		mq.nmq.Init(name,host,port,channel)
	}
	if name == "nats2" {
		mq.nats2 = true
		mq.nmq2 = &NMQType2{}
		fmt.Printf("Connecting to MQ with %v - %v - %v -%v\n",name,host,port,channel)
		mq.nmq2.Init(name,host,port,channel)
	}

}

func (mq *MQType) Stat(qname string) bool {
	if mq.dbq {
		return mq.pmq.Ping()
	} else if mq.sqs {
		return mq.smq.Ping()
	} else if mq.nats {
		return mq.nmq.Ping()
	} else if mq.nats2 {
		return mq.nmq2.Ping()
	}
	return false

}

func (mq *MQType) Connect(shared bool) error {
	if mq.dbq {
		return mq.pmq.Connect(shared)
	}  else if mq.sqs {
		return mq.smq.Connect(shared)
	} else if mq.nats {
		return mq.nmq.Connect(shared)
	} else if mq.nats2 {
		return mq.nmq2.Connect(shared)
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) OpenQueue(qname string) error {
	if mq.dbq {
		mq.pmq.inputq = qname
		return mq.pmq.OpenQueue(qname)
	} else if mq.sqs {
		return mq.smq.OpenQueue(qname)
	} else if mq.nats {
		return mq.nmq.OpenQueue(qname)
	} else if mq.nats2 {
		return mq.nmq2.OpenQueue(qname)
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) OpenQueueForBrowse(qname string) error {
	if mq.dbq {
		return nil
	} else if mq.sqs {
		return nil
	} else if mq.nats {
		return nil
	} else if mq.nats2 {
		return nil
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) DeleteCurrentMessage(wait int32) (string, error) {

	if mq.dbq {
		return "", nil
	}  else if mq.sqs {
		return "", nil
	} else if mq.nats {
		return "", nil
	} else if mq.nats2 {
		return "", nil
	}
	return "", fmt.Errorf("MQ not supported")
}

func (mq *MQType) GetWithBrowse(wait int32, beginning bool) (string, error) {
	if mq.dbq {
		return "", nil
	} else if mq.sqs {
		return "", nil
	} else if mq.nats {
		return "", nil
	} else if mq.nats2 {
		return "", nil
	}
	return "",fmt.Errorf("MQ not supported")
}

func (mq *MQType) Put(qname string, data []byte) error {
	if mq.dbq {
		return mq.pmq.Put(qname, data)
	} else if mq.sqs {
		return mq.smq.Put(qname, data)
	} else if mq.nats {
		return mq.nmq.Put(qname, data)
	} else if mq.nats2 {
		return mq.nmq2.Put(qname, data)
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) Get(qname string, wait int32) ([]byte, error) {
	if mq.dbq {
		data,err := mq.pmq.Get(int64(wait))
		return data, err
	} else if mq.sqs {
		data,handle,err := mq.smq.Get(int64(wait))
		mq.smq.SetHandle(handle)
		return data, err
	} else if mq.nats {
		data,err := mq.nmq.Get(int64(wait))
		return data, err
	} else if mq.nats2 {
		data,err := mq.nmq2.Get(int64(wait))
		return data, err
	}
	return nil, fmt.Errorf("MQ not supported")
}

func (mq *MQType) GetMessage(wait int32) (*interface{}, []byte, error) {
	if mq.dbq {
		data,err := mq.pmq.Get(int64(wait))
		return nil, data, err
	} else if mq.sqs {
		data,handle, err := mq.smq.Get(int64(wait))
		mq.smq.SetHandle(handle)
		return nil, data, err
	} else if mq.nats {
		data,err := mq.nmq.Get(int64(wait))
		return nil,data, err
	} else if mq.nats2 {
		data,err := mq.nmq2.Get(int64(wait))
		return nil,data, err
	}
	return nil, nil, fmt.Errorf("MQ not supported")
}

func (mq *MQType) Commit() error {
	if mq.dbq {
		return mq.pmq.Commit()
	} else if mq.sqs {
		return mq.smq.Commit()
	} else if mq.nats {
		return mq.nmq.Commit()
	} else if mq.nats2 {
		return mq.nmq2.Commit()
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) Backout() error {
	if mq.dbq {
		return mq.pmq.Backout()
	} else if mq.sqs {
		return mq.smq.Backout()
	} else if mq.nats {
		return mq.nmq.Backout()
	} else if mq.nats2 {
		return mq.nmq2.Backout()
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) Disconnect() error {
	if mq.dbq {
		return mq.pmq.Disconnect()
	} else if mq.sqs {
		return mq.smq.Disconnect()
	} else if mq.nats {
		return mq.nmq.Disconnect()
	} else if mq.nats2 {
		return mq.nmq2.Disconnect()
	}
	return fmt.Errorf("MQ not supported")
}

func (mq *MQType) Close() error {
	if mq.dbq {
		return nil
	} else if mq.sqs {
		return nil
	} else if mq.nats {
		return mq.nmq.Close()
	} else if mq.nats2 {
		return mq.nmq2.Close()
	}
	return fmt.Errorf("MQ not supported")
}
