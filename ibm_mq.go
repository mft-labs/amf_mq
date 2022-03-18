// +build ibmmq

/****************************************************************************
 *
 * Copyright (C) Agile Data, Inc - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * Written by Stephen Noel <noel@hub4edi.com>, January 2019
 *
 ****************************************************************************/
package mq

import (
	"fmt"
	"github.com/ibm-messaging/mq-golang/ibmmq"
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
	//	buffer  []byte
	inputq ibmmq.MQObject
	mgr    ibmmq.MQQueueManager
}

func (mq *MQType) Init(name, host, port, channel string) {
	mq.name = name
	mq.host = host
	mq.port = port
	mq.channel = channel
}

func (mq *MQType) Stat(qname string) bool {
	connected := false
	mqod := ibmmq.NewMQOD()
	mqod.ObjectType = ibmmq.MQOT_Q
	mqod.ObjectName = qname
	options := ibmmq.MQOO_FAIL_IF_QUIESCING | ibmmq.MQOO_INQUIRE
	q, err := mq.mgr.Open(mqod, options)
	if err == nil {
		connected = true
		defer q.Close(0)
	}
	return connected
}

func (mq *MQType) ConnectWithUsernamePassword(shared bool, userId, password string) error {
	var err error
	mqco := ibmmq.NewMQCNO()
	cd := ibmmq.NewMQCD()
	cd.ChannelName = mq.channel
	cd.ConnectionName = fmt.Sprintf("%s(%s)", mq.host, mq.port)
	mqco.ClientConn = cd
	csp := ibmmq.NewMQCSP()
	csp.AuthenticationType = ibmmq.MQCSP_AUTH_USER_ID_AND_PWD
	csp.UserId = userId
	csp.Password = password
	mqco.SecurityParms = csp
	if shared {
		mqco.Options = ibmmq.MQCNO_CLIENT_BINDING | ibmmq.MQCNO_HANDLE_SHARE_BLOCK
		mq.mgr, err = ibmmq.Connx(mq.name, mqco)
	} else {
		mqco.Options = ibmmq.MQCNO_CLIENT_BINDING
		mq.mgr, err = ibmmq.Connx(mq.name, mqco)
	}
	return err
}

func (mq *MQType) Connect(shared bool) error {
	var err error
	mqco := ibmmq.NewMQCNO()
	cd := ibmmq.NewMQCD()
	cd.ChannelName = mq.channel
	cd.ConnectionName = fmt.Sprintf("%s(%s)", mq.host, mq.port)
	mqco.ClientConn = cd
	if shared {
		mqco.Options = ibmmq.MQCNO_CLIENT_BINDING | ibmmq.MQCNO_HANDLE_SHARE_BLOCK
		mq.mgr, err = ibmmq.Connx(mq.name, mqco)
	} else {
		mqco.Options = ibmmq.MQCNO_CLIENT_BINDING
		mq.mgr, err = ibmmq.Connx(mq.name, mqco)
	}
	return err
}

func (mq *MQType) OpenQueue(qname string) error {
	var err error
	mqod := ibmmq.NewMQOD()
	mqod.ObjectType = ibmmq.MQOT_Q
	mqod.ObjectName = qname
	options := ibmmq.MQOO_FAIL_IF_QUIESCING | ibmmq.MQOO_INPUT_SHARED
	mq.inputq, err = mq.mgr.Open(mqod, options)
	if err != nil {
		return err
	}
	return nil
}

func (mq *MQType) OpenQueueForBrowse(qname string) error {
	var err error
	mqod := ibmmq.NewMQOD()
	mqod.ObjectType = ibmmq.MQOT_Q
	mqod.ObjectName = qname
	options := ibmmq.MQOO_FAIL_IF_QUIESCING | ibmmq.MQOO_INPUT_SHARED | ibmmq.MQOO_BROWSE
	mq.inputq, err = mq.mgr.Open(mqod, options)
	if err != nil {
		return err
	}
	return nil
}

func (mq *MQType) DeleteCurrentMessage(wait int32) (string, error) {
	var err error
	var data string
	buffer := make([]byte, 4096)
	md := ibmmq.NewMQMD()
	md.CodedCharSetId = 1208
	gmo := ibmmq.NewMQGMO()
	gmo.Options = ibmmq.MQGMO_WAIT | ibmmq.MQGMO_MSG_UNDER_CURSOR
	gmo.WaitInterval = (wait * 1000) // The WaitInterval is in ms
	datalen, err := mq.inputq.Get(md, gmo, buffer)
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
			return data, err
		} else {
			return "", nil
		}
	}
	return string(buffer[:datalen]), nil
}

func (mq *MQType) GetWithBrowse(wait int32, beginning bool) (string, error) {
	var err error
	var data string
	buffer := make([]byte, 4096)

	md := ibmmq.NewMQMD()
	md.CodedCharSetId = 1208
	gmo := ibmmq.NewMQGMO()
	if beginning {
		gmo.Options = ibmmq.MQGMO_BROWSE_FIRST
	} else {
		gmo.Options = ibmmq.MQGMO_BROWSE_NEXT
	}
	gmo.WaitInterval = (0) // The WaitInterval is in ms
	datalen, err := mq.inputq.Get(md, gmo, buffer)
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
			return data, err
		} else {
			return "", nil
		}
	}

	return string(buffer[:datalen]), nil
}

func (mq *MQType) Put(qname string, data []byte) error {
	od := ibmmq.NewMQOD()
	od.ObjectType = ibmmq.MQOT_Q
	od.ObjectName = qname
	md := ibmmq.NewMQMD()
	md.Format = ibmmq.MQFMT_STRING
	md.Encoding = ibmmq.MQENC_NATIVE
	md.ApplIdentityData = "AMFv1.0"
	md.MsgType = ibmmq.MQMT_DATAGRAM
	pmo := ibmmq.NewMQPMO()
	pmo.Options = ibmmq.MQPMO_FAIL_IF_QUIESCING | ibmmq.MQPMO_SYNCPOINT
	if err := mq.mgr.Put1(od, md, pmo, data); err != nil {
		return err
	}
	return nil
}

func (mq *MQType) Get(qname string, wait int32) ([]byte, error) {
	var err error
	var data []byte
	buffer := make([]byte, 4096)

	md := ibmmq.NewMQMD()
	md.CodedCharSetId = 1208
	gmo := ibmmq.NewMQGMO()
	gmo.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_WAIT
	gmo.WaitInterval = (wait * 1000) // The WaitInterval is in ms
	datalen, err := mq.inputq.Get(md, gmo, buffer)
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
			return make([]byte, 0), nil
		} else {
			return data, err
		}
	}

	return buffer[:datalen], nil
}

// for those who need message descriptor
func (mq *MQType) GetMessage(wait int32) (*ibmmq.MQMD, []byte, error) {
	buffer := make([]byte, 100000)
	md := ibmmq.NewMQMD()
	md.CodedCharSetId = 1208
	gmo := ibmmq.NewMQGMO()
	gmo.Options = ibmmq.MQGMO_SYNCPOINT | ibmmq.MQGMO_WAIT
	gmo.WaitInterval = (wait * 1000) // The WaitInterval is in ms
	if mq==nil  {
		return md, buffer, fmt.Errorf("MQ not available, returning")
	}
	datalen, err := mq.inputq.Get(md, gmo, buffer)
	if err != nil {
		mqret := err.(*ibmmq.MQReturn)
		if mqret.MQRC != ibmmq.MQRC_NO_MSG_AVAILABLE {
			return md, buffer, err
		} else {
			buffer = make([]byte, 0)
			return md, buffer, nil
		}
	}
	return md, buffer[:datalen], nil
}

func (mq *MQType) Commit() error {
	return mq.mgr.Cmit()
}

func (mq *MQType) Backout() error {
	return mq.mgr.Back()
}

// Disconnect from the queue manager
func (mq *MQType) Disconnect() error {
	if mq == nil {
		return nil
	}
	return mq.mgr.Disc()
}

// Close the queue if it was opened
func (mq *MQType) Close() error {
	if mq == nil {
		return nil
	}
	return mq.inputq.Close(0)
}
