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
"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/google/uuid"
"strings"
"time"
)

const (
	INSERT_QUEUE = `INSERT INTO QT___QUEUE_NAME__ (queue_id,queue_data, queued_time, queued_by) values ($1,$2,$3,$4)`
	SELECT_QUEUE = `DELETE FROM QT___QUEUE_NAME__ WHERE queue_id = (SELECT queue_id FROM QT___QUEUE_NAME__ 
                        ORDER BY queue_id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING queue_id, queue_data`
)

type PMQType struct {
	url    string
	inputq string
	con    *sql.DB
	ctx    context.Context
	tx     *sql.Tx
}

func (pmq *PMQType) Init(name, host, port, channel string) {
	pmq.url = channel
}

func (pmq *PMQType) Stat(qname string) bool {
	return pmq.con.Ping() != nil
}

func (pmq *PMQType) Connect(shared bool) error {
	var err error
	pmq.con, err = sql.Open("postgres", pmq.url)
	if err != nil {
		return fmt.Errorf("\nError occurred while connecting database :%v",err)
	}
	return nil
}

func (pmq *PMQType) OpenQueue(qname string) error {
	pmq.inputq = qname
	return nil
}

/*
func (pmq *PMQType) Put(qname string, data []byte) (err error) {
	if pmq.tx == nil {
		pmq.tx, err = pmq.con.Begin()
		if err != nil {
			return fmt.Errorf("Connection establishment failed:%v",err)
		}
	}

	insert_sql := strings.Replace(INSERT_QUEUE, "__QUEUE_NAME__", qname, -1)
	if pmq.tx != nil {
		_, err = pmq.tx.Exec(insert_sql, pmq.GetUUID(),string(data), time.Now(), "system")
		if err != nil {
			return fmt.Errorf("\nError occurred while put message in queue(2):%v\n%s",err,pmq.url)
		}
		return nil
	} else {
		return fmt.Errorf("Invalid MQ transaction (DB)")
	}

}
*/

func (pmq *PMQType) Put(qname string, data []byte) (err error) {
	insert_sql := strings.Replace(INSERT_QUEUE, "__QUEUE_NAME__", qname, -1)
	if pmq.tx == nil {
		_, err = pmq.con.Exec(insert_sql, pmq.GetUUID(),string(data), time.Now(), "system")
		if err != nil {
			return fmt.Errorf("\nError occurred while put message in queue(2): %v\n%s",err,pmq.url)
		}
	} else {
		_, err = pmq.tx.Exec(insert_sql, pmq.GetUUID(),string(data), time.Now(), "system")
		if err != nil {
			return fmt.Errorf("\nError occurred while put message in queue(2) with tx: %v\n%s",err,pmq.url)
		}
	}
	return nil
}

func (pmq *PMQType) Get(wait int64) (data []byte, err error) {
	var buf string
	id := pmq.GetUUID()

	select_sql := strings.Replace(SELECT_QUEUE, "__QUEUE_NAME__", pmq.inputq, -1)

	st := time.Now().Unix()

	for {
		pmq.tx, err = pmq.con.Begin()
		if err != nil {
			return data, err
		}
		err = pmq.tx.QueryRow(select_sql).Scan(&id, &buf)
		if err == nil {
			data = []byte(buf)
			break
		} else {
			pmq.tx.Rollback()
			if err == sql.ErrNoRows {
				time.Sleep(1 * time.Second)
				if (time.Now().Unix() - st) >= wait {
					return data, nil
				}
			} else {
				return data, err
			}
		}
	}
	return data, nil
}

func (pmq *PMQType) Ping() bool  {
	return pmq.con.Ping() != nil
}

func (pmq *PMQType) Commit() error {
	if pmq.tx != nil {
		pmq.tx.Commit()
		pmq.tx = nil
	}
	return nil
}

func (pmq *PMQType) Backout() error {
	if pmq.tx != nil {
		pmq.tx.Rollback()
		pmq.tx = nil
	}
	return nil
}

func (pmq *PMQType) Disconnect() error {
	pmq.con.Close()
	return nil
}

func (pmq *PMQType) GetUUID() uuid.UUID {
	key,_ := uuid.NewRandom()
	return key
}

