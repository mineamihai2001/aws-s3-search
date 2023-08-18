package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Data struct {
	UserId   string                 `json:"userId"`
	Status   string                 `json:"status"`
	Request  map[string]interface{} `json:"request"`
	Response interface{}            `json:"response"`
}

type Meta struct {
	Resource   string `json:"resource"`
	Identifier string `json:"identifier"`
	SourceIp   string `json:"sourceIp"`
	SourcePort int    `json:"sourcePort"`
	DestIp     string `json:"destIp"`
	DestPort   int    `json:"destPort"`
	UserAgent  string `json:"userAgent"`
	Os         string `json:"os"`
	DeviceId   string `json:"deviceId"`
}

type Log struct {
	Timestamp int64 `json:"timestamp"`
	Data      Data  `json:"data"`
	Meta      Meta  `json:"meta"`
}

type Event struct {
	path      string
	action    string
	timestamp int64
	status    string
	ip        string
	os        string
	userAgent string
	user      string
}

func (l *Log) ResourceToString() string {
	actions := map[string]string{
		"auth/login": "log in",
	}

	for key, value := range actions {
		if strings.Contains(l.Meta.Resource, key) {
			return value
		}
	}

	return "unknown action"
}

func (l *Log) ToString() string {
	var user string
	if l.Data.Request["email"] != nil {
		user = l.Data.Request["email"].(string)
	} else if !(l.Data.UserId == "" || l.Data.UserId == "null") {
		user = l.Data.UserId
	} else {
		user = "<identification number/email not found>"
	}

	bytesResponse, _ := json.Marshal(l.Data.Response)

	return fmt.Sprintf("User %s has %s to %s at %s, from IP %s, OS %s, userAgent %s. Server Response: %s\n",
		user,
		l.Data.Status,
		l.ResourceToString(),
		time.Unix(l.Timestamp/1000, 0).Format("01-02-2006, 15:04:05"),
		l.Meta.DestIp,
		l.Meta.Os,
		l.Meta.UserAgent,
		string(bytesResponse),
	)
}
