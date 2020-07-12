package toolkits

import (
	//"github.com/ngaut/log"
	"github.com/siddontang/go-log/log"
	"regexp"
	"strconv"
)

func ConvStrToInt64(s string) int64 {
	if s == "" || s == "NULL" {
		return 0
	}
	value := regexp.MustCompile("\\d+").FindString(s)
	i, err := strconv.ParseInt(value, 10, 64)
	if nil != err {
		log.Errorf("convStrToInt64 err: parse(%v) to int64 err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToInt(s string) int {
	if s == "" || s == "NULL" {
		return 0
	}
	i, err := strconv.Atoi(s)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to int err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToFloat(s string) float64 {
	if s == "" || s == "NULL" {
		return 0
	}
	i, err := strconv.ParseFloat(s, 64)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to int err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToBool(s string) bool {
	if s == "" || s == "NULL" {
		return false
	}
	i, err := strconv.ParseBool(s)
	if nil != err {
		log.Errorf("ConvStrToBool err: parse(%v) to bool err:%v\n", s, err)
		return false
	}
	return i
}
