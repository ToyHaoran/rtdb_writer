/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/rpc"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	host     string
	port     string
	user     string
	password string
)
var baseRoot = "root.test"
var sessionPool client.SessionPool
var testID int
var filePath string
var periodic bool

func main() {
	flag.StringVar(&host, "host", "xty111", "--host=192.168.150.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.IntVar(&testID, "testID", 0, "--testID=43")
	flag.BoolVar(&periodic, "periodic", false, "--periodic=true")
	flag.StringVar(&filePath, "filePath", "", "--filePath=../CSV20240614/1720063164967_ZHE_XIAN_QU_SHI.csv")
	flag.Parse()
	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	sessionPool = client.NewSessionPool(config, 20, 60000, 60000, false)

	rand.Seed(999)  // 固定随机种子

	switch testID {
	case 1:
		// 读取txt中的非查询SQL，执行SQL
		commonInsert(filePath)
	// 数据量 类型不一样，没法通用。
	case 43:
		insertData43(filePath)
	case 44:
		insertData44(filePath)
	case 45:
		insertData45(filePath)
	case 46:
		insertData46(filePath)
	case 614:
		measurements := []string{"DATA"}
		dataTypes := []client.TSDataType{client.INT32}
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		interval := time.Second
		timeStart2, err := time.Parse("2006-01-02 15:04:05", "2021-01-01 00:00:00")
		checkError(nil, err)
		device := baseRoot + ".d614"

		go createAndInsertData(100, timeStart, interval,device, measurements, dataTypes)
		time.Sleep(time.Second * 2)
		createAndInsertData(100, timeStart2, interval, device, measurements, dataTypes)
	case 615:
		measurements := []string{"s1", "s2", "s3", "s4", "s5",}
		dataTypes := []client.TSDataType{client.INT32, client.INT32, client.INT32, client.INT32, client.INT32}
		interval := time.Second
		device := baseRoot + ".d615"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertData(100, timeStart, interval, device, measurements, dataTypes)
	case 618:
		measurements := []string{"s1", "s2", "s3",}
		dataTypes := []client.TSDataType{client.INT32, client.INT32, client.INT32}
		interval := time.Second
		device := baseRoot + ".d618"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertDataRandom100(10, timeStart, interval, device, measurements, dataTypes)
	case 6112:
		measurements := []string{"s1",}
		dataTypes := []client.TSDataType{client.INT32}
		interval := time.Second
		device := baseRoot + ".d6112"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2024-06-28 00:00:00")
		checkError(nil, err)
		createAndInsertData(2*30*24*3600, timeStart, interval, device, measurements, dataTypes)
	default:
		fmt.Println("输入错误，无法执行")
	}

	sessionPool.Close()
}

func insertData43(csvPath string) {
	file, err := os.Open(csvPath)
	checkError(nil, err)
	csvReader := csv.NewReader(file)
	device := baseRoot + ".d43"
	measurementSchemas := []*client.MeasurementSchema{
		{
			Measurement: "DATA",
			DataType:    client.DOUBLE,
		},
	}
	tablet, err := client.NewTablet(device, measurementSchemas, 100000)
	checkError(nil, err)
	rowNum := 0
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(rec) == 0 || rec[0] == "TIME" {
			continue // 去除首行和空行
		}
		timestamp, err := strconv.ParseInt(rec[0], 10, 64)
		checkError(nil, err)
		data, err := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
		checkError(nil, err)
		tablet.SetTimestamp(timestamp*1000000, rowNum)
		err = tablet.SetValueAt(data, 0, rowNum)
		checkError(nil, err)
		tablet.RowSize++
		rowNum++
	}
	session, _ := sessionPool.GetSession()
	checkError(session.InsertTablet(tablet, false))
	sessionPool.PutBack(session)
	err = file.Close()
	checkError(nil, err)
}

func insertData44(csvPath string) {
	file, err := os.Open(csvPath)
	checkError(nil, err)
	csvReader := csv.NewReader(file)
	device := baseRoot + ".d44"
	measurementSchemas := []*client.MeasurementSchema{
		{
			Measurement: "DATA",
			DataType:    client.DOUBLE,
		},
	}
	tablet, err := client.NewTablet(device, measurementSchemas, 100000)
	checkError(nil, err)
	rowNum := 0
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(rec) == 0 || rec[0] == "TIME" {
			continue // 去除首行和空行
		}
		timestamp, err := strconv.ParseInt(rec[0], 10, 64)
		checkError(nil, err)
		data, err := strconv.ParseFloat(strings.TrimSpace(rec[1]), 64)
		checkError(nil, err)
		tablet.SetTimestamp(timestamp*1000000, rowNum)
		err = tablet.SetValueAt(data, 0, rowNum)
		checkError(nil, err)
		tablet.RowSize++
		rowNum++
	}
	session, _ := sessionPool.GetSession()
	checkError(session.InsertTablet(tablet, false))
	sessionPool.PutBack(session)
	err = file.Close()
	checkError(nil, err)
}

func insertData45(csvPath string) {
	file, err := os.Open(csvPath)
	checkError(nil, err)
	csvReader := csv.NewReader(file)
	device := baseRoot + ".d45"
	measurementSchemas := []*client.MeasurementSchema{
		{
			Measurement: "DATA_POINT_1",
			DataType:    client.INT64,
		},
		{
			Measurement: "DATA_POINT_2",
			DataType:    client.INT64,
		},
		{
			Measurement: "DATA_POINT_3",
			DataType:    client.INT64,
		},
	}
	tablet, err := client.NewTablet(device, measurementSchemas, 105)
	checkError(nil, err)
	rowNum := 0
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(rec) == 0 || rec[0] == "TIME" {
			continue // 去除首行和空行
		}
		timestamp, err := strconv.ParseInt(rec[0], 10, 64)
		checkError(nil, err)
		tablet.SetTimestamp(timestamp*1000000, rowNum)
		for i := 1; i <= 3; i++ {
			data, err := strconv.ParseInt(strings.TrimSpace(rec[i]), 10, 64)
			checkError(nil, err)
			err = tablet.SetValueAt(data, i-1, rowNum)
			checkError(nil, err)
		}
		tablet.RowSize++
		rowNum++
	}
	session, _ := sessionPool.GetSession()
	checkError(session.InsertTablet(tablet, false))
	sessionPool.PutBack(session)
	err = file.Close()
	checkError(nil, err)
}

func insertData46(csvPath string) {
	file, err := os.Open(csvPath)
	checkError(nil, err)
	csvReader := csv.NewReader(file)
	var device string
	if strings.Contains(csvPath, "A.csv") {
		device = baseRoot + ".d46A"
	} else {
		device = baseRoot + ".d46B"
	}

	measurementSchemas := []*client.MeasurementSchema{
		{
			Measurement: "DATA",
			DataType:    client.INT64,
		},
	}
	tablet, err := client.NewTablet(device, measurementSchemas, 40)
	checkError(nil, err)
	rowNum := 0
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(rec) == 0 || rec[0] == "TIME" {
			continue // 去除首行和空行
		}
		timestamp, err := strconv.ParseInt(rec[0], 10, 64)
		checkError(nil, err)
		data, err := strconv.ParseInt(strings.TrimSpace(rec[1]), 10, 64)
		checkError(nil, err)
		tablet.SetTimestamp(timestamp*1000000, rowNum)
		err = tablet.SetValueAt(data, 0, rowNum)
		checkError(nil, err)
		tablet.RowSize++
		rowNum++
	}
	session, _ := sessionPool.GetSession()
	checkError(session.InsertTablet(tablet, false))
	sessionPool.PutBack(session)
	err = file.Close()
	checkError(nil, err)
}

func commonInsert(filePath string) {
	file, err := os.Open(filePath)
	checkError(nil, err)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue // 跳过空行和注释行
		}
		session, err := sessionPool.GetSession()
		checkError(nil, err)
		checkError(session.ExecuteNonQueryStatement(line))
		sessionPool.PutBack(session)
	}

	err = file.Close()
	checkError(nil, err)
}

func createAndInsertData(batchCount int, timeStart time.Time, interval time.Duration,device string, measurements []string, dataTypes []client.TSDataType) {
	columnCount := len(measurements)
	measurementSchemas := make([]*client.MeasurementSchema, columnCount)
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}

	// 是否周期性写入
	if periodic {
		for i := 0; i < batchCount; i++ {
			time.Sleep(interval) // 间隔时间
			tablet, err := client.NewTablet(device, measurementSchemas, 1)
			timestamp := timeStart.Add(interval * time.Duration(i)).UnixNano()
			// 每个tablet只有一行数据
			tablet.SetTimestamp(timestamp, 0)
			for col := 0; col < columnCount; col++ {
				err = tablet.SetValueAt(rand.Int31(), col, 0)
				checkError(nil, err)
			}
			tablet.RowSize++

			fmt.Println("insert time:", time.Unix(timestamp/1e9, 0))

			session, err := sessionPool.GetSession()
			checkError(nil, err)
			checkError(session.InsertTablet(tablet, false))
			sessionPool.PutBack(session)
		}
	}else {
		// 极速写入
		tablet, err := client.NewTablet(device, measurementSchemas, batchCount)
		checkError(nil, err)
		for i := 0; i < batchCount; i++ {
			timestamp := timeStart.Add(interval * time.Duration(i)).UnixNano()
			// 每个tablet有batchCount行数据
			tablet.SetTimestamp(timestamp, i)
			for col := 0; col < columnCount; col++ {
				err = tablet.SetValueAt(rand.Int31(), col, i)
				checkError(nil, err)
			}
			tablet.RowSize++
		}
		session, err := sessionPool.GetSession()
		checkError(nil, err)
		checkError(session.InsertTablet(tablet, false))
		fmt.Println("插入批次", batchCount)
		sessionPool.PutBack(session)
	}
}


func createAndInsertDataRandom100(batchCount int, timeStart time.Time, interval time.Duration,device string, measurements []string, dataTypes []client.TSDataType) {
	columnCount := len(measurements)
	measurementSchemas := make([]*client.MeasurementSchema, columnCount)
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}

	for i := 0; i < batchCount; i++ {
		tablet, err := client.NewTablet(device, measurementSchemas, 1)
		timestamp := timeStart.Add(interval * time.Duration(i)).UnixNano()
		tablet.SetTimestamp(timestamp, 0)
		for col := 0; col < columnCount; col++ {
			err = tablet.SetValueAt(int32(rand.Intn(100)), col, 0)
			checkError(nil, err)
		}
		tablet.RowSize++
		//fmt.Println("insert time:", time.Unix(timestamp/1e9, 0))
		session, err := sessionPool.GetSession()
		checkError(nil, err)
		checkError(session.InsertTablet(tablet, false))
		sessionPool.PutBack(session)
	}
}

func checkError(status *rpc.TSStatus, err error) {
	if err != nil {
		log.Fatal(err)
	}

	if status != nil {
		if err = client.VerifySuccess(status); err != nil {
			log.Println(err)
		}
	}
}

// If your IotDB is a cluster version or doubleLive, you can use the following code for session pool connection
func useSessionPool() {

	config := &client.PoolConfig{
		UserName: user,
		Password: password,
		NodeUrls: strings.Split("127.0.0.1:6667,127.0.0.1:6668", ","),
	}
	sessionPool = client.NewSessionPool(config, 3, 60000, 60000, false)
	defer sessionPool.Close()
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err != nil {
		log.Print(err)
		return
	}

}
