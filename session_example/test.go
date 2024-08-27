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
	"math"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
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
var exportSh = "/opt/soft/iotdb-enterprise-1.3.3.1-bin/tools/export-data.sh"
var startTimestamp int64 // 开始的时间戳
var startTime string     // 开始的时间字符串
var fastSectionCount int64
var normalSectionCount int64

func main() {
	flag.StringVar(&host, "host", "192.168.11.102", "--host=192.168.150.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.IntVar(&testID, "testID", 0, "--testID=43")
	flag.BoolVar(&periodic, "periodic", false, "--periodic=true")
	flag.StringVar(&filePath, "filePath", "", "--filePath=../CSV/XXX.csv")
	flag.StringVar(&startTime, "startTime", "", "--startTimes=1970-03-02T08:00:00")
	flag.Int64Var(&startTimestamp, "startTimestamp", 0, "--startTimestamp=0")
	flag.Int64Var(&fastSectionCount, "fastSectionCount", 0, "--fastSectionCount=0")
	flag.Int64Var(&normalSectionCount, "normalSectionCount", 0, "--normalSectionCount=0")
	flag.Parse()
	if startTime != "" {
		// 将开始时间字符串转为时间戳
		stamp, _ := time.ParseInLocation("2006-01-02T15:04:05", startTime, time.Local)
		startTimestamp = stamp.UnixMilli()
	}
	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	sessionPool = client.NewSessionPool(config, 200, 60000, 60000, false)

	rand.Seed(999) // 固定随机种子

	// 1~669都是对应测试用例。991～993用来生成测试数据，无用。
	switch testID {
	case 1:
		// 读取txt中的非查询SQL，执行SQL
		commonInsert(filePath)
	case 43:
		insertData43(filePath)
	case 44:
		insertData44(filePath)
	case 45:
		insertData45(filePath)
	case 46:
		insertData46(filePath)
	case 613:
		// ./test --testID=613 --startTimestamp=5183940400 --filePath=../CSV/testHisAnalog.csv,../CSV/testHisDigital.csv
		// 验证1号测点最近1分钟是否一致，CMD命令可以手动执行。
		// 先删除已导出的数据 或 创建新目录
		//RunCommand("rm -rf ../CSV2/*")
		RunCommand("mkdir -p ../CSV2/his")
		// 导出IOTDB数据
		RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/his -s ./sqlfile/613.sql -tf timestamp -lpf 50000")
		//// 原始数据太大，用tail将最后n行导出。 tail -n 30000 ../CSV/testHisAnalog.csv > ../CSV2/his/testHisAnalog.csv
		sourceFiles := strings.Split(filePath, ",")
		cmd1 := exec.Command("bash", "-c", "tail -n 20000 "+sourceFiles[0]+" | tr -d '\\r'") //  2.5*60秒*30测点
		cmd1.Stdout, _ = os.Create("../CSV2/his/testHisAnalog.csv")
		cmd1.Run()
		cmd2 := exec.Command("bash", "-c", "tail -n 20000 "+sourceFiles[1]+" | tr -d '\\r'") //  2.5*60秒*70测点
		cmd2.Stdout, _ = os.Create("../CSV2/his/testHisDigital.csv")
		cmd2.Run()
		// 逐条对比模拟数据
		fmt.Println("开始对比Analog数据...")
		verifyAnalogData("../CSV2/his/dump0_0.csv", "../CSV2/his/testHisAnalog.csv")
		fmt.Println("开始对比Digital数据...")
		verifyDigitalData("../CSV2/his/dump1_0.csv", "../CSV2/his/testHisDigital.csv")

	case 614:
		measurements := []string{"DATA"}
		dataTypes := []client.TSDataType{client.INT32}
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		interval := time.Second
		timeStart2, err := time.Parse("2006-01-02 15:04:05", "2021-01-01 00:00:00")
		checkError(nil, err)
		device := baseRoot + ".d614"

		go createAndInsertData(100, timeStart, interval, device, measurements, dataTypes)
		time.Sleep(time.Second * 2)
		createAndInsertData(100, timeStart2, interval, device, measurements, dataTypes)
	case 615:
		measurements := []string{"s1", "s2", "s3", "s4", "s5"}
		dataTypes := []client.TSDataType{client.INT32, client.INT32, client.INT32, client.INT32, client.INT32}
		interval := time.Second
		device := baseRoot + ".d615"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertData(100, timeStart, interval, device, measurements, dataTypes)
	case 618:
		measurements := []string{"s1", "s2", "s3"}
		dataTypes := []client.TSDataType{client.INT32, client.INT32, client.INT32}
		interval := time.Second
		device := baseRoot + ".d618"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2020-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertDataRandom100(10, timeStart, interval, device, measurements, dataTypes)
	case 619:
		commonInsert("./sqlfile/619-1")
		time.Sleep(time.Second * 10)
		commonInsert("./sqlfile/619-2")
	case 6112:
		measurements := []string{"s1"}
		dataTypes := []client.TSDataType{client.INT32}
		interval := time.Second
		device := baseRoot + ".d6112"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2024-06-28 00:00:00")
		checkError(nil, err)
		createAndInsertData(2*30*24*3600, timeStart, interval, device, measurements, dataTypes)
	case 6114:
		// ./test --testID=6114 --host=192.168.1.112 --filePath=../CSV/1721454092945_HISTORY_NORMAL_ANALOG.csv,../CSV/1721454092945_HISTORY_NORMAL_DIGITAL.csv
		// 先删除已导出的数据 或 创建新目录
		RunCommand("mkdir -p ../CSV2/his96")
		// 导出IOTDB数据，分时间区间导出。
		RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/his96 -s ./sqlfile/6114.sql -tf timestamp -lpf 40000000")
		// 合并文件（一个600M）
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump1_0.csv >> ../CSV2/his96/dump0_0.csv && rm -rf ../CSV2/his96/dump1_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump2_0.csv >> ../CSV2/his96/dump0_0.csv && rm -rf ../CSV2/his96/dump2_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump3_0.csv >> ../CSV2/his96/dump0_0.csv && rm -rf ../CSV2/his96/dump3_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump5_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump5_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump6_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump6_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump7_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump7_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump8_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump8_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump9_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump9_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump10_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump10_0.csv").Run()
		exec.Command("bash", "-c", "cat ../CSV2/his96/dump11_0.csv >> ../CSV2/his96/dump4_0.csv && rm -rf ../CSV2/his96/dump11_0.csv").Run()
		fmt.Println("合并文件完成...")
		// 原始数据太大，用head将前面n行导出。2.5*60秒*60*24*4*30或70
		sourceFiles := strings.Split(filePath, ",")
		cmd1 := exec.Command("bash", "-c", "head -n 26000000 "+sourceFiles[0]+" | tr -d '\\r'")
		cmd1.Stdout, _ = os.Create("../CSV2/his96/testHisAnalog.csv")
		cmd1.Run()
		cmd2 := exec.Command("bash", "-c", "head -n 62000000 "+sourceFiles[1]+" | tr -d '\\r'")
		cmd2.Stdout, _ = os.Create("../CSV2/his96/testHisDigital.csv")
		cmd2.Run()
		fmt.Println("导出原始数据完成...")
		// 逐条对比模拟数据
		fmt.Println("开始对比Analog数据...")
		verifyAnalogDataHis96("../CSV2/his96/dump0_0.csv", "../CSV2/his96/testHisAnalog.csv")
		fmt.Println("开始对比Digital数据...")
		verifyDigitalDataHis96("../CSV2/his96/dump4_0.csv", "../CSV2/his96/testHisDigital.csv")

	case 621:
		fallthrough
	case 622:
		// 验证实时数据集是否一致
		// ./test --testID=621 --startTimestamp=3590000 --filePath=../CSV/testFastAnalog.csv,../CSV/testFastDigital.csv,../CSV/testNormalAnalog.csv,../CSV/testNormalDigital.csv
		// 先删除已导出的数据 或 创建新目录
		RunCommand("mkdir -p ../CSV2/fast")
		RunCommand("mkdir -p ../CSV2/normal")
		sourceFiles := strings.Split(filePath, ",")
		if len(sourceFiles) == 4 || (len(sourceFiles) == 2 && strings.Contains(sourceFiles[0], "FAST")) {
			// 导出IOTDB数据
			RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/fast -s ./sqlfile/621fast.sql -tf timestamp -lpf 4000000")
			cmd1 := exec.Command("bash", "-c", "tail -n 1550000 "+sourceFiles[0]+" | tr -d '\\r'") //  1000*10秒*150测点
			cmd1.Stdout, _ = os.Create("../CSV2/fast/testFastAnalog.csv")
			cmd1.Run()
			cmd2 := exec.Command("bash", "-c", "tail -n 3550000 "+sourceFiles[1]+" | tr -d '\\r'") //  1000*10秒*350测点
			cmd2.Stdout, _ = os.Create("../CSV2/fast/testFastDigital.csv")
			cmd2.Run()
			fmt.Println("开始对比FastAnalog数据...")
			verifyAnalogData("../CSV2/fast/dump0_0.csv", "../CSV2/fast/testFastAnalog.csv")
			fmt.Println("开始对比FastDigital数据...")
			verifyDigitalData("../CSV2/fast/dump1_0.csv", "../CSV2/fast/testFastDigital.csv")
		}
		if len(sourceFiles) == 4 || (len(sourceFiles) == 2 && strings.Contains(sourceFiles[0], "NORMAL")) {
			var src0, src1 string
			if len(sourceFiles) == 4 {
				src0 = sourceFiles[2]
				src1 = sourceFiles[3]
			} else {
				src0 = sourceFiles[0]
				src1 = sourceFiles[1]
			}
			RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/normal -s ./sqlfile/621normal.sql -tf timestamp -lpf 4000000")
			cmd3 := exec.Command("bash", "-c", "tail -n 1550000 "+src0+" | tr -d '\\r'") //  2.5*10秒*60000测点
			cmd3.Stdout, _ = os.Create("../CSV2/normal/testNormalAnalog.csv")
			cmd3.Run()
			cmd4 := exec.Command("bash", "-c", "tail -n 3550000 "+src1+" | tr -d '\\r'") //  2.5*10秒*140000测点
			cmd4.Stdout, _ = os.Create("../CSV2/normal/testNormalDigital.csv")
			cmd4.Run()
			fmt.Println("开始对比NormalAnalog数据...")
			verifyAnalogData("../CSV2/normal/dump0_0.csv", "../CSV2/normal/testNormalAnalog.csv")
			fmt.Println("开始对比NormalDigital数据...")
			verifyDigitalData("../CSV2/normal/dump1_0.csv", "../CSV2/normal/testNormalDigital.csv")
		}
	case 6281:
		// 循环创建测点
		createMeasurements(true)
	case 6282:
		createMeasurements(false)
	case 651:
		// 验证实时数据集是否一致，根据任意断面数量验证，速度较慢
		// ./test --testID=651 --host=192.168.1.112 --fastSectionCount=40400 --normalSectionCount=102 --filePath=../CSV/1721454092945_REALTIME_FAST_ANALOG.csv,../CSV/1721454092945_REALTIME_FAST_DIGITAL.csv,../CSV/1721454092945_REALTIME_NORMAL_ANALOG.csv,../CSV/1721454092945_REALTIME_NORMAL_DIGITAL.csv
		// 先删除已导出的数据 或 创建新目录
		RunCommand("mkdir -p ../CSV2/record100")
		sourceFiles := strings.Split(filePath, ",")
		// 导出IOTDB数据
		RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/record100 -s ./sqlfile/651.sql -tf timestamp -lpf 400")
		// 导出源数据(因为数据起始和条数不同，需要最大参数)
		fastAEnd := fastSectionCount*150 + 1
		fastAStart := fastAEnd - 100
		fastDEnd := fastSectionCount*350 + 1
		fastDStart := fastDEnd - 100
		normalAEnd := normalSectionCount*59850 + 1
		normalAStart := normalAEnd - 100
		normalDEnd := normalSectionCount*139650 + 1
		normalDStart := normalDEnd - 100
		var wg4 sync.WaitGroup
		wg4.Add(4)
		go func() {
			exec.Command("bash", "-c", "sed -n '"+strconv.FormatInt(fastAStart, 10)+","+strconv.FormatInt(fastAEnd, 10)+"p' "+sourceFiles[0]+" | tr -d '\\r' | tac  > ../CSV2/record100/testFastAnalog.csv").Run()
			fmt.Println("导出FastAnalog数据完成。")
			wg4.Done()
		}()
		go func() {
			exec.Command("bash", "-c", "sed -n '"+strconv.FormatInt(fastDStart, 10)+","+strconv.FormatInt(fastDEnd, 10)+"p' "+sourceFiles[1]+" | tr -d '\\r' | tac  > ../CSV2/record100/testFastDigital.csv").Run()
			fmt.Println("导出FastDigital数据完成。")
			wg4.Done()
		}()
		go func() {
			exec.Command("bash", "-c", "sed -n '"+strconv.FormatInt(normalAStart, 10)+","+strconv.FormatInt(normalAEnd, 10)+"p' "+sourceFiles[2]+" | tr -d '\\r' | tac  > ../CSV2/record100/testNormalAnalog.csv").Run()
			fmt.Println("导出NormalAnalog数据完成。")
			wg4.Done()
		}()
		go func() {
			exec.Command("bash", "-c", "sed -n '"+strconv.FormatInt(normalDStart, 10)+","+strconv.FormatInt(normalDEnd, 10)+"p' "+sourceFiles[3]+" | tr -d '\\r' | tac  > ../CSV2/record100/testNormalDigital.csv").Run()
			fmt.Println("导出NormalDigital数据完成。")
			wg4.Done()
		}()
		wg4.Wait()
		fmt.Println("开始对比FastAnalog数据...")
		verifyAnalogData("../CSV2/record100/dump0_0.csv", "../CSV2/record100/testFastAnalog.csv")
		fmt.Println("开始对比FastDigital数据...")
		verifyDigitalData("../CSV2/record100/dump1_0.csv", "../CSV2/record100/testFastDigital.csv")
		fmt.Println("开始对比NormalAnalog数据...")
		verifyAnalogData("../CSV2/record100/dump2_0.csv", "../CSV2/record100/testNormalAnalog.csv")
		fmt.Println("开始对比NormalDigital数据...")
		verifyDigitalData("../CSV2/record100/dump3_0.csv", "../CSV2/record100/testNormalDigital.csv")
	case 6511:
		// 验证实时数据集是否一致，最后100条数据验证。
		// ./test --testID=6511 --host=192.168.1.102 --filePath=../CSV/1721454092945_REALTIME_FAST_ANALOG.csv,../CSV/1721454092945_REALTIME_FAST_DIGITAL.csv,../CSV/1721454092945_REALTIME_NORMAL_ANALOG.csv,../CSV/1721454092945_REALTIME_NORMAL_DIGITAL.csv
		// 先删除已导出的数据 或 创建新目录
		RunCommand("mkdir -p ../CSV2/record100")
		sourceFiles := strings.Split(filePath, ",")
		// 导出IOTDB数据
		RunCommand(exportSh + " -h " + host + " -p 6667 -u root -pw root -t ../CSV2/record100 -s ./sqlfile/651.sql -tf timestamp -lpf 400")
		// 导出源数据(因为数据起始和条数不同，需要最大参数)
		exec.Command("bash", "-c", "tail -n 100 "+sourceFiles[0]+" | tr -d '\\r' | tac  > ../CSV2/record100/testFastAnalog.csv").Run()
		exec.Command("bash", "-c", "tail -n 100 "+sourceFiles[1]+" | tr -d '\\r' | tac  > ../CSV2/record100/testFastDigital.csv").Run()
		exec.Command("bash", "-c", "tail -n 100 "+sourceFiles[2]+" | tr -d '\\r' | tac  > ../CSV2/record100/testNormalAnalog.csv").Run()
		exec.Command("bash", "-c", "tail -n 100 "+sourceFiles[3]+" | tr -d '\\r' | tac  > ../CSV2/record100/testNormalDigital.csv").Run()
		fmt.Println("开始对比FastAnalog数据...")
		verifyAnalogData("../CSV2/record100/dump0_0.csv", "../CSV2/record100/testFastAnalog.csv")
		fmt.Println("开始对比FastDigital数据...")
		verifyDigitalData("../CSV2/record100/dump1_0.csv", "../CSV2/record100/testFastDigital.csv")
		fmt.Println("开始对比NormalAnalog数据...")
		verifyAnalogData("../CSV2/record100/dump2_0.csv", "../CSV2/record100/testNormalAnalog.csv")
		fmt.Println("开始对比NormalDigital数据...")
		verifyDigitalData("../CSV2/record100/dump3_0.csv", "../CSV2/record100/testNormalDigital.csv")
	case 667:
		measurements := []string{"s1"}
		dataTypes := []client.TSDataType{client.INT32}
		interval := time.Second
		device := baseRoot + ".d667"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2024-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertData(100000, timeStart, interval, device, measurements, dataTypes)
	case 668:
		measurements := []string{"s1"}
		dataTypes := []client.TSDataType{client.INT32}
		interval := time.Second
		device := baseRoot + ".d668"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2024-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertData(100000, timeStart, interval, device, measurements, dataTypes)
	case 669:
		measurements := []string{"s1"}
		dataTypes := []client.TSDataType{client.INT32}
		interval := time.Second
		device := baseRoot + ".d669"
		timeStart, err := time.Parse("2006-01-02 15:04:05", "2024-01-01 00:00:00")
		checkError(nil, err)
		createAndInsertData(600, timeStart, interval, device, measurements, dataTypes)
	case 991:
		// 制造历史模拟量
		initAnalogData := []string{
			",1,301.86736935689703,301.86736935689703,False,True,True,301.86736935689703,False,d,27294",
			",2,300.7082776433076,300.7082776433076,True,False,False,300.7082776433076,True,e,12809",
			",3,302.8205594044708,302.8205594044708,False,False,True,302.8205594044708,True,j,21709",
			",4,301.671979447537,301.671979447537,False,True,False,301.671979447537,False,d,56557",
			",5,301.906227389479,301.906227389479,False,True,True,301.906227389479,True,m,25308",
			",6,300.78442366384627,300.78442366384627,True,False,False,300.78442366384627,True,x,59005",
			",7,300.04856621726435,300.04856621726435,True,True,False,300.04856621726435,False,i,5561",
			",8,300.1299248317014,300.1299248317014,True,False,False,300.1299248317014,True,o,52405",
			",9,302.0507177004741,302.0507177004741,False,True,False,302.0507177004741,False,w,10132",
			",10,300.03306808594095,300.03306808594095,False,False,True,300.03306808594095,False,e,29804",
			",11,301.01573412930315,301.01573412930315,True,True,False,301.01573412930315,False,j,64664",
			",12,302.99362164921297,302.99362164921297,True,True,True,302.99362164921297,True,e,21256",
			",13,301.17429758316536,301.17429758316536,True,False,False,301.17429758316536,False,b,31278",
			",14,300.3143119632131,300.3143119632131,False,False,False,300.3143119632131,False,a,1375",
			",15,302.3813341945054,302.3813341945054,False,False,True,302.3813341945054,True,p,8418",
			",16,302.8588373248654,302.8588373248654,True,False,False,302.8588373248654,True,s,13077",
			",17,300.27924809862174,300.27924809862174,True,False,True,300.27924809862174,False,a,26515",
			",18,301.7361976834379,301.7361976834379,False,True,True,301.7361976834379,False,n,4870",
			",19,302.8115441496715,302.8115441496715,True,False,True,302.8115441496715,False,j,59301",
			",20,300.9242988810998,300.9242988810998,False,False,True,300.9242988810998,False,i,157",
			",21,302.83227500076845,302.83227500076845,True,False,False,302.83227500076845,True,t,30221",
			",22,300.2347609573683,300.2347609573683,False,True,True,300.2347609573683,False,g,25264",
			",23,302.09232265085814,302.09232265085814,True,True,True,302.09232265085814,True,c,41770",
			",24,301.48743731924674,301.48743731924674,False,False,True,301.48743731924674,True,l,30692",
			",25,301.2165711825047,301.2165711825047,False,True,False,301.2165711825047,False,o,7025",
			",26,300.56672785601404,300.56672785601404,False,False,True,300.56672785601404,True,t,23476",
			",27,302.3211582116006,302.3211582116006,False,True,False,302.3211582116006,False,q,22233",
			",28,302.07673619612865,302.07673619612865,True,True,True,302.07673619612865,False,h,60000",
			",29,301.33380873635116,301.33380873635116,False,False,True,301.33380873635116,False,w,37505",
			",30,301.0018884568768,301.0018884568768,True,True,True,301.0018884568768,False,j,7051",
		}
		// 制造历史数字量
		initDigitalData := []string{
			",1,True,True,True,False,False,True,False,w,33645",
			",2,True,True,False,True,True,True,False,r,50283",
			",3,True,True,False,True,False,True,False,m,58208",
			",4,True,True,True,False,False,True,True,c,24005",
			",5,False,False,True,True,False,False,True,x,15204",
			",6,True,True,False,True,False,True,False,a,46096",
			",7,True,True,True,True,True,True,False,z,30373",
			",8,True,True,True,False,False,True,False,n,30888",
			",9,True,True,False,True,False,True,False,m,61999",
			",10,True,True,False,True,False,True,False,z,29111",
			",11,True,True,False,False,True,True,True,d,11845",
			",12,False,False,True,False,False,False,False,c,47081",
			",13,False,False,False,True,True,False,False,u,33782",
			",14,True,True,False,False,False,True,False,y,49515",
			",15,True,True,False,True,True,True,True,l,45631",
			",16,True,True,False,True,True,True,False,r,45638",
			",17,False,False,False,False,True,False,True,q,4247",
			",18,True,True,False,False,False,True,False,q,20511",
			",19,False,False,True,False,False,False,False,v,11755",
			",20,True,True,False,True,True,True,False,z,19022",
			",21,True,True,False,False,False,True,True,g,5854",
			",22,True,True,True,True,False,True,True,g,408",
			",23,True,True,True,False,True,True,True,a,47421",
			",24,True,True,False,False,False,True,False,g,50101",
			",25,True,True,False,False,True,True,True,o,54745",
			",26,True,True,True,True,False,True,False,p,5298",
			",27,False,False,True,True,True,False,True,k,8105",
			",28,True,True,True,False,False,True,True,v,53542",
			",29,True,True,True,True,False,True,False,q,56291",
			",30,False,False,True,True,True,False,True,m,49222",
			",31,True,True,True,True,True,True,False,n,18457",
			",32,False,False,True,False,True,False,True,d,18464",
			",33,True,True,False,False,False,True,False,b,7263",
			",34,True,True,False,False,True,True,False,y,52509",
			",35,True,True,True,True,True,True,True,s,10491",
			",36,True,True,True,True,False,True,True,h,4117",
			",37,True,True,False,False,False,True,False,f,52380",
			",38,True,True,True,True,True,True,True,n,35645",
			",39,True,True,True,True,False,True,False,f,17814",
			",40,True,True,False,True,True,True,False,x,3008",
			",41,True,True,False,True,True,True,False,d,34725",
			",42,False,False,False,True,False,False,False,n,58587",
			",43,True,True,True,False,False,True,False,e,58292",
			",44,True,True,True,True,False,True,False,i,63798",
			",45,True,True,False,True,False,True,True,n,58730",
			",46,True,True,False,False,True,True,False,e,63403",
			",47,True,True,True,False,True,True,True,c,16616",
			",48,True,True,True,False,False,True,False,i,35680",
			",49,False,False,False,True,False,False,True,d,57893",
			",50,False,False,False,True,True,False,False,d,19265",
			",51,True,True,False,True,False,True,False,a,20549",
			",52,False,False,True,False,False,False,False,b,6311",
			",53,False,False,True,False,True,False,False,k,53123",
			",54,False,False,True,False,True,False,True,o,2915",
			",55,True,True,True,False,False,True,False,w,2954",
			",56,True,True,True,True,True,True,True,r,44885",
			",57,True,True,True,True,False,True,False,l,734",
			",58,False,False,False,True,True,False,False,s,65475",
			",59,True,True,False,False,False,True,True,p,8640",
			",60,True,True,False,False,True,True,True,g,11119",
			",61,True,True,True,False,False,True,False,l,16811",
			",62,True,True,False,False,False,True,False,u,7419",
			",63,True,True,False,False,True,True,False,v,48079",
			",64,False,False,True,False,False,False,True,w,34156",
			",65,True,True,False,False,False,True,True,t,48200",
			",66,True,True,True,False,False,True,False,j,51268",
			",67,False,False,True,True,True,False,True,a,18769",
			",68,True,True,True,True,False,True,False,y,26140",
			",69,False,False,False,False,False,False,True,f,17753",
			",70,True,True,False,True,False,True,True,m,57305",
		}
		count := 60 * 24 * 60 * 60 * 1000
		// 打开文件（以追加模式）
		go func() {
			file1, _ := os.OpenFile("../CSV/testHisAnalog.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			writer1 := bufio.NewWriter(file1) // 创建一个缓冲写入器
			interval := 400
			for t := 0; t < count; t += interval {
				for _, line := range initAnalogData {
					writer1.WriteString(strconv.FormatInt(int64(t), 10) + line + "\n")
				}
				writer1.Flush()
			}
			file1.Close()
		}()

		file2, _ := os.OpenFile("../CSV/testHisDigital.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		writer2 := bufio.NewWriter(file2) // 创建一个缓冲写入器
		interval := 400
		for t := 0; t < count; t += interval {
			for _, line := range initDigitalData {
				writer2.WriteString(strconv.FormatInt(int64(t), 10) + line + "\n")
			}
			writer2.Flush() // 刷新缓冲区
		}
		file2.Close()
	case 992:
		// 快采点模拟量
		initFastAnalog := []string{
			",300.51895612926927,300.51895612926927,False,True,True,300.51895612926927,True,b,17887",
			",301.3360220914003,301.3360220914003,False,False,False,301.3360220914003,True,a,17986",
			",301.7538845756861,301.7538845756861,False,True,False,301.7538845756861,False,l,9191",
			",300.2033427340847,300.2033427340847,True,False,True,300.2033427340847,True,x,6441",
			",300.78892701638114,300.78892701638114,False,True,False,300.78892701638114,True,w,23669",
			",301.84380991325435,301.84380991325435,True,False,False,301.84380991325435,False,f,18677",
			",300.4005546405889,300.4005546405889,True,True,False,300.4005546405889,True,y,59996",
			",300.1198801800455,300.1198801800455,False,False,True,300.1198801800455,True,m,12539",
			",302.7308889001857,302.7308889001857,False,False,False,302.7308889001857,True,w,48375",
			",302.12840888273524,302.12840888273524,False,True,True,302.12840888273524,False,c,61816",
		}
		// 快采点数字量
		initFastDigital := []string{
			",True,True,False,True,True,True,True,z,16697",
			",True,True,True,True,False,True,True,i,39976",
			",True,True,False,True,True,True,False,q,55019",
			",True,True,False,True,True,True,True,w,109",
			",False,False,False,True,False,False,True,u,10951",
			",False,False,True,True,True,False,True,d,63064",
			",True,True,False,False,True,True,True,n,37486",
			",False,False,False,True,True,False,False,a,29801",
			",True,True,True,False,False,True,True,r,36900",
			",True,True,False,True,False,True,True,r,51645",
		}
		count := 1 * 60 * 60 * 1000
		interval := 1

		// 打开文件（以追加模式）
		go func() {
			file1, _ := os.OpenFile("../CSV/testFastAnalog.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			writer1 := bufio.NewWriter(file1) // 创建一个缓冲写入器
			for t := 0; t < count; t += interval {
				timestamp := strconv.FormatInt(int64(t), 10)
				for pnum := 1; pnum <= 150; pnum++ {
					writer1.WriteString(timestamp + "," + strconv.FormatInt(int64(pnum), 10) + initFastAnalog[rand.Intn(10)] + "\n")
				}
				writer1.Flush()
			}
			file1.Close()
		}()

		file2, _ := os.OpenFile("../CSV/testFastDigital.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		writer2 := bufio.NewWriter(file2) // 创建一个缓冲写入器
		for t := 0; t < count; t += interval {
			timestamp := strconv.FormatInt(int64(t), 10)
			for pnum := 1; pnum <= 350; pnum++ {
				writer2.WriteString(timestamp + "," + strconv.FormatInt(int64(pnum), 10) + initFastDigital[rand.Intn(10)] + "\n")
			}
			writer2.Flush() // 刷新缓冲区
		}
		file2.Close()
	case 993:
		// 普通点模拟量
		initNormalAnalog := []string{
			",300.51895612926927,300.51895612926927,False,True,True,300.51895612926927,True,b,17887",
			",301.3360220914003,301.3360220914003,False,False,False,301.3360220914003,True,a,17986",
			",301.7538845756861,301.7538845756861,False,True,False,301.7538845756861,False,l,9191",
			",300.2033427340847,300.2033427340847,True,False,True,300.2033427340847,True,x,6441",
			",300.78892701638114,300.78892701638114,False,True,False,300.78892701638114,True,w,23669",
			",301.84380991325435,301.84380991325435,True,False,False,301.84380991325435,False,f,18677",
			",300.4005546405889,300.4005546405889,True,True,False,300.4005546405889,True,y,59996",
			",300.1198801800455,300.1198801800455,False,False,True,300.1198801800455,True,m,12539",
			",302.7308889001857,302.7308889001857,False,False,False,302.7308889001857,True,w,48375",
			",302.12840888273524,302.12840888273524,False,True,True,302.12840888273524,False,c,61816",
		}
		// 普通点数字量
		initNormalDigital := []string{
			",True,True,False,True,True,True,True,z,16697",
			",True,True,True,True,False,True,True,i,39976",
			",True,True,False,True,True,True,False,q,55019",
			",True,True,False,True,True,True,True,w,109",
			",False,False,False,True,False,False,True,u,10951",
			",False,False,True,True,True,False,True,d,63064",
			",True,True,False,False,True,True,True,n,37486",
			",False,False,False,True,True,False,False,a,29801",
			",True,True,True,False,False,True,True,r,36900",
			",True,True,False,True,False,True,True,r,51645",
		}
		count := 1 * 60 * 60 * 1000
		interval := 400

		// 打开文件（以追加模式）
		go func() {
			file1, _ := os.OpenFile("../CSV/testNormalAnalog.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			writer1 := bufio.NewWriter(file1) // 创建一个缓冲写入器
			for t := 0; t < count; t += interval {
				timestamp := strconv.FormatInt(int64(t), 10)
				for pnum := 1; pnum <= 59850; pnum++ {
					writer1.WriteString(timestamp + "," + strconv.FormatInt(int64(pnum), 10) + initNormalAnalog[rand.Intn(10)] + "\n")
				}
				writer1.Flush()
			}
			file1.Close()
		}()

		file2, _ := os.OpenFile("../CSV/testNormalDigital.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		writer2 := bufio.NewWriter(file2) // 创建一个缓冲写入器
		for t := 0; t < count; t += interval {
			timestamp := strconv.FormatInt(int64(t), 10)
			for pnum := 1; pnum <= 139650; pnum++ {
				writer2.WriteString(timestamp + "," + strconv.FormatInt(int64(pnum), 10) + initNormalDigital[rand.Intn(10)] + "\n")
			}
			writer2.Flush() // 刷新缓冲区
		}
		file2.Close()

	default:
		fmt.Println("输入错误，无法执行")
	}

	sessionPool.Close()
}

func verifyAnalogData(exportFile string, sourceFile string) {
	file1, _ := os.Open(exportFile)
	file2, _ := os.Open(sourceFile)
	reader1 := csv.NewReader(file1)
	reader2 := csv.NewReader(file2)
	row1Ch := make(chan []string)
	row2Ch := make(chan []string)
	remain5 := func(row []string) {
		// 保留n位小数
		for _, index := range []int{2, 3, 7} {
			t1, _ := strconv.ParseFloat(row[index], 32)
			row[index] = fmt.Sprintf("%.5f", t1)
		}
	}
	// 处理导出的数据
	go func() {
		for {
			row1, err1 := reader1.Read()
			if err1 == io.EOF {
				close(row1Ch)
				file1.Close()
				return
			}
			if len(row1[0]) == 0 || row1[0] == "Time" {
				continue // 去除首行和空行
			}
			row1[0] = row1[0][:len(row1[0])-6] // 去掉时间列的ns值
			//去掉normal设备列
			if len(row1) == 12 {
				newRow := append(row1[:1], row1[2:]...)
				remain5(newRow)
				row1Ch <- newRow
			} else {
				remain5(row1)
				row1Ch <- row1
			}
		}
	}()
	boolLower := func(row []string) {
		// 将False True小写
		for _, index := range []int{4, 5, 6, 8} {
			row[index] = strings.ToLower(row[index])
		}
	}
	// 处理原始数据
	go func() {
		for {
			row2, err2 := reader2.Read()
			if err2 == io.EOF {
				close(row2Ch)
				file2.Close()
				return
			}
			if len(row2[0]) == 0 || row2[0] == "TIME" {
				continue // 去除首行和空行
			}
			if testID == 613 {
				// 1号测点
				if row2[1] == "1" {
					timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
					// 最近一分钟
					if timestamp >= startTimestamp {
						remain5(row2)
						boolLower(row2)
						row2Ch <- row2
					}
				}
			} else { // 621
				timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
				if timestamp >= startTimestamp {
					remain5(row2)
					boolLower(row2)
					row2Ch <- row2
				}
			}
		}
	}()
	// 检验
	verifyRecord(row1Ch, row2Ch)
}

func verifyDigitalData(exportFile string, sourceFile string) {
	file1, _ := os.Open(exportFile)
	file2, _ := os.Open(sourceFile)
	reader1 := csv.NewReader(file1)
	reader2 := csv.NewReader(file2)
	row1Ch := make(chan []string)
	row2Ch := make(chan []string)
	// 处理导出的数据
	go func() {
		for {
			row1, err1 := reader1.Read()
			if err1 == io.EOF {
				close(row1Ch)
				file1.Close()
				return
			}
			if len(row1[0]) == 0 || row1[0] == "Time" {
				continue // 去除首行和空行
			}
			row1[0] = row1[0][:len(row1[0])-6] // 去掉时间列的ns值
			//去掉normal设备列
			if len(row1) == 12 {
				newRow := append(row1[:1], row1[2:]...)
				row1Ch <- newRow
			} else {
				row1Ch <- row1
			}
		}
	}()
	boolLower := func(row []string) {
		// 将False True小写
		for _, index := range []int{2, 3, 4, 5, 6, 7, 8} {
			row[index] = strings.ToLower(row[index])
		}
	}
	// 处理原始数据
	go func() {
		for {
			row2, err2 := reader2.Read()
			if err2 == io.EOF {
				close(row2Ch)
				file2.Close()
				return
			}
			if len(row2[0]) == 0 || row2[0] == "TIME" {
				continue // 去除首行和空行
			}
			if testID == 613 {
				// 1号测点
				if row2[1] == "1" {
					timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
					// 开始时间
					if timestamp >= startTimestamp {
						boolLower(row2)
						row2Ch <- row2
					}
				}
			} else {
				timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
				// 开始时间
				if timestamp >= startTimestamp {
					boolLower(row2)
					row2Ch <- row2
				}
			}

		}
	}()
	// 检验
	verifyRecord(row1Ch, row2Ch)
}

// 96H历史数据量 因为数据量太大，无法导出的时候直接排序，导致和原始数据不对应，必须手动处理
// select P_NUM, AV, AVR, Q, BF, FQ, FAI, MS, TEW, CST from root.sg.unit0.historyA.d* where time < 1970-01-04T08:00:00 order by time asc, P_NUM asc align by device
func verifyAnalogDataHis96(exportFile string, sourceFile string) {
	file1, _ := os.Open(exportFile)
	file2, _ := os.Open(sourceFile)
	reader1 := csv.NewReader(file1)
	reader2 := csv.NewReader(file2)
	row1Ch := make(chan []string)
	row2Ch := make(chan []string)
	remain5 := func(row []string) {
		// 保留n位小数
		for _, index := range []int{2, 3, 7} {
			t1, _ := strconv.ParseFloat(row[index], 32)
			row[index] = fmt.Sprintf("%.5f", t1)
		}
	}
	// 处理导出的数据
	go func() {
		var rowTmp = make([][]string, 31) // 0不用
		var count = 1
		for {
			row1, err1 := reader1.Read()
			if err1 == io.EOF {
				close(row1Ch)
				file1.Close()
				return
			}
			if len(row1[0]) == 0 || row1[0] == "Time" {
				continue // 去除首行和空行
			}

			//去掉设备列
			if len(row1) == 12 {
				row1[0] = row1[0][:len(row1[0])-6] // 去掉时间列的ns值
				newRow := append(row1[:1], row1[2:]...)
				remain5(newRow)
				id, _ := strconv.ParseInt(newRow[1], 10, 32)
				if count == 31 {
					for i := 1; i <= 30; i++ {
						//fmt.Println(rowTmp[i])
						row1Ch <- rowTmp[i]
					}
					count = 1
				}
				rowTmp[id] = newRow
				count++

			} else if len(row1) == 11 {
				// export导出bug，没有0时间戳
				newRow := append([]string{"0"}, row1[1:]...)
				remain5(newRow)
				id, _ := strconv.ParseInt(newRow[1], 10, 32)
				if count == 31 {
					for i := 1; i <= 30; i++ {
						row1Ch <- rowTmp[i]
					}
					count = 1
				} else {
					rowTmp[id] = newRow
					count++
				}
			}
		}
	}()
	boolLower := func(row []string) {
		// 将False True小写
		for _, index := range []int{4, 5, 6, 8} {
			row[index] = strings.ToLower(row[index])
		}
	}
	// 处理原始数据
	go func() {
		for {
			row2, err2 := reader2.Read()
			if err2 == io.EOF {
				close(row2Ch)
				file2.Close()
				return
			}
			if len(row2[0]) == 0 || row2[0] == "TIME" {
				continue // 去除首行和空行
			}
			if testID == 613 {
				// 1号测点
				if row2[1] == "1" {
					timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
					// 最近一分钟
					if timestamp >= startTimestamp {
						remain5(row2)
						boolLower(row2)
						row2Ch <- row2
					}
				}
			} else { // 621
				timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
				if timestamp >= startTimestamp {
					remain5(row2)
					boolLower(row2)
					row2Ch <- row2
				}
			}
		}
	}()
	// 检验
	verifyRecord(row1Ch, row2Ch)
}

func verifyDigitalDataHis96(exportFile string, sourceFile string) {
	file1, _ := os.Open(exportFile)
	file2, _ := os.Open(sourceFile)
	reader1 := csv.NewReader(file1)
	reader2 := csv.NewReader(file2)
	row1Ch := make(chan []string)
	row2Ch := make(chan []string)
	// 处理导出的数据
	go func() {
		var rowTmp = make([][]string, 71) // 0不用
		var count = 1
		for {
			row1, err1 := reader1.Read()
			if err1 == io.EOF {
				close(row1Ch)
				file1.Close()
				return
			}
			if len(row1[0]) == 0 || row1[0] == "Time" {
				continue // 去除首行和空行
			}
			//去掉normal设备列
			if len(row1) == 12 {
				row1[0] = row1[0][:len(row1[0])-6] // 去掉时间列的ns值
				newRow := append(row1[:1], row1[2:]...)
				id, _ := strconv.ParseInt(newRow[1], 10, 32)
				if count == 71 {
					for i := 1; i <= 70; i++ {
						row1Ch <- rowTmp[i]
					}
					count = 1
				}
				rowTmp[id] = newRow
				count++

			} else if len(row1) == 11 {
				newRow := append([]string{"0"}, row1[1:]...)
				id, _ := strconv.ParseInt(newRow[1], 10, 32)
				if count == 71 {
					for i := 1; i <= 70; i++ {
						row1Ch <- rowTmp[i]
					}
					count = 1
				} else {
					rowTmp[id] = newRow
					count++
				}
			}
		}
	}()
	boolLower := func(row []string) {
		// 将False True小写
		for _, index := range []int{2, 3, 4, 5, 6, 7, 8} {
			row[index] = strings.ToLower(row[index])
		}
	}
	// 处理原始数据
	go func() {
		for {
			row2, err2 := reader2.Read()
			if err2 == io.EOF {
				close(row2Ch)
				file2.Close()
				return
			}
			if len(row2[0]) == 0 || row2[0] == "TIME" {
				continue // 去除首行和空行
			}
			if testID == 613 {
				// 1号测点
				if row2[1] == "1" {
					timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
					// 开始时间
					if timestamp >= startTimestamp {
						boolLower(row2)
						row2Ch <- row2
					}
				}
			} else {
				timestamp, _ := strconv.ParseInt(row2[0], 10, 64)
				// 开始时间
				if timestamp >= startTimestamp {
					boolLower(row2)
					row2Ch <- row2
				}
			}

		}
	}()
	// 检验
	verifyRecord(row1Ch, row2Ch)
}

func verifyRecord(row1Ch chan []string, row2Ch chan []string) {
	for {
		row1, ok1 := <-row1Ch
		if !ok1 {
			break
		}
		row2, ok2 := <-row2Ch
		if !ok2 {
			break
		}

		for i := 0; i < len(row1); i++ {
			if row1[i] != row2[i] {
				fmt.Println(row1)
				fmt.Println(row2)
				return
			}
		}
	}
	fmt.Println("数据逐条比较完成，数据一致！")
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
	pretimestamp := int64(-400)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if len(rec) == 0 || rec[0] == "TIME" {
			continue // 去除首行和空行
		}
		timestamp, err := strconv.ParseInt(rec[0], 10, 64)

		// ####产生缺失的数据集
		for {
			if timestamp != pretimestamp+400 {
				session, _ := sessionPool.GetSession()
				checkError(session.InsertRecord(device, []string{"DATA2"},
					[]client.TSDataType{client.DOUBLE}, []interface{}{0.0}, (pretimestamp+400)*1000000))
				sessionPool.PutBack(session)
				checkError(nil, err)
				//tablet.SetTimestamp((pretimestamp+400)*1000000, rowNum)
				//err = tablet.SetValueAt(nil, 0, rowNum)
				//checkError(nil, err)
				//tablet.RowSize++
				//rowNum++
				pretimestamp += 400
			} else {
				break
			}
		}
		pretimestamp = timestamp
		// ##########

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
			Measurement: "DATA_A",
			DataType:    client.INT64,
		},
		{
			Measurement: "DATA_B",
			DataType:    client.INT64,
		},
		{
			Measurement: "DATA_C",
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
		if periodic {
			time.Sleep(time.Second)
		}
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue // 跳过空行和注释行
		}
		session, _ := sessionPool.GetSession()
		_, _ = session.ExecuteNonQueryStatement(line)
		sessionPool.PutBack(session)
	}

	err = file.Close()
	checkError(nil, err)
}

func createAndInsertData(batchCount int, timeStart time.Time, interval time.Duration, device string, measurements []string, dataTypes []client.TSDataType) {
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
			time.Sleep(interval) // 间隔时间  + ".d" + strconv.Itoa(rand.Intn(10000))
			tablet, err := client.NewTablet(device, measurementSchemas, 1)
			timestamp := timeStart.Add(interval * time.Duration(i)).UnixMilli()
			// 每个tablet只有一行数据
			tablet.SetTimestamp(timestamp, 0)
			for col := 0; col < columnCount; col++ {
				err = tablet.SetValueAt(rand.Int31(), col, 0)
				checkError(nil, err)
			}
			tablet.RowSize++
			//fmt.Println("insert time:", time.Unix(timestamp/1e9, 0))
			session, err := sessionPool.GetSession()
			checkError(nil, err)
			checkError(session.InsertTablet(tablet, false))
			sessionPool.PutBack(session)
		}
	} else {
		// 极速写入
		tablet, err := client.NewTablet(device, measurementSchemas, batchCount)
		checkError(nil, err)
		for i := 0; i < batchCount; i++ {
			timestamp := timeStart.Add(interval * time.Duration(i)).UnixMilli()
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
		//fmt.Println("插入批次", batchCount)
		sessionPool.PutBack(session)
	}
}

func createAndInsertDataRandom100(batchCount int, timeStart time.Time, interval time.Duration, device string, measurements []string, dataTypes []client.TSDataType) {
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

func createMeasurements(isAnalog bool) {
	// 循环创建时间序列，直到数据库崩溃。
	var measurements []string
	var dataTypes []client.TSDataType
	if isAnalog {
		measurements = []string{"P_NUM", "AV", "AVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
		dataTypes = []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32}

	} else {
		measurements = []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
		dataTypes = []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32}
	}
	encodings := []client.TSEncoding{client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN, client.PLAIN}
	compressors := []client.TSCompressionType{client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED, client.UNCOMPRESSED}

	for i := int64(0); i < math.MaxInt64; i++ {
		var paths []string
		for _, measurement := range measurements {
			if isAnalog {
				paths = append(paths, baseRoot+".d628.A"+strconv.FormatInt(i, 10)+"."+measurement)
			} else {
				paths = append(paths, baseRoot+".d628.D"+strconv.FormatInt(i, 10)+"."+measurement)
			}
		}
		session, _ := sessionPool.GetSession()
		r, err := session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors)
		if err != nil {
			log.Println(err)
			fmt.Println("设备总数=", i)
			fmt.Println("测点总数=", i*10)
			return
		}
		if err = client.VerifySuccess(r); err != nil {
			log.Println(err)
			fmt.Println("设备总数=", i)
			fmt.Println("测点总数=", i*10)
			return
		}
		sessionPool.PutBack(session)
	}
	fmt.Println("设备总数=", math.MaxInt64)
}

func RunCommand(args string) error {
	cmdAndParams := strings.Split(args, " ")
	cmd := exec.Command(cmdAndParams[0], cmdAndParams[1:]...)
	// 命令的错误输出和标准输出都连接到同一个管道
	stdout, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout

	if err != nil {
		return err
	}

	if err = cmd.Start(); err != nil {
		return err
	}
	// 从管道中实时获取输出并打印到终端
	for {
		tmp := make([]byte, 1024)
		_, err := stdout.Read(tmp)
		fmt.Print(string(tmp))
		if err != nil {
			break
		}
	}

	if err = cmd.Wait(); err != nil {
		return err
	}
	return nil
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
