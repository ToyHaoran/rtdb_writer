package main

/*
#include <stdint.h>
#include <stdbool.h>
#include "write_plugin.h"
*/
import "C"
import (
	"flag"
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/rpc"
	"github.com/panjf2000/ants/v2"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	host     string
	port     string
	user     string
	password string
)
var baseRoot = "root.sg"
var sessionPool client.SessionPool
var nodeUrls string
var batchSize int64
var conMaxSize int64
var startTime int64
var endTime int64

var wg sync.WaitGroup // go协程池用
var threadPool *ants.PoolWithFunc

func main() {
	// Need a main function to make CGO compile package as C shared library
}

//export login
func login(param *C.char) C.int {
	var err error
	goParam := C.GoString(param)
	var params []string
	params = strings.Split(goParam, ",")
	if len(params) == 1 {
		// 使用默认值
		fmt.Println(fmt.Sprintf("使用默认参数--param=%s,xty111:6667,root,root,1000,5000,root.sg", goParam))
		params = strings.Split(goParam+",xty111:6667,root,root,1000,5000,root.sg", ",")
	} else {
		fmt.Println("参数--param=" + goParam)
	}
	//fmt.Println("登录数据库，运行" + params[0])
	startTime = time.Now().UnixMilli()
	// 使用所给参数
	//flag.StringVar(&host, "host", params[1], "--host=127.0.0.1")
	//flag.StringVar(&port, "port", params[2], "--port=6667")
	flag.StringVar(&nodeUrls, "nodeUrls", params[1], "--nodeUrls=xty111:6667-xty112:6667-xty113:6667")
	flag.StringVar(&user, "user", params[2], "--user=root")
	flag.StringVar(&password, "password", params[3], "--password=root")
	flag.Parse()
	config := &client.PoolConfig{
		//Host:     host,
		//Port:     port,
		NodeUrls: strings.Split(nodeUrls, "-"),
		UserName: user,
		Password: password,
	}

	conMaxSize, _ = strconv.ParseInt(params[4], 10, 32)
	batchSize, _ = strconv.ParseInt(params[5], 10, 32)

	baseRoot = params[6]
	// 控制session的并发连接数上限，否则可能断开连接
	sessionPool = client.NewSessionPool(config, int(conMaxSize), 600000, 600000, false)

	// 线程池
	threadPool, err = ants.NewPoolWithFunc(
		int(conMaxSize-10), // 控制线程池大小，小于sessionPool的大小
		func(i interface{}) {
			switch i.(type) {
			case Data:
				insertData(i.(Data))
			case *client.Tablet:
				insertTable(i.(*client.Tablet))
			}
			wg.Done()
		})
	checkError(nil, err)

	return 0
}

//export logout
func logout() {
	//fmt.Println("等待数据库退出......")
	if tabletHisAnalog[1] != nil {
		// 插入剩下的历史模拟量数据
		if tabletHisAnalog[1].RowSize > 0 {
			var wgAnalog sync.WaitGroup
			for i := 1; i < len(tabletHisAnalog); i++ {
				wgAnalog.Add(1)
				go func(tablet *client.Tablet) {
					session, _ := sessionPool.GetSession()
					checkError(session.InsertTablet(tablet, false))
					sessionPool.PutBack(session)
					tablet.Reset()
					wgAnalog.Done()
				}(tabletHisAnalog[i])
			}
			wgAnalog.Wait()
			hisAnalogBatchCount = 0
		}
	}
	if tabletHisDigital[1] != nil {
		// 插入剩下的历史数字量数据
		if tabletHisDigital[1].RowSize > 0 {
			var wgDigital sync.WaitGroup
			for i := 1; i < len(tabletHisDigital); i++ {
				wgDigital.Add(1)
				go func(tablet *client.Tablet) {
					session, _ := sessionPool.GetSession()
					checkError(session.InsertTablet(tablet, false))
					sessionPool.PutBack(session)
					tablet.Reset()
					wgDigital.Done()
				}(tabletHisDigital[i])
			}
			wgDigital.Wait()
			hisDigitalBatchCount = 0
		}
	}
	// 等待线程池执行完毕
	wg.Wait()
	threadPool.Release()
	sessionPool.Close()
	//endTime = time.Now().UnixMilli()
	//fmt.Println("登出数据库，结束程序，耗时" + fmt.Sprint(endTime-startTime) + "ms")
}

type Analog struct {
	GLOBAL_ID int64   // 全局ID
	P_NUM     int32   // P_NUM, 4Byte
	AV        float32 // AV, 4Byte
	AVR       float32 // AVR, 4Byte
	Q         bool    // Q, 1Byte
	BF        bool    // BF, 1Byte
	FQ        bool    // FQ, 1Byte
	FAI       float32 // FAI, 4Byte
	MS        bool    // MS, 1Byte
	TEW       byte    // TEW, 1Byte
	CST       uint16  // CST, 2Byte
}

type Digital struct {
	GLOBAL_ID int64  // 全局ID
	P_NUM     int32  // P_NUM, 4Byte
	DV        bool   // DV, 1Byte
	DVR       bool   // DVR, 1Byte
	Q         bool   // Q, 1Byte
	BF        bool   // BF, 1Byte
	FQ        bool   // FQ, 1Byte
	FAI       bool   // FAI, 1Byte
	MS        bool   // MS, 1Byte
	TEW       byte   // TEW, 1Byte
	CST       uint16 // CST, 2Byte
}

// insertData 封装后插入
func insertData(data Data) {
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(data.devices, data.measurementss, data.dataTypess, data.valuess, data.timestamps))
	}
	sessionPool.PutBack(session)
}

// insertTable 封装为tablet插入
func insertTable(tablet *client.Tablet) {
	session, _ := sessionPool.GetSession()
	checkError(session.InsertTablet(tablet, false))
	sessionPool.PutBack(session)
}

// measureTime 用来测量函数的运行时间
func measureTime(functionToMeasure func()) time.Duration {
	start := time.Now()
	functionToMeasure()
	end := time.Now()
	return end.Sub(start)
}

// sumaryString 统计输出信息
func sumaryString(deviceCount int, isFast C.bool, isAnalog bool) string {
	var name string
	var analogOrDigital string
	if isFast {
		name = "快采点"
	} else {
		name = "普通点"
	}
	if isAnalog {
		analogOrDigital = "模拟量"
	} else {
		analogOrDigital = "数字量"
	}
	return fmt.Sprintf("%s 写实时 %s OK，插入 %d 条数据，即IoTDB中的 %d 测点", name, analogOrDigital, deviceCount, deviceCount*9)
}

type Data struct {
	devices       []string
	timestamps    []int64
	measurementss [][]string
	dataTypess    [][]client.TSDataType
	valuess       [][]interface{}
}

// 1写实时模拟量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
// is_fast: 当为true时表示写快采点, 当为false时表示写普通点
//
//export write_rt_analog
func write_rt_analog(magic C.int32_t, unit_id C.int64_t, timestamp C.int64_t, analog_array_ptr *C.Analog, count C.int64_t, is_fast C.bool) {
	//fmt.Println("写实时模拟量start")
	deviceCount := int64(count)
	analogs := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "AV", "AVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32}
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}

	if is_fast {
		// 150个测点。
		device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastA"
		tablet, _ := client.NewTablet(device, measurementSchemas, int(deviceCount))
		for row, an := range analogs {
			tablet.SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano()+int64(an.P_NUM), row)
			for i, col := range getAnalogValues(an) {
				_ = tablet.SetValueAt(col, i, row)
			}
			tablet.RowSize++
		}
		// 快采点数据量少，直接写就行，但要控制连接数
		wg.Add(1)
		_ = threadPool.Invoke(tablet)
	} else {
		if true {
			// 普通点数据量巨大，时间序列融合  一秒15万条数据。
			// 写实时模拟量OK，插入59850条数据，约60万测点
			visualDeviceCount := int64(50) // 虚拟设备数量0～50
			// 如果PNUM是乱序来的怎么办，直接放到一个设备中，也可以根据设备%batchSize求余，放在对应table中
			normalBatchSize := deviceCount / visualDeviceCount
			var wgslow sync.WaitGroup
			for num := int64(0); num <= visualDeviceCount; num++ {
				start := num * normalBatchSize
				end := start + normalBatchSize
				if end > deviceCount {
					end = deviceCount
				}
				wgslow.Add(1)
				go func(start, end int, num int64) {
					device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".normalA" + strconv.FormatInt(num, 10)
					rowCount := int(normalBatchSize)
					tablet, _ := client.NewTablet(device, measurementSchemas, rowCount)
					for row, an := range analogs[start:end] {
						tablet.SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano()+int64(an.P_NUM), row)
						for i, col := range getAnalogValues(an) {
							_ = tablet.SetValueAt(col, i, row)
						}
						tablet.RowSize++
					}
					wg.Add(1)
					_ = threadPool.Invoke(tablet)
					wgslow.Done()
				}(int(start), int(end), num)
			}
			wgslow.Wait()
			//fmt.Println(sumaryString(int(deviceCount), is_fast, true))
		}
	}
	//fmt.Println(sumaryString(int(deviceCount), is_fast, true))
}

// 1.1 批量写实时模拟量断面
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 模拟量断面数组, 包含count个断面的模拟量
// array_count: 每个断面中包含值的数量
// 备注: 只有写快采点的时候会调用此接口
//
//export write_rt_analog_list
func write_rt_analog_list(magic C.int32_t, unit_id C.int64_t, timeArray *C.int64_t, analog_array_array_ptr **C.Analog, array_count *C.int64_t, count C.int64_t) {
	//fmt.Println("写实时模拟量断面start")
	sectionCount := int64(count)
	times := (*[1 << 30]C.int64_t)(unsafe.Pointer(timeArray))[:sectionCount:sectionCount]
	analogsArray := (*[1 << 30]*C.Analog)(unsafe.Pointer(analog_array_array_ptr))[:sectionCount:sectionCount]
	arrayCounts := (*[1 << 30]C.int64_t)(unsafe.Pointer(array_count))[:sectionCount:sectionCount]

	totalCount := 0 // 总共多少条数据
	for _, tcount := range arrayCounts {
		totalCount += int(tcount)
	}

	measurements := []string{"P_NUM", "AV", "AVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32}

	device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastA"
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	tablet, _ := client.NewTablet(device, measurementSchemas, totalCount)

	rowStart := 0
	for i := int64(0); i < sectionCount; i++ {
		deviceCount := int64(arrayCounts[i])
		analogs := (*[1 << 30]Analog)(unsafe.Pointer(analogsArray[i]))[:deviceCount:deviceCount]
		for rowI, an := range analogs {
			rowOut := rowStart + rowI
			tablet.SetTimestamp(time.UnixMilli(int64(times[i])).UnixNano()+int64(an.P_NUM), rowOut)
			for j, col := range getAnalogValues(an) {
				_ = tablet.SetValueAt(col, j, rowOut)
			}
			tablet.RowSize++
		}
		rowStart += int(deviceCount)
	}
	// 快采点数据量少，直接写就行，但要控制连接数
	wg.Add(1)
	_ = threadPool.Invoke(tablet)
	//fmt.Println("批量写实时模拟量断面OK", sumaryString(totalCount, true, true))
}

// 2写实时数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
// is_fast: 当为true时表示写快采点, 当为false时表示写普通点
//
//export write_rt_digital
func write_rt_digital(magic C.int32_t, unit_id C.int64_t, timestamp C.int64_t, digital_array_ptr *C.Digital, count C.int64_t, is_fast C.bool) {
	//fmt.Println("写实时数字量start")
	deviceCount := int64(count)
	digitals := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32}

	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}

	if is_fast {
		device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastD"
		tablet, _ := client.NewTablet(device, measurementSchemas, int(deviceCount))
		for row, di := range digitals {
			tablet.SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano()+int64(di.P_NUM), row)
			for i, col := range getDigitalValues(di) {
				_ = tablet.SetValueAt(col, i, row)
			}
			tablet.RowSize++
		}
		wg.Add(1)
		_ = threadPool.Invoke(tablet)
	} else {

		if true {
			// 普通点数量多，使用时间序列融合
			// 写实时数字量OK，插入139650条数据，约140万测点。
			visualDeviceCount := int64(100) // 虚拟设备数量
			normalBatchSize := deviceCount / visualDeviceCount
			var wgslow sync.WaitGroup
			for num := int64(0); num <= visualDeviceCount; num++ {
				start := num * normalBatchSize
				end := start + normalBatchSize
				if end > deviceCount {
					end = deviceCount
				}
				wgslow.Add(1)
				go func(start, end int, num int64) {
					device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".normalD" + strconv.FormatInt(num, 10)
					rowCount := int(normalBatchSize)
					tablet, _ := client.NewTablet(device, measurementSchemas, rowCount)
					for row, di := range digitals[start:end] {
						tablet.SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano()+int64(di.P_NUM), row)
						for i, col := range getDigitalValues(di) {
							_ = tablet.SetValueAt(col, i, row)
						}
						tablet.RowSize++
					}
					wg.Add(1)
					_ = threadPool.Invoke(tablet)
					wgslow.Done()
				}(int(start), int(end), num)
			}
			wgslow.Wait()
			//fmt.Println(sumaryString(int(deviceCount), is_fast, false))
		}
	}
	//fmt.Println(sumaryString(int(deviceCount), is_fast, false))
}

// 2.1 批量写实时数字量
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 数字量断面数组, 包含count个断面的数字量
// array_count: 每个断面中包含值的数量
// 备注: 只有写快采点的时候会调用此接口
//
//export write_rt_digital_list
func write_rt_digital_list(magic C.int32_t, unit_id C.int64_t, timeArray *C.int64_t, digital_array_array_ptr **C.Digital, array_count *C.int64_t, count C.int64_t) {
	//fmt.Println("写实时数字量断面start")
	sectionCount := int64(count)
	times := (*[1 << 30]C.int64_t)(unsafe.Pointer(timeArray))[:sectionCount:sectionCount]
	digitalsArray := (*[1 << 30]*C.Digital)(unsafe.Pointer(digital_array_array_ptr))[:sectionCount:sectionCount]
	arrayCounts := (*[1 << 30]C.int64_t)(unsafe.Pointer(array_count))[:sectionCount:sectionCount]

	totalCount := 0 // 总共多少条数据
	for _, tcount := range arrayCounts {
		totalCount += int(tcount)
	}

	measurements := []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32}

	device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastD"
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	tablet, _ := client.NewTablet(device, measurementSchemas, totalCount)

	rowStart := 0
	for i := int64(0); i < sectionCount; i++ {
		deviceCount := int64(arrayCounts[i])
		digitals := (*[1 << 30]Digital)(unsafe.Pointer(digitalsArray[i]))[:deviceCount:deviceCount]
		for rowI, di := range digitals {
			rowOut := rowStart + rowI
			tablet.SetTimestamp(time.UnixMilli(int64(times[i])).UnixNano()+int64(di.P_NUM), rowOut)
			for j, col := range getDigitalValues(di) {
				_ = tablet.SetValueAt(col, j, rowOut)
			}
			tablet.RowSize++
		}
		rowStart += int(deviceCount)
	}
	// 快采点数据量少，直接写就行，但要控制连接数
	wg.Add(1)
	_ = threadPool.Invoke(tablet)
	//fmt.Println("批量写实时数字量断面OK", sumaryString(int(totalCount), true, false))
}

// 攒批数据，需要提前创建30个，0不用。没有多机组并行需求，不考虑多机组。
var tabletHisAnalog = make([]*client.Tablet, 31)
var hisAnalogBatchCount = 0

func getAnalogValues(an Analog) []interface{} {
	return []interface{}{an.P_NUM, an.AV, an.AVR, an.Q, an.BF, an.FQ, an.FAI, an.MS, string(an.TEW), int32(an.CST)}
}

// 3写历史模拟量
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
//
//export write_his_analog
func write_his_analog(magic C.int32_t, unit_id C.int64_t, timestamp C.int64_t, analog_array_ptr *C.Analog, count C.int64_t) {
	//fmt.Println("写历史模拟量start")
	deviceCount := int64(count)
	analogs := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "AV", "AVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32}
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	for _, an := range analogs {
		id := an.P_NUM // P_NUM从1开始
		if tabletHisAnalog[id] == nil || tabletHisAnalog[id].RowSize == 0 {
			device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".historyA.d" + strconv.FormatInt(int64(id), 10)
			tabletHisAnalog[id], _ = client.NewTablet(device, measurementSchemas, int(batchSize))
		}
		tabletHisAnalog[id].SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano(), hisAnalogBatchCount)
		for i, col := range getAnalogValues(an) {
			_ = tabletHisAnalog[id].SetValueAt(col, i, hisAnalogBatchCount)
		}
		tabletHisAnalog[id].RowSize++
	}

	hisAnalogBatchCount++

	// 批次满后插入，一个满，全部满
	if tabletHisAnalog[1].RowSize >= int(batchSize) {
		var wgAnalog sync.WaitGroup
		for i := 1; i < len(tabletHisAnalog); i++ {
			wgAnalog.Add(1)
			go func(tablet *client.Tablet) {
				session, _ := sessionPool.GetSession()
				checkError(session.InsertTablet(tablet, false))
				sessionPool.PutBack(session)
				tablet.Reset()
				wgAnalog.Done()
			}(tabletHisAnalog[i])
		}
		wgAnalog.Wait()
		hisAnalogBatchCount = 0
	}
	// 最后一个没有满的批次在退出数据库的时候插入。

	//fmt.Println("写历史模拟量OK，插入" + strconv.Itoa(int(deviceCount)) + "条数据")
}

// 攒批数据，需要提前创建70个, 0不用
var tabletHisDigital = make([]*client.Tablet, 71)
var hisDigitalBatchCount = 0 // 计数，统计攒了多少数据

func getDigitalValues(di Digital) []interface{} {
	return []interface{}{di.P_NUM, di.DV, di.DVR, di.Q, di.BF, di.FQ, di.FAI, di.MS, string(di.TEW), int32(di.CST)}
}

// 4写历史数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
//
//export write_his_digital
func write_his_digital(magic C.int32_t, unit_id C.int64_t, timestamp C.int64_t, digital_array_ptr *C.Digital, count C.int64_t) {
	//fmt.Println("写历史数字量start")
	deviceCount := int64(count)
	digitals := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"}
	dataTypes := []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32}
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	for _, di := range digitals {
		id := di.P_NUM
		if tabletHisDigital[id] == nil || tabletHisDigital[id].RowSize == 0 {
			device := baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".historyD.d" + strconv.FormatInt(int64(id), 10)
			tabletHisDigital[id], _ = client.NewTablet(device, measurementSchemas, int(batchSize))
		}
		tabletHisDigital[id].SetTimestamp(time.UnixMilli(int64(timestamp)).UnixNano(), hisDigitalBatchCount)
		for i, col := range getDigitalValues(di) {
			_ = tabletHisDigital[id].SetValueAt(col, i, hisDigitalBatchCount)
		}
		tabletHisDigital[id].RowSize++
	}

	hisDigitalBatchCount++

	if tabletHisDigital[1].RowSize >= int(batchSize) {
		var wgDigital sync.WaitGroup
		for i := 1; i < len(tabletHisDigital); i++ {
			wgDigital.Add(1)
			go func(tablet *client.Tablet) {
				session, _ := sessionPool.GetSession()
				checkError(session.InsertTablet(tablet, false))
				sessionPool.PutBack(session)
				tablet.Reset()
				wgDigital.Done()
			}(tabletHisDigital[i])
		}
		wgDigital.Wait()
		hisDigitalBatchCount = 0
	}

	//fmt.Println("写历史数字量OK，插入" + strconv.Itoa(int(deviceCount)) + "条数据")
}

type StaticAnalog struct {
	GLOBAL_ID int64     // 全局ID
	P_NUM     int32     // P_NUM, 4Byte
	TAGT      uint16    // TAGT, 1Byte
	FACK      uint16    // FACK, 1Byte
	L4AR      bool      // L4AR, 1Byte
	L3AR      bool      // L3AR, 1Byte
	L2AR      bool      // L2AR, 1Byte
	L1AR      bool      // L1AR, 1Byte
	H4AR      bool      // H4AR, 1Byte
	H3AR      bool      // H3AR, 1Byte
	H2AR      bool      // H2AR, 1Byte
	H1AR      bool      // H1AR, 1Byte
	CHN       [32]byte  // CHN, 32Byte
	PN        [32]byte  // PN, 32Byte
	DESC      [128]byte // DESC, 128Byte
	UNIT      [32]byte  // UNIT, 32Byte
	MU        float32   // MU, 4Byte
	MD        float32   // MD, 4Byte
}

// 5写静态模拟量
// unit_id: 机组ID
// static_analog_array_ptr: 指向静态模拟量数组的指针
// count: 数组长度
// _type: 数据类型, 通过命令行传递, 具体参数用户可自定义, 推荐: 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点
//
//export write_static_analog
func write_static_analog(magic C.int32_t, unit_id C.int64_t, static_analog_array_ptr *C.StaticAnalog, count C.int64_t, _type C.int64_t) {
	//fmt.Println("写静态模拟量start")
	deviceCount := int64(count)
	staticAnalogs := (*[1 << 30]StaticAnalog)(unsafe.Pointer(static_analog_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "TAGT", "FACK", "L4AR", "L3AR", "L2AR", "L1AR", "H4AR", "H3AR", "H2AR", "H1AR", "CHN", "PN", "DESC", "UNIT", "MU", "MD"}
	dataTypes := []client.TSDataType{client.INT32, client.INT32, client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.TEXT, client.TEXT, client.TEXT, client.FLOAT, client.FLOAT}
	getValues := func(sa StaticAnalog) []interface{} {
		return []interface{}{sa.P_NUM, int32(sa.TAGT), int32(sa.FACK), sa.L4AR, sa.L3AR, sa.L2AR, sa.L1AR, sa.H4AR, sa.H3AR, sa.H2AR, sa.H1AR, string(sa.CHN[:]), string(sa.PN[:]), string(sa.DESC[:]), string(sa.UNIT[:]), sa.MU, sa.MD}
	}

	var device string
	switch int64(_type) {
	case 0:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastSA"
	case 1:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".normalSA"
	case 2:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".historySA"
	default:
		fmt.Println("write_static_analog: type参数错误")
		return
	}
	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	tablet, _ := client.NewTablet(device, measurementSchemas, int(deviceCount))
	for row, sa := range staticAnalogs {
		tablet.SetTimestamp(int64(sa.P_NUM), row)
		for i, col := range getValues(sa) {
			_ = tablet.SetValueAt(col, i, row)
		}
		tablet.RowSize++
	}
	wg.Add(1)
	_ = threadPool.Invoke(tablet)
	//fmt.Println("写静态模拟量OK，插入" + strconv.Itoa(int(deviceCount)) + "条数据")
}

type StaticDigital struct {
	GLOBAL_ID int64     // 全局ID
	P_NUM     int32     // P_NUM, 4Byte
	FACK      uint16    // FACK, 2Byte
	CHN       [32]byte  // CHN, 32Byte
	PN        [32]byte  // PN, 32Byte
	DESC      [128]byte // DESC, 128Byte
	UNIT      [32]byte  // UNIT, 32Byte
}

// 6写静态数字量
// unit_id: 机组ID
// static_digital_array_ptr: 指向静态数字量数组的指针
// count: 数组长度
// _type: 数据类型, 通过命令行传递, 具体参数用户可自定义, 推荐: 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点
//
//export write_static_digital
func write_static_digital(magic C.int32_t, unit_id C.int64_t, static_digital_array_ptr *C.StaticDigital, count C.int64_t, _type C.int64_t) {
	//fmt.Println("写静态数字量start")
	deviceCount := int64(count)
	staticDigitals := (*[1 << 30]StaticDigital)(unsafe.Pointer(static_digital_array_ptr))[:deviceCount:deviceCount]

	measurements := []string{"P_NUM", "FACK", "CHN", "PN", "DESC", "UNIT"}
	dataTypes := []client.TSDataType{client.INT32, client.INT32, client.TEXT, client.TEXT, client.TEXT, client.TEXT}
	getValues := func(sd StaticDigital) []interface{} {
		return []interface{}{sd.P_NUM, int32(sd.FACK), string(sd.CHN[:]), string(sd.PN[:]), string(sd.DESC[:]), string(sd.UNIT[:])}
	}
	var device string
	switch int64(_type) {
	case 0:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".fastSD"
	case 1:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".normalSD"
	case 2:
		device = baseRoot + ".unit" + strconv.FormatInt(int64(unit_id), 10) + ".historySD"
	default:
		fmt.Println("write_static_digital: type参数错误")
		return
	}

	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	for j := range measurements {
		measurementSchemas[j] = &client.MeasurementSchema{
			Measurement: measurements[j],
			DataType:    dataTypes[j],
		}
	}
	tablet, _ := client.NewTablet(device, measurementSchemas, int(deviceCount))
	for row, sd := range staticDigitals {
		tablet.SetTimestamp(int64(sd.P_NUM), row)
		for i, col := range getValues(sd) {
			_ = tablet.SetValueAt(col, i, row)
		}
		tablet.RowSize++
	}
	wg.Add(1)
	_ = threadPool.Invoke(tablet)
	//fmt.Println("写静态数字量OK，插入" + strconv.Itoa(int(deviceCount)) + "条数据")
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
