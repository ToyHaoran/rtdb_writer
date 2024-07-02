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
	"log"
	"unsafe"
)

var (
	host     string
	port     string
	user     string
	password string
)
var session client.Session

func main() {
	// Need a main function to make CGO compile package as C shared library
}

//export login
func login(param *C.char) {
	fmt.Println("登录数据库")
	flag.StringVar(&host, "host", "192.168.150.100", "--host=192.168.150.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}
	// TODO 是否要使用线程池？
	session = client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
}

//export logout
func logout() {
	fmt.Println("登出数据库")
	session.Close()
}

type Analog struct {
	P_NUM int32   // P_NUM, 4Byte
	AV    float32 // AV, 4Byte
	AVR   float32 // AVR, 4Byte
	Q     bool    // Q, 1Byte
	BF    bool    // BF, 1Byte
	QF    bool    // QF, 1Byte
	FAI   float32 // FAI, 4Byte
	MS    bool    // MS, 1Byte
	TEW   byte    // TEW, 1Byte
	CST   uint16  // CST, 2Byte
}

type Digital struct {
	P_NUM int32  // P_NUM, 4Byte
	DV    bool   // DV, 1Byte
	DVR   bool   // DVR, 1Byte
	Q     bool   // Q, 1Byte
	BF    bool   // BF, 1Byte
	FQ    bool   // FQ, 1Byte
	FAI   bool   // FAI, 1Byte
	MS    bool   // MS, 1Byte
	TEW   byte   // TEW, 1Byte
	CST   uint16 // CST, 2Byte
}

// 1写实时模拟量
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
//
//export write_rt_analog
func write_rt_analog(unit_id C.int64_t, time C.int64_t, analog_array_ptr *C.Analog, count C.int64_t) {
	fmt.Println("写实时模拟量start")
	goCount := int64(count)
	analogs := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:goCount:goCount]
	var (
		deviceId     = "root.sg1.dev1"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)

	for _, an := range analogs {
		timestamps = append(timestamps, int64(time))
		measurements = append(measurements, []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(an.P_NUM), an.AV, an.AVR, an.Q, an.BF, an.QF, an.FAI, an.MS, string(an.TEW), int32(an.CST)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))
	fmt.Println("写实时模拟量OK")
}

// 1.1 写实时模拟量断面
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 模拟量断面数组, 包含count个断面的模拟量
// array_count: 每个断面中包含值的数量
//
//export write_rt_analog_list
func write_rt_analog_list(unit_id C.int64_t, time *C.int64_t, analog_array_array_ptr **C.Analog, array_count *C.int64_t, count C.int64_t) {
	fmt.Println("写实时模拟量断面start")
	goSectionCount := int64(count)
	analogsPtr := (*[1 << 30]*Analog)(unsafe.Pointer(analog_array_array_ptr))[:goSectionCount:goSectionCount]
	times := (*[1 << 30]int64)(unsafe.Pointer(time))[:goSectionCount:goSectionCount]
	analogsCount := (*[1 << 30]int64)(unsafe.Pointer(array_count))[:goSectionCount:goSectionCount]
	for i := int64(0); i < goSectionCount; i++ {
		// 获取每个断面的模拟量数组长度
		goCount := int(analogsCount[i])
		// 确保 goCount 不会超过 analogsPtr[i] 指向的数组的实际长度
		if goCount > cap((*[1 << 30]Analog)(unsafe.Pointer(analogsPtr[i]))) {
			fmt.Printf("goCount %d exceeds the capacity of analogs array for index %d\n", goCount, i)
			continue
		}
		// 获取模拟量数组
		analogs := (*[1 << 30]Analog)(unsafe.Pointer(analogsPtr[i]))[:goCount:goCount]
		// 初始化存储数据的变量
		var (
			deviceId     = "root.sg1.dev11"
			timestamps   []int64
			measurements [][]string
			dataTypes    [][]client.TSDataType
			values       [][]interface{}
		)
		for _, an := range analogs {
			timestamps = append(timestamps, int64(times[i]))
			measurements = append(measurements, []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"})
			dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
			values = append(values, []interface{}{int32(an.P_NUM), an.AV, an.AVR, an.Q, an.BF, an.QF, an.FAI, an.MS, string(an.TEW), int32(an.CST)})
		}

		// 模拟调用数据库插入函数
		checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))
	}
	fmt.Println("写实时模拟量断面OK")
}

// 2写实时数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
//
//export write_rt_digital
func write_rt_digital(unit_id C.int64_t, time C.int64_t, digital_array_ptr *C.Digital, count C.int64_t) {
	fmt.Println("写实时数字量start")
	goCount := int64(count)
	goDigitalArray := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:goCount:goCount]
	var (
		deviceId     = "root.sg1.dev2"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)
	for index, di := range goDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurements = append(measurements, []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(di.P_NUM), di.DV, di.DVR, di.Q, di.BF, di.FQ, di.FAI, di.MS, string(di.TEW), int32(di.CST)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))

	fmt.Println("写实时数字量OK")
}

// 2.1 写实时数字量
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 数字量断面数组, 包含count个断面的数字量
// array_count: 每个断面中包含值的数量
//
//export write_rt_digital_list
func write_rt_digital_list(unit_id C.int64_t, time *C.int64_t, digital_array_array_ptr **C.Digital, array_count *C.int64_t, count C.int64_t) {
	fmt.Println("写实时数字量断面start")
	fmt.Println("写实时数字量断面OK")
}

// 3写历史模拟量
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
//
//export write_his_analog
func write_his_analog(unit_id C.int64_t, time C.int64_t, analog_array_ptr *C.Analog, count C.int64_t) {
	fmt.Println("写历史模拟量start")
	goCount := int64(count)
	goAnalogArray := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:goCount:goCount]

	var (
		deviceId     = "root.sg1.dev3"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)
	for index, an := range goAnalogArray {
		timestamps = append(timestamps, int64(index))
		measurements = append(measurements, []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(an.P_NUM), an.AV, an.AVR, an.Q, an.BF, an.QF, an.FAI, an.MS, string(an.TEW), int32(an.CST)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))
	fmt.Println("写历史模拟量OK")
}

// 4写历史数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
//
//export write_his_digital
func write_his_digital(unit_id C.int64_t, time C.int64_t, digital_array_ptr *C.Digital, count C.int64_t) {
	fmt.Println("写历史数字量start")
	goCount := int64(count)
	goDigitalArray := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:goCount:goCount]
	var (
		deviceId     = "root.sg1.dev4"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)
	for index, di := range goDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurements = append(measurements, []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(di.P_NUM), di.DV, di.DVR, di.Q, di.BF, di.FQ, di.FAI, di.MS, string(di.TEW), int32(di.CST)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))

	fmt.Println("写历史数字量OK")
}

type StaticAnalog struct {
	P_NUM int32     // P_NUM, 4Byte
	TAGT  uint16    // TAGT, 1Byte
	FACK  uint16    // FACK, 1Byte
	L4AR  bool      // L4AR, 1Byte
	L3AR  bool      // L3AR, 1Byte
	L2AR  bool      // L2AR, 1Byte
	L1AR  bool      // L1AR, 1Byte
	H4AR  bool      // H4AR, 1Byte
	H3AR  bool      // H3AR, 1Byte
	H2AR  bool      // H2AR, 1Byte
	H1AR  bool      // H1AR, 1Byte
	CHN   [32]byte  // CHN, 32Byte
	PN    [32]byte  // PN, 32Byte
	DESC  [128]byte // DESC, 128Byte
	UNIT  [32]byte  // UNIT, 32Byte
	MU    float32   // MU, 4Byte
	MD    float32   // MD, 4Byte
}

// 5写静态模拟量
// unit_id: 机组ID
// static_analog_array_ptr: 指向静态模拟量数组的指针
// count: 数组长度
//
//export write_static_analog
func write_static_analog(unit_id C.int64_t, static_analog_array_ptr *C.StaticAnalog, count C.int64_t) {
	fmt.Println("写静态模拟量start")
	goCount := int64(count)
	goStaticAnalogArray := (*[1 << 30]StaticAnalog)(unsafe.Pointer(static_analog_array_ptr))[:goCount:goCount]

	var (
		deviceId     = "root.sg1.dev5"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)

	// 遍历 goStaticAnalogArray
	for index, sa := range goStaticAnalogArray {
		timestamps = append(timestamps, int64(index))
		measurements = append(measurements, []string{"P_NUM", "TAGT", "FACK", "L4AR", "L3AR", "L2AR", "L1AR", "H4AR", "H3AR", "H2AR", "H1AR", "CHN", "PN", "DESC", "UNIT", "MU", "MD"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.INT32, client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.TEXT, client.TEXT, client.TEXT, client.FLOAT, client.FLOAT})
		values = append(values, []interface{}{sa.P_NUM, int32(sa.TAGT), int32(sa.FACK), sa.L4AR, sa.L3AR, sa.L2AR, sa.L1AR, sa.H4AR, sa.H3AR, sa.H2AR, sa.H1AR, string(sa.CHN[:]), string(sa.PN[:]), string(sa.DESC[:]), string(sa.UNIT[:]), sa.MU, sa.MD})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))
	fmt.Println("写静态模拟量OK")
}

type StaticDigital struct {
	P_NUM int32     // P_NUM, 4Byte
	FACK  uint16    // FACK, 2Byte
	CHN   [32]byte  // CHN, 32Byte
	PN    [32]byte  // PN, 32Byte
	DESC  [128]byte // DESC, 128Byte
	UNIT  [32]byte  // UNIT, 32Byte
}

// 6写静态数字量
// unit_id: 机组ID
// static_digital_array_ptr: 指向静态数字量数组的指针
// count: 数组长度
//
//export write_static_digital
func write_static_digital(unit_id C.int64_t, static_digital_array_ptr *C.StaticDigital, count C.int64_t) {
	fmt.Println("写静态数字量start")
	goCount := int64(count)
	goStaticDigitalArray := (*[1 << 30]StaticDigital)(unsafe.Pointer(static_digital_array_ptr))[:goCount:goCount]

	var (
		deviceId     = "root.sg1.dev6"
		timestamps   []int64
		measurements [][]string
		dataTypes    [][]client.TSDataType
		values       [][]interface{}
	)
	for index, sd := range goStaticDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurements = append(measurements, []string{"P_NUM", "FACK", "CHN", "PN", "DESC", "UNIT"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.INT32, client.TEXT, client.TEXT, client.TEXT, client.TEXT})
		values = append(values, []interface{}{sd.P_NUM, int32(sd.FACK), string(sd.CHN[:]), string(sd.PN[:]), string(sd.DESC[:]), string(sd.UNIT[:])})
	}

	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurements, dataTypes, values, false))
	fmt.Println("写静态数字量OK")
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
