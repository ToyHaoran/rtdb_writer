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
func login() {
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
	P_num int32   // P_NUM, 4Byte
	Av    float32 // AV, 4Byte
	Avr   float32 // AVR, 4Byte
	Q     bool    // Q, 1Byte
	Bf    bool    // BF, 1Byte
	Qf    bool    // QF, 1Byte
	Fai   float32 // FAI, 4Byte
	Ms    bool    // MS, 1Byte
	Tew   byte    // TEW, 1Byte
	Cst   uint16  // CST, 2Byte
}

type Digital struct {
	P_num int32  // P_NUM, 4Byte
	Dv    bool   // DV, 1Byte
	Dvr   bool   // DVR, 1Byte
	Q     bool   // Q, 1Byte
	Bf    bool   // BF, 1Byte
	Bq    bool   // FQ, 1Byte
	Fai   bool   // FAI, 1Byte
	Ms    bool   // MS, 1Byte
	Tew   byte   // TEW, 1Byte
	Cst   uint16 // CST, 2Byte
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
	goAnalogArray := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:goCount:goCount]
	var (
		deviceId          = "root.sg1.dev1"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)

	for index, sa := range goAnalogArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Av", "Avr", "Q", "Bf", "Qf", "Fai", "Ms", "Tew", "Cst"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(sa.P_num), sa.Av, sa.Avr, sa.Q, sa.Bf, sa.Qf, sa.Fai, sa.Ms, string(sa.Tew), int32(sa.Cst)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	fmt.Println("写实时模拟量OK")
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
		deviceId          = "root.sg1.dev2"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)
	for index, sa := range goDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Dv", "Dvr", "Q", "Bf", "Bq", "Fai", "Ms", "Tew", "Cst"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(sa.P_num), sa.Dv, sa.Dvr, sa.Q, sa.Bf, sa.Bq, sa.Fai, sa.Ms, string(sa.Tew), int32(sa.Cst)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))

	fmt.Println("写实时数字量OK")
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
		deviceId          = "root.sg1.dev3"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)
	for index, sa := range goAnalogArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Av", "Avr", "Q", "Bf", "Qf", "Fai", "Ms", "Tew", "Cst"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(sa.P_num), sa.Av, sa.Avr, sa.Q, sa.Bf, sa.Qf, sa.Fai, sa.Ms, string(sa.Tew), int32(sa.Cst)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
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
		deviceId          = "root.sg1.dev4"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)
	for index, sa := range goDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Dv", "Dvr", "Q", "Bf", "Bq", "Fai", "Ms", "Tew", "Cst"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		values = append(values, []interface{}{int32(sa.P_num), sa.Dv, sa.Dvr, sa.Q, sa.Bf, sa.Bq, sa.Fai, sa.Ms, string(sa.Tew), int32(sa.Cst)})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))

	fmt.Println("写历史数字量OK")
}

type StaticAnalog struct {
	P_num int32
	Tagt  uint16
	Fack  uint16
	L4ar  bool
	L3ar  bool
	L2ar  bool
	L1ar  bool
	H4ar  bool
	H3ar  bool
	H2ar  bool
	H1ar  bool
	Chn   [32]byte
	Pn    [32]byte
	Desc  [128]byte
	Unit  [32]byte
	Mu    float32
	Md    float32
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
		deviceId          = "root.sg1.dev5"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)

	// 遍历 goStaticAnalogArray
	for index, sa := range goStaticAnalogArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Tagt", "Fack", "L4ar", "L3ar", "L2ar", "L1ar", "H4ar", "H3ar", "H2ar", "H1ar", "Chn", "Pn", "Desc", "Unit", "Mu", "Md"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.INT32, client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.TEXT, client.TEXT, client.TEXT, client.FLOAT, client.FLOAT})
		values = append(values, []interface{}{sa.P_num, int32(sa.Tagt), int32(sa.Fack), sa.L4ar, sa.L3ar, sa.L2ar, sa.L1ar, sa.H4ar, sa.H3ar, sa.H2ar, sa.H1ar, string(sa.Chn[:]), string(sa.Pn[:]), string(sa.Desc[:]), string(sa.Unit[:]), sa.Mu, sa.Md})
	}
	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	fmt.Println("写静态模拟量OK")
}

type StaticDigital struct {
	P_num int32     // P_NUM, 4Byte
	Fack  uint16    // FACK, 2Byte
	Chn   [32]byte  // CHN, 32Byte
	Pn    [32]byte  // PN, 32Byte
	Desc  [128]byte // DESC, 128Byte
	Unit  [32]byte  // UNIT, 32Byte
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
		deviceId          = "root.sg1.dev6"
		timestamps        []int64
		measurementsSlice [][]string
		dataTypes         [][]client.TSDataType
		values            [][]interface{}
	)
	for index, sa := range goStaticDigitalArray {
		timestamps = append(timestamps, int64(index))
		measurementsSlice = append(measurementsSlice, []string{"P_num", "Fack", "Chn", "Pn", "Desc", "Unit"})
		dataTypes = append(dataTypes, []client.TSDataType{client.INT32, client.INT32, client.TEXT, client.TEXT, client.TEXT, client.TEXT})
		values = append(values, []interface{}{sa.P_num, int32(sa.Fack), string(sa.Chn[:]), string(sa.Pn[:]), string(sa.Desc[:]), string(sa.Unit[:])})
	}

	checkError(session.InsertRecordsOfOneDevice(deviceId, timestamps, measurementsSlice, dataTypes, values, false))
	fmt.Println("写静态数字量OK")
}

func toGoArray() {

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
