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
var startTime int64
var endTime int64

func main() {
	// Need a main function to make CGO compile package as C shared library
}

//export login
func login(param *C.char) {
	fmt.Println("登录数据库")
	startTime = time.Now().UnixMilli()
	flag.StringVar(&host, "host", "192.168.150.100", "--host=192.168.150.100")
	flag.StringVar(&port, "port", "6667", "--port=6667")
	flag.StringVar(&user, "user", "root", "--user=root")
	flag.StringVar(&password, "password", "root", "--password=root")
	flag.Parse()
	config := &client.PoolConfig{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	// 控制session的并发连接数上限，否则可能断开连接
	sessionPool = client.NewSessionPool(config, 2000, 60000, 600000, false)
}

//export logout
func logout() {
	sessionPool.Close()
	endTime = time.Now().UnixMilli()
	fmt.Println("登出数据库，结束程序，时间" + fmt.Sprint(endTime-startTime) + "ms")
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
	//fmt.Println("写实时模拟量start")
	deviceCount := int64(count)
	analogs := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, an := range analogs {
		// path组成：baseRoot.unitID.devID(其中devID是Analog的P_NUM) TODO 设备名是什么要确定一下
		devices = append(devices, fmt.Sprintf("%s.unit%d.analogdev", baseRoot, int64(unit_id)))
		timestamps = append(timestamps, int64(time))
		measurementss = append(measurementss, []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		valuess = append(valuess, []interface{}{an.P_NUM, an.AV, an.AVR, an.Q, an.BF, an.QF, an.FAI, an.MS, string(an.TEW), int32(an.CST)})

	}
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	sessionPool.PutBack(session)
	//fmt.Println("写实时模拟量OK")
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
	sectionCount := int64(count)
	times := (*[1 << 30]C.int64_t)(unsafe.Pointer(time))[:sectionCount:sectionCount]
	analogsArray := (*[1 << 30]*C.Analog)(unsafe.Pointer(analog_array_array_ptr))[:sectionCount:sectionCount]
	arrayCounts := (*[1 << 30]C.int64_t)(unsafe.Pointer(array_count))[:sectionCount:sectionCount]

	//方式1：直接调用写实时模拟量函数，insertRecords
	for i := int64(0); i < sectionCount; i++ {
		write_rt_analog(unit_id, times[i], analogsArray[i], arrayCounts[i])
	}

	// 方式2：使用insertTablets(有问题)，使用insertTablet TODO 设备从哪里来？
	//var (
	//	devices       []string
	//	timestamps    []int64
	//	measurementss [][]string
	//	dataTypess    [][]client.TSDataType
	//	valuess       [][]interface{}
	//)
	//
	////var tablets []*client.Tablet
	//for i := int64(0); i < sectionCount; i++ {
	//	// path组成：baseRoot.unitID.devID(其中devID是Analog的P_NUM)
	//	devices = fmt.Sprintf("%s.unit%d.dev%d", baseRoot, int64(unit_id), i)
	//	// TODO 精简代码
	//	// 构建表头schemas
	//	measurements := []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"}
	//	dataTypes := []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32}
	//	measurementSchemas := make([]*client.MeasurementSchema, len(measurements))
	//	for j := range measurements {
	//		measurementSchemas[j] = &client.MeasurementSchema{
	//			Measurement: measurements[j],
	//			DataType:    dataTypes[j],
	//		}
	//	}
	//	rowCount := 1 // 就一个time，就一行
	//	tablet, _ := client.NewTablet(device, measurementSchemas, rowCount)
	//	for row := 0; row < int(rowCount); row++ {
	//		tablet.SetTimestamp(int64(time), row)
	//		tablet.SetValueAt(analogs[i].P_NUM, 0, row)
	//		tablet.SetValueAt(analogs[i].AV, 1, row)
	//		tablet.SetValueAt(analogs[i].AVR, 2, row)
	//		tablet.SetValueAt(analogs[i].Q, 3, row)
	//		tablet.SetValueAt(analogs[i].BF, 4, row)
	//		tablet.SetValueAt(analogs[i].QF, 5, row)
	//		tablet.SetValueAt(analogs[i].FAI, 6, row)
	//		tablet.SetValueAt(analogs[i].MS, 7, row)
	//		tablet.SetValueAt(string(analogs[i].TEW), 8, row)
	//		tablet.SetValueAt(int32(analogs[i].CST), 9, row)
	//	}
	//	session, err := sessionPool.GetSession()
	//	defer sessionPool.PutBack(session)
	//	if err == nil {
	//		checkError(session.InsertTablet(tablet, false))
	//	}
	//}
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
	//fmt.Println("写实时数字量start")
	deviceCount := int64(count)
	digitals := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, di := range digitals {
		// TODO 同理这个设备名也要确定一下
		devices = append(devices, fmt.Sprintf("%s.unit%d.digitaldev", baseRoot, int64(unit_id)))
		timestamps = append(timestamps, int64(time))
		measurementss = append(measurementss, []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		valuess = append(valuess, []interface{}{di.P_NUM, di.DV, di.DVR, di.Q, di.BF, di.FQ, di.FAI, di.MS, string(di.TEW), int32(di.CST)})
	}
	session, err := sessionPool.GetSession()
	defer sessionPool.PutBack(session)
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	//fmt.Println("写实时数字量OK")
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
	sectionCount := int64(count)
	times := (*[1 << 30]C.int64_t)(unsafe.Pointer(time))[:sectionCount:sectionCount]
	digitalsArray := (*[1 << 30]*C.Digital)(unsafe.Pointer(digital_array_array_ptr))[:sectionCount:sectionCount]
	arrayCounts := (*[1 << 30]C.int64_t)(unsafe.Pointer(array_count))[:sectionCount:sectionCount]

	// 直接调用写实时模拟量
	for i := int64(0); i < sectionCount; i++ {
		write_rt_digital(unit_id, times[i], digitalsArray[i], arrayCounts[i])
	}

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
	deviceCount := int64(count)
	analogs := (*[1 << 30]Analog)(unsafe.Pointer(analog_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, an := range analogs {
		devices = append(devices, fmt.Sprintf("%s.unit%d.dev%d.analog", baseRoot, int64(unit_id), an.P_NUM))
		timestamps = append(timestamps, int64(time))
		measurementss = append(measurementss, []string{"P_NUM", "AV", "AVR", "Q", "BF", "QF", "FAI", "MS", "TEW", "CST"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.FLOAT, client.FLOAT, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.FLOAT, client.BOOLEAN, client.TEXT, client.INT32})
		valuess = append(valuess, []interface{}{an.P_NUM, an.AV, an.AVR, an.Q, an.BF, an.QF, an.FAI, an.MS, string(an.TEW), int32(an.CST)})
	}
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	sessionPool.PutBack(session)
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
	deviceCount := int64(count)
	digitals := (*[1 << 30]Digital)(unsafe.Pointer(digital_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, di := range digitals {
		devices = append(devices, fmt.Sprintf("%s.unit%d.dev%d.digital", baseRoot, int64(unit_id), di.P_NUM))
		timestamps = append(timestamps, int64(time))
		measurementss = append(measurementss, []string{"P_NUM", "DV", "DVR", "Q", "BF", "FQ", "FAI", "MS", "TEW", "CST"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.INT32})
		valuess = append(valuess, []interface{}{di.P_NUM, di.DV, di.DVR, di.Q, di.BF, di.FQ, di.FAI, di.MS, string(di.TEW), int32(di.CST)})
	}
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	sessionPool.PutBack(session)
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
	deviceCount := int64(count)
	staticAnalogs := (*[1 << 30]StaticAnalog)(unsafe.Pointer(static_analog_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, sa := range staticAnalogs {
		devices = append(devices, fmt.Sprintf("%s.unit%d.dev%d.analog", baseRoot, int64(unit_id), sa.P_NUM))
		// TODO 没有给时间，timestamp取当前时间戳
		timestamps = append(timestamps, time.Now().UnixMilli())
		measurementss = append(measurementss, []string{"P_NUM", "TAGT", "FACK", "L4AR", "L3AR", "L2AR", "L1AR", "H4AR", "H3AR", "H2AR", "H1AR", "CHN", "PN", "DESC", "UNIT", "MU", "MD"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.INT32, client.INT32, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.BOOLEAN, client.TEXT, client.TEXT, client.TEXT, client.TEXT, client.FLOAT, client.FLOAT})
		valuess = append(valuess, []interface{}{sa.P_NUM, int32(sa.TAGT), int32(sa.FACK), sa.L4AR, sa.L3AR, sa.L2AR, sa.L1AR, sa.H4AR, sa.H3AR, sa.H2AR, sa.H1AR, string(sa.CHN[:]), string(sa.PN[:]), string(sa.DESC[:]), string(sa.UNIT[:]), sa.MU, sa.MD})
	}
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	sessionPool.PutBack(session)
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
	deviceCount := int64(count)
	staticDigitals := (*[1 << 30]StaticDigital)(unsafe.Pointer(static_digital_array_ptr))[:deviceCount:deviceCount]

	var (
		devices       []string
		timestamps    []int64
		measurementss [][]string
		dataTypess    [][]client.TSDataType
		valuess       [][]interface{}
	)

	for _, sd := range staticDigitals {
		devices = append(devices, fmt.Sprintf("%s.unit%d.dev%d.digital", baseRoot, int64(unit_id), sd.P_NUM))
		// TODO 同理，没有给时间
		timestamps = append(timestamps, time.Now().UnixMilli())
		measurementss = append(measurementss, []string{"P_NUM", "FACK", "CHN", "PN", "DESC", "UNIT"})
		dataTypess = append(dataTypess, []client.TSDataType{client.INT32, client.INT32, client.TEXT, client.TEXT, client.TEXT, client.TEXT})
		valuess = append(valuess, []interface{}{sd.P_NUM, int32(sd.FACK), string(sd.CHN[:]), string(sd.PN[:]), string(sd.DESC[:]), string(sd.UNIT[:])})
	}
	session, err := sessionPool.GetSession()
	if err == nil {
		checkError(session.InsertRecords(devices, measurementss, dataTypess, valuess, timestamps))
	}
	sessionPool.PutBack(session)
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
