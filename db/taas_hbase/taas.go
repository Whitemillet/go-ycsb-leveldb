package taas_hbase

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/go-ycsb/db/taas"
	"github.com/pingcap/go-ycsb/db/taas_proto"
)

// 连接Taas统计延迟 读写操作数
// TxnCommit 属于txnDB类型？
func (db *txnDB) TxnCommit(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	// 初始化成功则等待50ms
	for taas.InitOk == 0 {
		time.Sleep(50)
	}

	// 获取当前时间纳秒
	t1 := time.Now().UnixNano()
	// csn +1 原子操作
	txnId := atomic.AddUint64(&taas.CSNCounter, 1) // return new value
	// total transaction counter +1 原子操作
	atomic.AddUint64(&taas.TotalTransactionCounter, 1)
	// 创建一个transaction
	txnSendToTaas := taas_proto.Transaction{
		StartEpoch:  0,
		CommitEpoch: 5,
		Csn:         uint64(time.Now().UnixNano()),
		ServerIp:    taas.TaasServerIp,
		ServerId:    0,
		ClientIp:    taas.LocalServerIp,
		ClientTxnId: txnId,
		TxnType:     taas_proto.TxnType_ClientTxn,
		TxnState:    0,
	}

	// 定义读写操作个数
	var readOpNum, writeOpNum uint64 = 0, 0
	time1 := time.Now()
	for i, key := range keys {
		// 无value则判定为读操作
		if values[i] == nil { //read
			// 读操作+1
			// 获取行键值 当前时间
			readOpNum++
			rowKey := db.getRowKey(table, key)
			time2 := time.Now()
			// 调用获取get的connection，若err不空则异常
			rowData, err := HBaseConncetion[txnId].Get(ctx, []byte(table), &TGet{Row: []byte(key)})
			if err != nil {
				return err
			} else if rowData == nil {
				return errors.New("txn read failed")
			}
			// 创建map
			res := make(map[string][]byte)
			// 遍历 rowData.ColumnValues，使用 reflect 包获取 column 的 family 和 value 字段的值，并将它们添加到 res
			for _, column := range rowData.ColumnValues {
				c := reflect.ValueOf(column).Elem()
				family := c.Field(0)
				value := c.Field(2)
				res[string(family.Interface().([]uint8))] = value.Interface().([]byte)
			}
			// 计算耗时，ReadLatency 的值加上这个差值
			timeLen2 := time.Now().Sub(time2)
			atomic.AddUint64(&taas.TikvReadLatency, uint64(timeLen2))
			if err != nil {
				return err
			}
			// 创建sendRow，并填写进Row
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Read,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   []byte(res["entire"]),
				Csn:    0,
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
			//fmt.Println("; Read, key : " + string(rowKey) + " Data : " + string(rowData))
		} else {
			// 写操作++，获取key，data，无需计算延迟
			writeOpNum++
			rowKey := db.getRowKey(table, key)
			rowData, err := db.r.Encode(nil, values[i])
			if err != nil {
				return err
			}
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Update,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   []byte(rowData),
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
			//fmt.Print("; Update, key : " + string(rowKey))
			//fmt.Println("; Write, key : " + string(rowKey) + " Data : " + string(rowData))
		}

	}
	//if err = tx.Commit(ctx); err != nil {
	//	return err
	//}
	// 记录当前时间，延迟
	timeLen := time.Now().Sub(time1)
	atomic.AddUint64(&taas.TikvTotalLatency, uint64(timeLen))
	//fmt.Println("; read op : " + strconv.FormatUint(readOpNum, 10) + ", write op : " + strconv.FormatUint(writeOpNum, 10))

	// 定义sendMessage，初始化Type
	sendMessage := &taas_proto.Message{
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	// 定义buffer
	var bufferBeforeGzip bytes.Buffer
	// 将sendMessage序列化
	sendBuffer, _ := proto.Marshal(sendMessage)
	bufferBeforeGzip.Reset()

	// NewWriter？
	gw := gzip.NewWriter(&bufferBeforeGzip)
	_, err := gw.Write(sendBuffer)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	// 将beforeGzip压缩付给transaction
	GzipedTransaction := bufferBeforeGzip.Bytes()
	GzipedTransaction = GzipedTransaction
	//fmt.Println("Send to Taas")
	// 将 GzipedTransaction 添加到通道中
	taas.TaasTxnCH <- taas.TaasTxn{GzipedTransaction}

	// 从通道中接收数据
	result, ok := <-(taas.ChanList[txnId%uint64(taas.ClientNum)])
	//fmt.Println("Receive From Taas")
	t2 := uint64(time.Now().UnixNano() - t1)
	// 计算延迟
	taas.TotalLatency += t2
	//append(latency, t2)
	//result, ok := "Abort", true
	// TotalReadCounter 的值加上 readOpNum 的值，将 taas.TotalUpdateCounter 的值加上 writeOpNum 的值
	atomic.AddUint64(&taas.TotalReadCounter, uint64(readOpNum))
	atomic.AddUint64(&taas.TotalUpdateCounter, uint64(writeOpNum))
	if ok {
		if result != "Commit" {
			// 不提交则
			atomic.AddUint64(&taas.FailedReadCounter, uint64(readOpNum))
			atomic.AddUint64(&taas.FailedUpdateounter, uint64(writeOpNum))
			atomic.AddUint64(&taas.FailedTransactionCounter, 1)
			//fmt.Println("Commit Failed")
			return errors.New("txn conflict handle failed")
		}
		atomic.AddUint64(&taas.SuccessReadCounter, uint64(readOpNum))
		atomic.AddUint64(&taas.SuccessUpdateCounter, uint64(writeOpNum))
		atomic.AddUint64(&taas.SuccessTransactionCounter, 1)
		//fmt.Println("Commit Success")
	} else {
		fmt.Println("txn_bak.go 481")
		log.Fatal(ok)
		return err
	}
	return nil
}
