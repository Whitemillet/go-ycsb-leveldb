package taas_tikv

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/go-ycsb/db/taas"
	"github.com/pingcap/go-ycsb/db/taas_proto"
	tikverr "github.com/tikv/client-go/v2/error"
)

//#include ""

// 提交，ctx，table，key，value，连接上taas？
func (db *txnDB) TxnCommit(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for taas.InitOk == 0 {
		time.Sleep(50)
	}
	// 获取当前时间t1
	t1 := time.Now().UnixNano()
	// 原子操作csn+1，总事务数+1
	txnId := atomic.AddUint64(&taas.CSNCounter, 1) // return new value
	atomic.AddUint64(&taas.TotalTransactionCounter, 1)
	// 创建事务 并初始化
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

	// 读写操作数
	var readOpNum, writeOpNum uint64 = 0, 0
	time1 := time.Now()
	tx, err := db.db.Begin() // tx是？
	if err != nil {
		return err
	}
	// 函数结束时执行rollback
	defer tx.Rollback()
	var tryRead int = 0
	for i, key := range keys {
		// 无value则表示为读操作
		if values[i] == nil { //read
			tryRead = 0
			readOpNum++
		TRYREAD: // 尝试读取
			rowKey := db.getRowKey(table, key)
			time2 := time.Now()
			// 获取当前rowKey下的data，时间，设置读延迟
			rowData, err := tx.Get(ctx, rowKey)
			timeLen2 := time.Now().Sub(time2)
			atomic.AddUint64(&taas.TikvReadLatency, uint64(timeLen2))
			if tikverr.IsErrNotFound(err) {
				return err
			} else if rowData == nil {
				if tryRead < 10 {
					tryRead++ /// 读取次数小于10则再次读取
					goto TRYREAD
				} else {
					return err
				}
			}
			// 定义row初始化
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Read,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   rowData,
				Csn:    0,
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
			//fmt.Println("; Read, key : " + string(rowKey) + " Data : " + string(rowData))
		} else {
			// 写操作
			writeOpNum++
			rowKey := db.getRowKey(table, key)
			rowData, err := db.r.Encode(nil, values[i])
			if err != nil {
				return err
			}
			// 创建row
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
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	// 计算总时延
	timeLen := time.Now().Sub(time1)
	atomic.AddUint64(&taas.TikvTotalLatency, uint64(timeLen))
	//fmt.Println("; read op : " + strconv.FormatUint(readOpNum, 10) + ", write op : " + strconv.FormatUint(writeOpNum, 10))

	// 初始化type，将 sendMessage 序列化为字节切片，并将结果赋值给 sendBuffer 变量
	sendMessage := &taas_proto.Message{
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	var bufferBeforeGzip bytes.Buffer
	sendBuffer, _ := proto.Marshal(sendMessage)
	bufferBeforeGzip.Reset()
	// 将 sendBuffer 写入 gw 对象
	gw := gzip.NewWriter(&bufferBeforeGzip)
	_, err = gw.Write(sendBuffer)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	// 获取transaction比特
	GzipedTransaction := bufferBeforeGzip.Bytes()
	GzipedTransaction = GzipedTransaction
	// 添加到通道
	taas.TaasTxnCH <- taas.TaasTxn{GzipedTransaction}

	// 从对应client通道中获取res
	result, ok := <-(taas.ChanList[txnId%uint64(taas.ClientNum)])
	//fmt.Println("Receive From Taas")
	t2 := uint64(time.Now().UnixNano() - t1)
	taas.TotalLatency += t2
	//append(latency, t2)
	//result, ok := "Abort", true
	// 读写数统计+1
	atomic.AddUint64(&taas.TotalReadCounter, uint64(readOpNum))
	atomic.AddUint64(&taas.TotalUpdateCounter, uint64(writeOpNum))
	if ok {
		if result != "Commit" {
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
