package taas_hbase_txn

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pingcap/go-ycsb/db/taas"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"net"
	"reflect"
	"sync/atomic"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"strconv"
)

import (
	"context"
)

//const (
//	tikvAsyncCommit = "tikv.async_commit"
//	tikvOnePC       = "tikv.one_pc"
//)

const (
	HOST = "127.0.0.1"
	PORT = "9090"
)

type txnConfig struct {
	asyncCommit bool
	onePC       bool
}

type txnDB struct {
	db      *THBaseServiceClient
	r       *util.RowCodec
	bufPool *util.BufPool
	//needed by HBase
	transport       *thrift.TSocket
	protocolFactory *thrift.TProtocolFactory
}

var HBaseConncetion []*THBaseServiceClient

func createTxnDB(p *properties.Properties) (ycsb.DB, error) {

	for i := 0; i < taas.ClientNum; i++ {
		protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
		transport, err := thrift.NewTSocket(net.JoinHostPort(taas.HbaseServerIp, strconv.Itoa(9090)))
		if err != nil {
			return nil, err
		}
		client := NewTHBaseServiceClientFactory(transport, protocolFactory)
		err = transport.Open()
		if err != nil {
			return nil, err
		}
		HBaseConncetion = append(HBaseConncetion, client)
	}

	bufPool := util.NewBufPool()

	return &txnDB{
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func (db *txnDB) Close() error {
	//seems that HBase
	return db.transport.Close()
}

func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *txnDB) CleanupThread(ctx context.Context) {
}

func (db *txnDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

// seems that HBase don't need this function
func (db *txnDB) beginTxn() (*transaction.KVTxn, error) {
	return nil, nil
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	tx := db.db
	row, err := tx.Get(ctx, []byte(table), &TGet{Row: []byte(key)})

	if err != nil {
		return nil, err
	}

	columnValues := row.ColumnValues
	res := make(map[string][]byte)
	for _, column := range columnValues {
		c := reflect.ValueOf(column).Elem()
		family := c.Field(0)
		value := c.Field(2)
		res[string(family.Interface().([]uint8))] = value.Interface().([]byte)
	}

	return res, nil
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	tx := db.db

	var tempTGet []*TGet

	rowValues := make([]map[string][]byte, len(keys))
	keyLoc := make(map[string]int)
	for i, key := range keys {
		tempTGet = append(tempTGet, &TGet{
			Row: []byte(key),
		})
		keyLoc[key] = i
	}

	res, err := tx.GetMultiple(ctx, []byte(table), tempTGet)

	if err != nil {
		return nil, err
	}

	for _, columnValues := range res {
		i := keyLoc[string(columnValues.Row)]
		column := columnValues.ColumnValues
		c := reflect.ValueOf(column).Elem()
		family := c.Field(0)
		value := c.Field(2)
		rowValues[i][string(family.Interface().([]uint8))] = value.Interface().([]byte)
	}
	return rowValues, nil
}

// TODO I don't know how to use scanner in HBase
func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("calling error, should use func TxnCommit")

	//return nil, nil
}

// in HBase, 'update' is equal to 'put', so only implement Insert() is ok
func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	panic("calling error, should use func TxnCommit")
	//for InitOk == 0 {
	//	time.Sleep(50)
	//}
	//txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	//atomic.AddUint64(&TotalTransactionCounter, 1)
	//
	//rowKey := db.getRowKey(table, key)
	//var bufferBeforeGzip bytes.Buffer
	//clientIP := LocalServerIp
	//txnSendToTaas := taas_proto.Transaction{
	//	//Row:         {},
	//	StartEpoch:  0,
	//	CommitEpoch: 5,
	//	Csn:         uint64(time.Now().UnixNano()),
	//	ServerIp:    TaasServerIp,
	//	ServerId:    0,
	//	ClientIp:    clientIP,
	//	ClientTxnId: txnId,
	//	TxnType:     taas_proto.TxnType_ClientTxn,
	//	TxnState:    0,
	//}
	//updateKey := rowKey
	//sendRow := taas_proto.Row{
	//	OpType: taas_proto.OpType_Update,
	//	Key:    *(*[]byte)(unsafe.Pointer(&updateKey)),
	//}
	//for field, value := range values {
	//	idColumn, _ := strconv.ParseUint(string(field[5]), 10, 32)
	//	updatedColumn := taas_proto.Column{
	//		Id:    uint32(idColumn),
	//		Value: value,
	//	}
	//	sendRow.Column = append(sendRow.Column, &updatedColumn)
	//}
	//txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
	//sendMessage := &taas_proto.Message{
	//	Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	//}
	//sendBuffer, _ := proto.Marshal(sendMessage)
	//bufferBeforeGzip.Reset()
	//gw := gzip.NewWriter(&bufferBeforeGzip)
	//_, err := gw.Write(sendBuffer)
	//if err != nil {
	//	return err
	//}
	//err = gw.Close()
	//if err != nil {
	//	return err
	//}
	//GzipedTransaction := bufferBeforeGzip.Bytes()
	//TaasTxnCH <- TaasTxn{GzipedTransaction}
	//
	//result, ok := <-(ChanList[txnId%2048])
	//if ok {
	//	if result != "Commit" {
	//		atomic.AddUint64(&FailedTransactionCounter, 1)
	//		return err
	//	}
	//	atomic.AddUint64(&SuccessTransactionCounter, 1)
	//} else {
	//	fmt.Println("txn_bak.go 481")
	//	log.Fatal(ok)
	//	return err
	//}
	//return nil
}

func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("calling error, should use func TxnCommit")
	//tx, err := db.beginTxn()
	//if err != nil {
	//	return err
	//}
	//defer tx.Rollback()
	//
	//for i, key := range keys {
	//	// TODO should we check the key exist?
	//	rowData, err := db.r.Encode(nil, values[i])
	//	if err != nil {
	//		return err
	//	}
	//	if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
	//		return err
	//	}
	//}
	//return tx.Commit(ctx)
	return nil
}

func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	txnId := atomic.AddUint64(&taas.CSNCounter, 1)
	var cvarr []*TColumnValue
	finalData, err1 := db.r.Encode(nil, values)
	if err1 != nil {
		return err1
	}
	cvarr = append(cvarr, &TColumnValue{
		Family:    []byte("entire"),
		Qualifier: []byte(""),
		Value:     []byte(string(finalData)),
	})

	tempTPut := TPut{Row: []byte(key), ColumnValues: cvarr}
	err := HBaseConncetion[txnId%uint64(taas.ClientNum)].Put(ctx, []byte(table), &tempTPut)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("not finished yet")
	//client := db.db
	//var tempTPuts []*TPut
	//for i, key := range keys {
	//	var cvarr []*TColumnValue
	//	for k, v := range values[i] {
	//		cvarr = append(cvarr, &TColumnValue{
	//			Family:    []byte(k),
	//			Qualifier: []byte(""),
	//			Value:     v,
	//		})
	//	}
	//	tempTPuts = append(tempTPuts, &TPut{
	//		Row:          []byte(key),
	//		ColumnValues: cvarr,
	//	})
	//}
	//return client.PutMultiple(ctx, []byte(table), tempTPuts)
}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {

	client := db.db

	tdelete := TDelete{Row: []byte(key)}
	err := client.DeleteSingle(ctx, []byte(table), &tdelete)
	return err
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {

	client := db.db

	var tDeletes []*TDelete

	for _, key := range keys {
		tDeletes = append(tDeletes, &TDelete{
			Row: []byte(key),
		})
	}

	_, err := client.DeleteMultiple(ctx, []byte(table), tDeletes)
	return err
}
