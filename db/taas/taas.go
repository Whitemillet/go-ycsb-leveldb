package taas

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/magiconair/properties"
	zmq "github.com/pebbe/zmq4"
	"io/ioutil"
	"log"
)

//#include ""
import (
	"github.com/golang/protobuf/proto"

	"github.com/pingcap/go-ycsb/db/taas_proto"
)

var LocalServerIp = "127.0.0.1"
var TaasServerIp = "127.0.0.1"
var StorageServerIp = "127.0.0.1"
var HbaseServerIp = "127.0.0.1"

var OpNum = 10
var ClientNum = 64
var UnPackNum = 16

type TaasTxn struct {
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000)
var UnPackCH = make(chan string, 100000)

var ChanList []chan string
var InitOk uint64 = 0
var CSNCounter uint64 = 0
var SuccessTransactionCounter, FailedTransactionCounter, TotalTransactionCounter uint64 = 0, 0, 0
var SuccessReadCounter, FailedReadCounter, TotalReadCounter uint64 = 0, 0, 0
var SuccessUpdateCounter, FailedUpdateounter, TotalUpdateCounter uint64 = 0, 0, 0
var TotalLatency, TikvReadLatency, TikvTotalLatency uint64 = 0, 0, 0
var latency []uint64

func SetConfig(globalProps *properties.Properties) {
	TaasServerIp = globalProps.GetString("taasServerIp", "127.0.0.1")
	LocalServerIp = globalProps.GetString("localServerIp", "127.0.0.1")
	StorageServerIp = globalProps.GetString("storageServerIp", "127.0.0.1")
	HbaseServerIp = globalProps.GetString("hbaseServerIp", "127.0.0.1")

	OpNum = globalProps.GetInt("opNum", 1)
	ClientNum = globalProps.GetInt("threadcount", 64)
	UnPackNum = globalProps.GetInt("unpackNum", 16)

	TaasServerIp = globalProps.GetString("taasServerIp", "")

	fmt.Println("localServerIp : " + LocalServerIp + ", taasServerIp : " + TaasServerIp + ", hbaseServerIp " + HbaseServerIp + " ;")

}

func SendTxnToTaas() {
	socket, _ := zmq.NewSocket(zmq.PUSH)
	err := socket.SetSndbuf(1000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(1000000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(1000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(1000000000)
	if err != nil {
		return
	}
	err = socket.Connect("tcp://" + TaasServerIp + ":5551")
	if err != nil {
		fmt.Println("taas.go 97")
		log.Fatal(err)
	}
	fmt.Println("连接Taas Send " + TaasServerIp)
	for {
		value, ok := <-TaasTxnCH
		if ok {
			_, err := socket.Send(string(value.GzipedTransaction), 0)
			//fmt.Println("taas send thread")
			if err != nil {
				return
			}
		} else {
			fmt.Println("taas.go 109")
			log.Fatal(ok)
		}
	}
}

func ListenFromTaas() {
	socket, err := zmq.NewSocket(zmq.PULL)
	err = socket.SetSndbuf(1000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(1000000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(1000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(1000000000)
	if err != nil {
		return
	}
	err = socket.Bind("tcp://*:5552")
	fmt.Println("连接Taas Listen")
	if err != nil {
		log.Fatal(err)
	}
	for {
		taasReply, err := socket.Recv(0)
		if err != nil {
			fmt.Println("taas.go 115")
			log.Fatal(err)
		}
		UnPackCH <- taasReply
	}
}

func UGZipBytes(in []byte) []byte {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out
	}
	defer reader.Close()
	out, _ := ioutil.ReadAll(reader)
	return out

}

func UnPack() {
	for {
		taasReply, ok := <-UnPackCH
		if ok {
			UnGZipedReply := UGZipBytes([]byte(taasReply))
			testMessage := &taas_proto.Message{}
			err := proto.Unmarshal(UnGZipedReply, testMessage)
			if err != nil {
				fmt.Println("taas.go 142")
				log.Fatal(err)
			}
			replyMessage := testMessage.GetReplyTxnResultToClient()
			ChanList[replyMessage.ClientTxnId%uint64(ClientNum)] <- replyMessage.GetTxnState().String()
		} else {
			fmt.Println("taas.go 148")
			log.Fatal(ok)
		}
	}
}
