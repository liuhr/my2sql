package base

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/siddontang/go-log/log"
)

func ReverseFileGo(threadIdx int, rollbackFileChan chan map[string]string, bytesCntFiles map[string][][]int, keepTrx bool, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Infof("start thread %d to revert rollback sql files", threadIdx)
	for arr := range rollbackFileChan {
		//ReverseFileToNewFile(arr["tmp"], arr["rollback"], batchLines)
		//ReverseFileToNewFileOneByOneLineAndKeepTrx(arr["tmp"], arr["rollback"])
		ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(arr["tmp"], arr["rollback"], bytesCntFiles[arr["tmp"]], keepTrx)
		err := os.Remove(arr["tmp"])
		if err != nil {
			log.Fatalf("fail to remove tmp file %s", arr["tmp"])
		}
	}
	log.Infof(fmt.Sprintf("exit thread %d to revert rollback sql files", threadIdx))
}

func ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(srcFile string, destFile string, trxPoses [][]int, keepTrx bool) error {
	var (
		srcFH            *os.File
		destFH           *os.File
		err              error
		srcInfo          os.FileInfo
		readByteCntTotal int64 = 0
		srcSize          int64
		bufStr           string
		LineSep          string = "\n"
		lastTrxIdx       int    = 0
	)

	log.Infof(fmt.Sprintf("start to revert tmp file %s into %s", srcFile, destFile))
	srcFH, err = os.Open(srcFile)
	if srcFH != nil {
		defer srcFH.Close()
	}
	if err != nil {
		log.Errorf("fail to open tmp file %s", srcFile)
		return err
	}

	destFH, err = os.OpenFile(destFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if destFH != nil {
		defer destFH.Close()
	}
	if err != nil {
		log.Errorf("fail to open file %s", destFile)
		return err
	}

	srcInfo, err = srcFH.Stat()
	if err != nil {
		log.Errorf("fail to stat file %s", srcFile)
		return err
	}

	srcSize = srcInfo.Size() //int64

	//var ifCommit bool = true

	_, err = srcFH.Seek(0, os.SEEK_END)
	if err != nil {
		log.Errorf("fail to seek file %s", srcFile)
		return err
	}

	for batchIdx := len(trxPoses) - 1; batchIdx >= 0; batchIdx-- {

		startPos, err := srcFH.Seek(-int64(trxPoses[batchIdx][0]), os.SEEK_CUR)
		if err != nil {
			log.Errorf("fail to seek file %s", srcFile)
			return err
		}
		var buf []byte = make([]byte, trxPoses[batchIdx][0])
		//_, err = srcFH.Read(buf)
		_, err = io.ReadFull(srcFH, buf)
		if err != nil {
			log.Errorf("fail to read file %s", srcFile)
			return err
		}

		readByteCntTotal += int64(trxPoses[batchIdx][0])

		bufStr = string(buf)
		strArr := strings.Split(bufStr, LineSep)
		var strArrStrs []string = make([]string, len(strArr))

		for ji, ai := 0, len(strArr)-1; ai >= 0; ai-- {

			if strArr[ai] == "" {
				continue
			}
			/*
				if trxPoses[batchIdx][1] == 1 {
					if strArr[ai] == "commit" {
						ifCommit = true
						if batchIdx == 0 && ai == 0 {
							strArrStrs[ji] = "" // "commit" is written as the first line in the tmp file, so we skip it
						} else {
							strArrStrs[ji] = "begin"
						}

					} else if strArr[ai] == "rollback" {
						ifCommit = false
						strArrStrs[ji] = "begin"
					} else if strArr[ai] == "begin" {
						if ifCommit {
							strArrStrs[ji] = "commit"
						} else {
							strArrStrs[ji] = "rollback"
						}
						ifCommit = true // default is commit
					}
				} else {
					strArrStrs[ji] = strArr[ai]
				}
			*/
			strArrStrs[ji] = strArr[ai]
			ji++

		}
		if keepTrx && lastTrxIdx != trxPoses[batchIdx][1] {
			destFH.WriteString("commit;\nbegin;\n")
		}
		lastTrxIdx = trxPoses[batchIdx][1]
		_, err = destFH.WriteString(strings.Join(strArrStrs, LineSep))
		if err != nil {
			log.Errorf("fail to write file %s", destFile)
			return err
		}

		if readByteCntTotal == srcSize || startPos == 0 {
			break // finishing reading
		}
		if batchIdx > 0 {
			_, err := srcFH.Seek(-int64(trxPoses[batchIdx][0]), os.SEEK_CUR)
			if err != nil {
				log.Errorf("fail to seek file %s", srcFile)
				return err
			}
		}
	}

	if keepTrx {
		destFH.WriteString("commit;\n")
	}
	log.Infof(fmt.Sprintf("finish reverting tmp file %s into %s", srcFile, destFile))
	return nil

}
