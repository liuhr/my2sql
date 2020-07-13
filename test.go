package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

func GetStrCommaSepFromStrSlice(arr []string) string {
	arrTmp := make([]string, len(arr))
	for i, v := range arr {
		arrTmp[i] = fmt.Sprintf("'%s'", v)
	}
	return strings.Join(arrTmp, ",")
}

func main() {
	var srcFH *os.File
	//var srcInfo os.FileInfo

	srcFH, _ = os.Open("./tmpdir/.rollback.44.sql")
	_, err := srcFH.Seek(0, os.SEEK_END)
	if err != nil {
		fmt.Println(err)
	}
	//srcInfo, _ = srcFH.Stat()
	//size := srcInfo.Size()
	//_ := srcInfo.Size()

	startPos, _ := srcFH.Seek(-int64(160), os.SEEK_CUR)
	var buf []byte = make([]byte, 160)
	io.ReadFull(srcFH, buf)
	fmt.Println(string(buf))

	fmt.Println(startPos)

	str := `Could not execute Write_student; Duplicate entry '24' for key 'PRIMARY', Error_code: 1062; handler error HA_ERR_FOUND_DUPP_KEY; the event's master log mysql-bin.000011, end_log_pos 1003  xs`
	re := regexp.MustCompile(`.*\b(?P<file>mysql-bin.([a-z0-9-.]+)),\s+\bend_log_pos\s+\b(?P<pos>[0-9]+)`)

	match := re.FindStringSubmatch(str)

	groupNames := re.SubexpNames()
	result := make(map[string]string)
	for i, name := range groupNames {
		if i != 0 && name != "" { // 第一个分组为空（也就是整个匹配）
			result[name] = match[i]
		}
	}
	fmt.Println(result)
	a := []string{"a", "b'''d", "c"}
	fmt.Println(GetStrCommaSepFromStrSlice(a))

	arr := strings.Split("json", "(")
	fmt.Println(arr)
}
