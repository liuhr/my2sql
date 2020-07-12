package toolkits

import (
	"regexp"
)

func IsIP(ip string) (b bool) {

	m, _ := regexp.MatchString("^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$", ip)

	if !m {
		return false
	}
	return true
}

func GetFilePos(LastSQLError string) map[string]string {

	FilePosPatten := `.*\b(?P<file>[\-\w\S]+\.([a-z0-9-.]+)),\s+\bend_log_pos\s+\b(?P<pos>[0-9]+)`
	re := regexp.MustCompile(FilePosPatten)
	match := re.FindStringSubmatch(LastSQLError)
	groupNames := re.SubexpNames()

	result := make(map[string]string)

	for i, name := range groupNames {
		if i != 0 && name != "" { // 第一个分组为空（也就是整个匹配）
			result[name] = match[i]
		}
	}
	return result
}
