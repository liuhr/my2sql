package base

import (
	"github.com/siddontang/go-log/log"
	"net"
	"os"
)

// 获取 系统的hostname 和 ip地址
func GetSystemHomeNameAndAdderss() (hostname string, address string) {
	// get system hostname
	host, err := os.Hostname()
	if err != nil {
		log.Error("%v %s", err, "fail to get system hostname")

	} else {
		hostname = host
	}

	// get system address
	netInterfaces, err := net.Interfaces()
	if err != nil {
		log.Error("%v %s", err, "fail to get system adderss")
	}
	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()
			for _, add := range addrs {
				if ipnet, ok := add.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						//address = append(address, ipnet.IP.String())
						address = ipnet.IP.String()
					}
				}
			}
		}
	}

	return hostname, address
}
