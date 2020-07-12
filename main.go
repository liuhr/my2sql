package main

import (
	"sync"
	my "my2sql/base"
)

func main() {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	if my.GConfCmd.WorkType != "stats" {
		// write forward or rollback sql to file
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}
	my.ParserAllBinEventsFromRepl(my.GConfCmd)
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait() 
}
