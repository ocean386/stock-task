package task

import (
	"fmt"
	"time"
)

func StockTask() {
	fmt.Printf("执行 StockHTTP 任务:%v\n", time.Now().Format("15:04:05"))
}
