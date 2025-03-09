package task

import (
	"github.com/zeromicro/go-zero/core/logx"
	"time"
)

func StockTask() {
	logx.Infof("执行 StockHTTP 任务:%v", time.Now().Format("15:04:05"))
}
