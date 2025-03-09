package nacos

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/logx"
	"os"
)

func initNacosConfig() (config_client.IConfigClient, error) {

	// 从环境变量获取 Nacos 用户名和密码(docker命令参数)
	username := os.Getenv("NACOS_USERNAME")
	password := os.Getenv("NACOS_PASSWORD")
	ipAddr := os.Getenv("NACOS_IP")
	// Nacos服务器地址
	serverConfigs := []constant.ServerConfig{
		{
			Scheme:      "http",
			ContextPath: "/nacos",
			IpAddr:      ipAddr,
			Port:        8848,
		},
	}
	// 客户端配置
	clientConfig := constant.ClientConfig{
		NamespaceId:         "e1ce49d0-37e6-4739-ae6a-32dcd6eef74f", // 命名空间ID
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		Username:            username,
		Password:            password,
		LogDir:              "../logs/nacos/log",
		CacheDir:            "./nacos/cache",
		LogLevel:            "error",
	}

	// 创建配置客户端
	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		logx.Errorf("Nacos 配置信息 error:%v", err.Error())
		return nil, err
	}
	return configClient, nil
}

func GetConfigFromNacos(dataId, group string) (string, error) {

	cfgClient, err := initNacosConfig()
	if err != nil {
		return "", err
	}

	config, err := cfgClient.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		logx.Errorf("Nacos 配置信息 获取失败 error:%v", err.Error())
		return "", err
	}

	logx.Info("Nacos 配置信息 获取成功")

	return config, nil
}
