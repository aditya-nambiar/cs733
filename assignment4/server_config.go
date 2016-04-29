package main


type RaftNodeConfig struct {
	Id        int
	fsAddr string
}

type ClusterConfig struct {
	Peers []RaftNodeConfig
}

var configs_fs ClusterConfig = ClusterConfig{
	Peers: []RaftNodeConfig{
		{Id:0,fsAddr:"localhost:9201"},
		{Id:1,fsAddr:"localhost:9202"},
		{Id:2,fsAddr:"localhost:9203"},
		{Id:3,fsAddr:"localhost:9204"},
		{Id:4,fsAddr:"localhost:9205"},
	},
}
