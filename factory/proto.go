package factory

type ProtoType int

const (
	ProtoVideoPacket ProtoType = 0
	ProtoHTTP        ProtoType = 1
	ProtoGRPC        ProtoType = 2
	ProtoThrift      ProtoType = 3
)
