package tag

import (
	"errors"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	Name          = "tag_round_robin"
	productionTag = "_production" // production tag
	ctxValue      = "tag"
)

var NoMatchConErr = errors.New("no match found conn")

// newTagBuilder creates a new tag balancer builder.
func newTagBuilder() {
	builder := base.NewBalancerBuilder(Name, &rrPickerBuilder{}, base.Config{HealthCheck: true})
	balancer.Register(builder)
	return
}

type rrPickerBuilder struct {
}

func (r *rrPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	var scs = make(map[string][]balancer.SubConn, len(info.ReadySCs))
	for conn, addr := range info.ReadySCs {
		tag := productionTag
		if nodeTag := GetNodeTag(addr.Address); nodeTag != "" {
			tag = nodeTag
		}
		scs[tag] = append(scs[tag], conn)
	}
	if len(scs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	return &rrPicker{
		node: scs,
	}
}

type rrPicker struct {
	node map[string][]balancer.SubConn
	mu   sync.Mutex
}

// Pick 选择合适的子链接发送请求
func (p *rrPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.Lock()
	t := time.Now().UnixNano() / 1e6
	defer p.mu.Unlock()

	var subConns []balancer.SubConn
	nodeTag := productionTag
	if tag := info.Ctx.Value(ctxValue).(string); tag != "" && len(p.node[tag]) > 0 { // 有标签
		nodeTag = tag
	}
	for _, conn := range p.node[nodeTag] {
		subConns = append(subConns, conn)
	}
	if len(subConns) == 0 {
		return balancer.PickResult{}, NoMatchConErr
	}

	index := rand.Intn(len(subConns))
	sc := subConns[index]
	return balancer.PickResult{SubConn: sc, Done: func(data balancer.DoneInfo) {
		log.Println("test=", info.FullMethodName, "end=", data.Err, "time=", time.Now().UnixNano()/1e6-t, "sc=", sc, "data=", data)
	}}, nil
}

func GetNodeTag(attr resolver.Address) string {
	v := attr.Attributes.Value(ctxValue).(*map[string]string)
	return (*v)[ctxValue]
}
