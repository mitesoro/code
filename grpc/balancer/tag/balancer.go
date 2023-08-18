package nacos

import (
	"errors"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Name          = "tag_round_robin"
	productionTag = "_production" // production tag
	ctxValue      = "tag"
)

var (
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
	mu sync.Mutex

	NoMatchConErr = errors.New("no match found conn")
)

// NewBuilder creates a new weight balancer builder.
func newVersionBuilder() {
	// balancer.Builder
	builder := base.NewBalancerBuilder(Name, &rrPickerBuilder{}, base.Config{HealthCheck: true})
	balancer.Register(builder)
	return
}

type rrPickerBuilder struct {
}

func (rp *rrPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
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
	mu.Lock()
	next := r.Intn(len(scs))
	mu.Unlock()

	return &rrPicker{
		subConns: scs,
		next:     uint32(next),
	}
}

type rrPicker struct {
	subConns map[string][]balancer.SubConn
	next     uint32
}

// Pick 选择合适的子链接发送请求
func (p *rrPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	tag := info.Ctx.Value("tag").(string)
	nodeTag := productionTag
	if tag != "" && len(p.subConns[tag]) > 0 { // 有标签
		nodeTag = tag
	}
	subConns := p.subConns[nodeTag]
	if len(subConns) == 0 {
		return balancer.PickResult{}, NoMatchConErr
	}

	subConnsLen := uint32(len(subConns))
	nextIndex := atomic.AddUint32(&p.next, 1)
	sc := subConns[nextIndex%subConnsLen]

	return balancer.PickResult{SubConn: sc}, nil
}

func GetNodeTag(attr resolver.Address) string {
	v := attr.Attributes.Value(ctxValue).(*map[string]string)
	return (*v)[ctxValue]
}
