package conhash

import (
	"math"
)

const (
	defaultCopies = 10
	opCapacity    = 4096
)

type (
	Router struct {
		nodes    []*node
		routeOps chan *routeOp
		downOps  chan *downOp
		upOps    chan *upOp
	}

	node struct {
		id        int
		low, high int
	}

	// TODO a normal hash request. given the hashcode of some operation,
	// output the node that this request should be routed to. load balancing,
	// bitch. a negative response signifies an error.
	routeOp struct {
		hash int
		resp chan int
	}

	downOp struct {
		/// TODO
	}

	upOp struct {
		// TODO
	}
)

// TODO create a consistent hasher with a given number of nodes and a given
// number of copies. make copies copies of each node and insert them all into
// our super cool dope router
func NewRouter(nodes, copies int) *Router {
	r := &Router{
		nodes:    make([]*node, 0, nodes*copies),
		routeOps: make(chan *routeOp, opCapacity),
		downOps:  make(chan *downOp, opCapacity),
		upOps:    make(chan *upOp, opCapacity),
	}

	step := math.MaxUint32 / nodes * copies

	for i, _ := range r.nodes {
		r.nodes[i] = &node{
			id:   i,
			low:  i * step,
			high: (i + 1) * step,
		}
	}

	return r
}

// TODO continuously run; stateful goroutine. this allows us to avoid using
// any concurrency primitives and isntead take advantage of go's biggest
// strength: the insanely powerful concurrency primitive known as channels.
func (r *Router) Run() {
	for {
		select {
		case op := <-r.routeOps:
			if op == nil {
				return
			}
			continue
		}

	}
}

// TODO find the correct range (key) given an input integer via a binary
// search on they keys of the array. optimization: start at an approximation
// of the end goal. start at range stepSize * (MAX_INT / input)
func (r *Router) findNode(hash int) *node {
	return nil
}
