package conhash

import (
	"math"
)

// TODO define error response codes. or mayeb just make it package errors, e.g.
//
//   ErrorMissingBucket
//   ErrorTooManyKeys
//   etc

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
		resp chan<- int
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

// TODO public-facing interface to route. returns the corresponding node through the
// resp channel.
func (r *Router) Route(hash int, resp chan<- int) {
	op := &routeOp{
		hash: hash,
		resp: resp,
	}
	r.findOps <- op
}

// TODO public facing interface. responses are as follows
// 0 is good. 1 is error. we could actually define a set of errors.
func (r *Router) NodeUp(hash int, resp chan<- int) {

}

// TODO public facing interface. responses are as follows
// 0 is good. 1 is error. we could actually define a set of errors.
func (r *Router) NodeDown(hash int, resp chan<- int) {

}

// TODO continuously run; stateful goroutine. this allows us to avoid using
// any concurrency primitives and isntead take advantage of go's biggest
// strength: the insanely powerful concurrency primitive known as channels.
func (r *Router) Run() {
	for {
		select {
		case op := <-r.routeOps:
			r.route(op)
		case op := <-r.downOps:
			r.down(op)
		case op := <-r.upOps:
			r.up(op)
		}
	}
}

// TODO process a routeOp. dont forget to push to the resp channel.
func (r *Router) route(op *routeOp) {
	op.resp <- search(op.hash).id
}

// TODO process a routeOp. dont forget to push to the resp channel.
func (r *Router) down(op *routeOp) {
	op.resp <- -1
}

// TODO process a routeOp. dont forget to push to the resp channel.
func (r *Router) up(op *routeOp) {
	op.resp <- -1
}

// Given a hash, return the node containing the range that contains the
// hash.
func (r *Router) search(hash int) *node {
	var low, high, middle int

	low = 0
	high = len(r.nodes)

	for low < high {
		middle = (high - low) / 2
		node = r.nodes[middle]

		if hash < node.low {
			high = middle
		} else if hash >= node.high {
			low = middle
		} else {
			return node
		}
	}

	return r.nodes[low]
}
