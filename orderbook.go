package hftorderbook

import (
	"fmt"
	"sync"
)

// maximum limits per orderbook side to pre-allocate memory
const MaxLimitsNum int = 10000

type OrderDepth struct {
	Price      float64
	Volume     float64
	OrderCount int
	isBuyDepth bool
}

type Orderbook struct {
	Bids           *redBlackBST
	Asks           *redBlackBST
	IdToOrderMap   map[int]*Order
	bidLimitsCache map[float64]*LimitOrder
	askLimitsCache map[float64]*LimitOrder
	pool           *sync.Pool
	TotalVolume    float64
}

func NewOrderbook() Orderbook {
	bids := NewRedBlackBST()
	asks := NewRedBlackBST()
	return Orderbook{
		Bids: &bids,
		Asks: &asks,

		IdToOrderMap:   make(map[int]*Order),
		bidLimitsCache: make(map[float64]*LimitOrder, MaxLimitsNum),
		askLimitsCache: make(map[float64]*LimitOrder, MaxLimitsNum),
		pool: &sync.Pool{
			New: func() interface{} {
				limit := NewLimitOrder(0.0)
				return &limit
			},
		},
	}
}

func (this *Orderbook) Add(price float64, o *Order) {
	var limit *LimitOrder

	if o.BidOrAsk {
		limit = this.bidLimitsCache[price]
	} else {
		limit = this.askLimitsCache[price]
	}

	if limit == nil {
		// getting a new limit from pool
		limit = this.pool.Get().(*LimitOrder)
		limit.Price = price

		// insert into the corresponding BST and cache
		if o.BidOrAsk {
			this.Bids.Put(price, limit)
			this.bidLimitsCache[price] = limit
		} else {
			this.Asks.Put(price, limit)
			this.askLimitsCache[price] = limit
		}
	}
	this.IdToOrderMap[o.Id] = o
	this.TotalVolume += o.Volume
	// add order to the limit
	limit.Enqueue(o)
}

func (this *Orderbook) Modify(price float64, o *Order) {
	if _, ok := this.IdToOrderMap[o.Id]; !ok {
		return
	}
	order := this.IdToOrderMap[o.Id]
	this.Cancel(order)
	this.Add(price, o)
}

// buy=bid
// ask=sell
func (this *Orderbook) Execute(price float64, buyOrder, sellOrder *Order) {
	if orderBid, ok := this.IdToOrderMap[buyOrder.Id]; ok {
		this.Cancel(orderBid)
		if orderBid.Volume > buyOrder.Volume {
			buyOrder.Volume = orderBid.Volume - buyOrder.Volume
			this.Add(price, buyOrder)
		}
	}
	if orderAsk, ok := this.IdToOrderMap[sellOrder.Id]; ok {
		this.Cancel(orderAsk)
		if orderAsk.Volume > sellOrder.Volume {
			sellOrder.Volume = orderAsk.Volume - sellOrder.Volume
			this.Add(price, sellOrder)
		}
	}
}

func (this *Orderbook) Cancel(order *Order) {
	if o, ok := this.IdToOrderMap[order.Id]; ok {
		limit := o.Limit
		limit.Delete(o)
		if limit.Size() == 0 {
			// remove the limit if there are no orders
			if o.BidOrAsk {
				this.Bids.Delete(limit.Price)
				delete(this.bidLimitsCache, limit.Price)
			} else {
				this.Asks.Delete(limit.Price)
				delete(this.askLimitsCache, limit.Price)
			}

			// put it back to the pool
			this.pool.Put(limit)
		}
		this.TotalVolume -= o.Volume
		delete(this.IdToOrderMap, o.Id)
	}

}

func (this *Orderbook) GetTotalOrderVolume() float64 {
	return this.TotalVolume
}

func (this *Orderbook) ClearBidLimit(price float64) {
	this.clearLimit(price, true)
}

func (this *Orderbook) ClearAskLimit(price float64) {
	this.clearLimit(price, false)
}

func (this *Orderbook) clearLimit(price float64, bidOrAsk bool) {
	var limit *LimitOrder
	if bidOrAsk {
		limit = this.bidLimitsCache[price]
	} else {
		limit = this.askLimitsCache[price]
	}

	if limit == nil {
		panic(fmt.Sprintf("there is no such price limit %0.8f", price))
	}

	limit.Clear()
}

func (this *Orderbook) DeleteBidLimit(price float64) {
	limit := this.bidLimitsCache[price]
	if limit == nil {
		return
	}

	this.deleteLimit(price, true)
	delete(this.bidLimitsCache, price)

	// put limit back to the pool
	limit.Clear()
	this.pool.Put(limit)

}

func (this *Orderbook) DeleteAskLimit(price float64) {
	limit := this.askLimitsCache[price]
	if limit == nil {
		return
	}

	this.deleteLimit(price, false)
	delete(this.askLimitsCache, price)

	// put limit back to the pool
	limit.Clear()
	this.pool.Put(limit)
}

func (this *Orderbook) deleteLimit(price float64, bidOrAsk bool) {
	if bidOrAsk {
		this.Bids.Delete(price)
	} else {
		this.Asks.Delete(price)
	}
}

func (this *Orderbook) GetVolumeAtBidLimit(price float64) float64 {
	limit := this.bidLimitsCache[price]
	if limit == nil {
		return 0
	}
	return limit.TotalVolume()
}

func (this *Orderbook) GetVolumeAtAskLimit(price float64) float64 {
	limit := this.askLimitsCache[price]
	if limit == nil {
		return 0
	}
	return limit.TotalVolume()
}

func (this *Orderbook) GetBest20(isBuyDepth bool) []OrderDepth {
	var nodePointer *nodeRedBlack
	if isBuyDepth {
		if this.Bids == nil || this.Bids.IsEmpty() {
			return nil
		}
		nodePointer = this.Bids.MaxPointer()
	} else {
		if this.Asks == nil || this.Asks.IsEmpty() {
			return nil
		}
		nodePointer = this.Asks.MinPointer()
	}
	if nodePointer == nil {
		return nil
	}
	depthList := make([]OrderDepth, 20)

	for i := 0; i < 20; i++ {
		depth := OrderDepth{
			isBuyDepth: isBuyDepth,
		}
		if nodePointer != nil {
			limit := nodePointer.Value
			depth.Price = limit.Price
			depth.Volume = limit.totalVolume
			depth.OrderCount = limit.Size()
			if isBuyDepth {
				nodePointer = nodePointer.Prev
			} else {
				nodePointer = nodePointer.Next
			}
		}
		depthList[i] = depth
	}
	return depthList
}

func (this *Orderbook) GetBest20Bid() []OrderDepth {
	return this.GetBest20(true)
}

func (this *Orderbook) GetBest20Offer() []OrderDepth {
	return this.GetBest20(false)
}

func (this *Orderbook) BLength() int {
	return len(this.bidLimitsCache)
}

func (this *Orderbook) ALength() int {
	return len(this.askLimitsCache)
}
