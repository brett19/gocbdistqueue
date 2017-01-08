package cbdistqueue

import (
	"errors"
	"github.com/google/uuid"
	"github.com/couchbase/gocb"
	"fmt"
	"encoding/json"
	"time"
)

type Queue struct {
	localUuid string
	bucket *gocb.Bucket
	keyPrefix string

	cacheLeases map[uint64]time.Time
	cacheData *jsonControlDoc
	cacheCas gocb.Cas
}

var (
	ErrNoItems = errors.New("There are no items in the queue")
	ErrLeaseLost = errors.New("The lease on this item was lost")
)

func NewQueue(bucket *gocb.Bucket, keyPrefix string) (*Queue, error) {
	q := Queue{
		localUuid: uuid.New().String(),
		bucket: bucket,
		keyPrefix: keyPrefix,
		cacheLeases: make(map[uint64]time.Time),
		cacheData: nil,
		cacheCas: 0,
	}

	err := q.init()
	if err != nil {
		return nil, err
	}

	return &q, nil
}

type LeasedItem interface {
	Get(data interface{}) error
	IsStillLeased() bool
	RenewLease() error
	Pop() error
}

type leaseMap map[string]uint64

func (m *leaseMap) keyString(key uint64) string {
	return fmt.Sprintf("%d", key)
}

func (m *leaseMap) Contains(key uint64) bool {
	_, ok := (*m)[m.keyString(key)]
	return ok
}

func (m *leaseMap) Get(key uint64) uint64 {
	return (*m)[m.keyString(key)]
}

func (m *leaseMap) Set(key uint64, value uint64) {
	(*m)[m.keyString(key)] = value
}

func (m *leaseMap) Delete(key uint64) {
	delete(*m, m.keyString(key))
}

type jsonControlDoc struct {
	Start uint64 `json:"start"`
	Leased uint64 `json:"leased"`
	End uint64 `json:"end"`
	LeaseCounter uint64 `json:"atom"`
	Leases leaseMap `json:"leases"`
}

func (q *Queue) init() error {
	return nil
}

type ControlDocUpdateFunc func(data *jsonControlDoc) error

func (q *Queue) defaultControlDoc() jsonControlDoc {
	return jsonControlDoc {
		Start: 0,
		End: 0,
		LeaseCounter: 0,
		Leases: make(map[string]uint64),
	}
}

func (q *Queue) updateControlDoc(updateFn ControlDocUpdateFunc) error {
	controlKey := q.keyPrefix

	for {
		if q.cacheData == nil {
			var data jsonControlDoc
			cas, err := q.bucket.Get(controlKey, &data)
			if err == gocb.ErrKeyNotFound {
				q.cacheData = nil
				q.cacheCas = 0
			} else if err != nil {
				return err
			} else {
				q.trackLeases(data.Leases)
				q.cacheData = &data
				q.cacheCas = cas
			}
		}

		var data jsonControlDoc
		if q.cacheData != nil {
			data = *q.cacheData
		} else {
			data = q.defaultControlDoc()
		}

		err := updateFn(&data)
		if err != nil {
			return err
		}

		if q.cacheData != nil {
			cas, err := q.bucket.Replace(controlKey, data, q.cacheCas, 0)
			if err == gocb.ErrKeyExists {
				q.cacheData = nil
				q.cacheCas = 0
				continue
			} else if err != nil {
				return err
			}

			q.trackLeases(data.Leases)
			q.cacheData = &data
			q.cacheCas = cas
		} else {
			cas, err := q.bucket.Insert(controlKey, data, 0)
			if err == gocb.ErrKeyExists {
				q.cacheData = nil
				q.cacheCas = 0
				continue
			} else if err != nil {
				return err
			}

			q.trackLeases(data.Leases)
			q.cacheData = &data
			q.cacheCas = cas
		}


		break
	}

	return nil
}

type leasedItem struct {
	Owner *Queue
	Data json.RawMessage
	Item uint64
	Lease uint64
	LeaseTime time.Time
}

func (l *leasedItem) Get(data interface{}) error {
	return json.Unmarshal(l.Data, data)
}

func (l *leasedItem) IsStillLeased() bool {
	panic("Not yet supported")
}

func (l *leasedItem) RenewLease() error {
	return l.Owner.renewLeasedItem(l)
}

func (l *leasedItem) Pop() error {
	return l.Owner.popLeasedItem(l)
}

func (q *Queue) trackLeases(leases leaseMap) {
	newCache := make(map[uint64]time.Time)

	for _, lease := range leases {
		if when, ok := q.cacheLeases[lease]; ok {
			newCache[lease] = when
		} else {
			newCache[lease] = time.Now()
		}
	}

	q.cacheLeases = newCache
}

func (q *Queue) hasLeaseExpired(lease uint64) bool {
	return q.cacheLeases[lease].Add(5 * time.Second).After(time.Now())
}

func (q *Queue) LeaseItem() (LeasedItem, error) {
	var poppedItem uint64
	var lease uint64
	var leaseTime time.Time

	err := q.updateControlDoc(func(data *jsonControlDoc) error {
		myItem := data.Start
		for ; myItem < data.End; myItem++ {
			if data.Leases.Contains(myItem) {
				itemLease := data.Leases.Get(myItem)
				if q.hasLeaseExpired(itemLease) {
					break
				}
			} else {
				break
			}
		}

		if myItem == data.End {
			return ErrNoItems
		}

		if myItem >= data.Leased {
			data.Leased = myItem + 1
		}

		var myLease = data.LeaseCounter
		data.LeaseCounter++

		data.Leases.Set(myItem, myLease)

		poppedItem = myItem
		lease = myLease
		leaseTime = time.Now()
		return nil
	})
	if err != nil {
		return nil, err
	}

	itemKey := fmt.Sprintf("%s::%d", q.keyPrefix, poppedItem)

	var itemData json.RawMessage
	_, err = q.bucket.Get(itemKey, &itemData)
	if err != nil {
		return nil, err
	}

	return &leasedItem {
		Owner: q,
		Data: itemData,
		Item: poppedItem,
		Lease: lease,
		LeaseTime: leaseTime,
	}, nil
}

func (q *Queue) releaseLease(item uint64, lease uint64) error {
	return q.updateControlDoc(func(data *jsonControlDoc) error {
		if !data.Leases.Contains(item) {
			return ErrLeaseLost
		}

		foundLease := data.Leases.Get(item)
		if foundLease != lease {
			return ErrLeaseLost
		}

		data.Leases.Delete(item)

		for advItem := data.Start; advItem < data.Leased; advItem++ {
			if !data.Leases.Contains(advItem) {
				data.Start = advItem + 1
			} else {
				break
			}
		}

		return nil
	})
}

func (q *Queue) renewLeasedItem(leaseItem *leasedItem) error {
	var newLeaseTime time.Time
	var newLease uint64

	item := leaseItem.Item
	lease := leaseItem.Lease

	err := q.updateControlDoc(func(data *jsonControlDoc) error {
		if !data.Leases.Contains(item) {
			return ErrLeaseLost
		}

		foundLease := data.Leases.Get(item)
		if foundLease != lease {
			return ErrLeaseLost
		}

		var myLease = data.LeaseCounter
		data.LeaseCounter++

		data.Leases.Set(item, myLease)

		newLease = myLease
		newLeaseTime = time.Now()

		return nil
	})
	if err != nil {
		return err
	}

	leaseItem.Lease = newLease
	leaseItem.LeaseTime = newLeaseTime

	return nil
}

func (q *Queue) popLeasedItem(leaseItem *leasedItem) error {
	err := q.releaseLease(leaseItem.Item, leaseItem.Lease)
	if err != nil {
		return err
	}

	itemKey := fmt.Sprintf("%s::%d", q.keyPrefix, leaseItem.Item)
	q.bucket.Remove(itemKey, 0)

	return nil
}

// NOTE: This can cause items to be orphaned in the bucket if the client<->server
//  connection fails while performing updates.
func (q *Queue) PopItem(data interface{}) error {
	var poppedItem uint64

	err := q.updateControlDoc(func(data *jsonControlDoc) error {
		myItem := data.Start
		for ; myItem < data.End; myItem++ {
			if data.Leases.Contains(myItem) {
				itemLease := data.Leases.Get(myItem)
				if q.hasLeaseExpired(itemLease) {
					break
				}
			} else {
				break
			}
		}

		if myItem == data.End {
			return ErrNoItems
		}

		if myItem == data.Start {
			data.Start++
		}

		if myItem >= data.Leased {
			data.Leased = myItem + 1
		}

		poppedItem = myItem
		return nil
	})
	if err != nil {
		return err
	}

	itemKey := fmt.Sprintf("%s::%d", q.keyPrefix, poppedItem)

	_, err = q.bucket.Get(itemKey, data)
	if err != nil {
		return err
	}

	q.bucket.Remove(itemKey, 0)

	return nil
}

func (q *Queue) PushItem(data interface{}) error {
	var pushItem uint64
	var pushLease uint64

	err := q.updateControlDoc(func(data *jsonControlDoc) error {
		myItem := data.End
		data.End++

		var myLease = data.LeaseCounter
		data.LeaseCounter++

		data.Leases.Set(myItem, myLease)

		pushItem = myItem
		pushLease = myLease
		return nil
	})
	if err != nil {
		return err
	}

	// Deferral of lease removal
	defer q.releaseLease(pushItem, pushLease)

	itemKey := fmt.Sprintf("%s::%d", q.keyPrefix, pushItem)

	_, err = q.bucket.Insert(itemKey, data, 0)
	if err != nil {
		return err
	}

	return nil
}
