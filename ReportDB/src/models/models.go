package models

type DataPoint struct {
	Timestamp uint32      `json:"timestamp"`
	Value     interface{} `json:"value"`
}
type Metric struct {
	ObjectID uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Value interface{} `json:"value"`

	Timestamp uint32 `json:"timestamp"`
}

type Query struct {
	QueryID uint64 `json:"query_id"`

	From uint32 `json:"from"`

	To uint32 `json:"to"`

	ObjectIDs []uint32 `json:"Object_id"`

	CounterId uint16 `json:"counter_id"`

	Aggregation string `json:"aggregation"`
}

type QueryResponse struct {
	QueryID uint64 `json:"query_id"`

	Data map[uint32][]DataPoint `json:"data"`
}
