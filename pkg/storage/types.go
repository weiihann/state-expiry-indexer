package storage

import (
	"encoding/json"
)

type ReadRangeDiffs struct {
	BlockNum uint64
	Diffs    []ReadDiffs `json:"diffs"`
}

type ReadDiffs struct {
	StateDiff map[string]Diff `json:"stateDiff"`
}

type Diff struct {
	Storage    []string
	IsContract bool
}

func (d *Diff) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	d.Storage = nil
	d.IsContract = false

	if code, ok := raw["code"]; ok && code != nil {
		var temp map[string]json.RawMessage
		if err := json.Unmarshal(code, &temp); err == nil {
			if _, isObj := temp["*"]; isObj {
				d.IsContract = true
			}
		}
	}

	if storage, ok := raw["storage"]; ok && storage != nil {
		var temp map[string]json.RawMessage
		if err := json.Unmarshal(storage, &temp); err == nil {
			for k := range temp {
				d.Storage = append(d.Storage, k)
			}
			if len(temp) > 0 {
				d.IsContract = true
			}
		}
	}

	return nil
}
