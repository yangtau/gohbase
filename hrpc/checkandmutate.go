// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"fmt"

	"github.com/yangtau/gohbase/filter"
	"github.com/yangtau/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// CheckAndMutate performs a provided Put operation if the value specified
// by condition equals to the one set in the HBase.
type CheckAndMutate struct {
	*Mutate

	family        []byte
	qualifier     []byte
	op            *filter.CompareType
	comparator    *pb.Comparator
	filter        *pb.Filter
	fromTimestamp int64
	toTimestamp   int64
}

// NewCheckAndMutate creates a new CheckAndMutate request that will compare provided
// expectedValue with the on in HBase located at put's row and provided family:qualifier,
// and if they are equal, perform the provided put request on the row
func NewCheckAndMutate(mut *Mutate, family string,
	qualifier string, op filter.CompareType, cmp filter.Comparator) (*CheckAndMutate, error) {
	pbCmp, err := cmp.ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	// CheckAndMutate is not batchable as MultiResponse doesn't return Processed field
	// for Mutate Action
	// TODO: ?
	mut.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate:        mut,
		family:        []byte(family),
		qualifier:     []byte(qualifier),
		op:            &op,
		comparator:    pbCmp,
		filter:        nil,
		fromTimestamp: MinTimestamp,
		toTimestamp:   MaxTimestamp,
	}, nil
}

// NewMutateIfEquals create a new CheckAndMutate request that check for equality of (family:qualifer, value)
func NewMutateIfEquals(mut *Mutate, family, qualifer string, value []byte) (*CheckAndMutate, error) {
	return NewCheckAndMutate(mut, family, qualifer, filter.Equal,
		filter.NewBinaryComparator(filter.NewByteArrayComparable(value)))
}

// NewMutateIfNotExists create a new CheckAndMutate request that check for lack of column (family:qualifer)
func NewMutateIfNotExists(mut *Mutate, family, qualifer string) (*CheckAndMutate, error) {
	return NewMutateIfEquals(mut, family, qualifer, nil)
}

// NewMutateIfMatch create a new CheckAndMutate request by filter
func NewMutateIfMatch(mut *Mutate, f filter.Filter) (*CheckAndMutate, error) {
	pbF, err := f.ConstructPBFilter()
	if err != nil {
		return nil, err
	}

	mut.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate:        mut,
		family:        nil,
		qualifier:     nil,
		op:            nil,
		comparator:    nil,
		filter:        pbF,
		fromTimestamp: MinTimestamp,
		toTimestamp:   MaxTimestamp,
	}, nil
}

// SetTimeRange sets time range for cm
func (cm *CheckAndMutate) SetTimeRange(from, to int64) error {
	if from > to {
		return fmt.Errorf("Invalid time range: (%v, %v)", from, to)
	}

	cm.fromTimestamp = from
	cm.toTimestamp = to
	return nil
}

// SetFilter sets
func (cm *CheckAndMutate) SetFilter(f filter.Filter) error {
	pbF, err := f.ConstructPBFilter()
	if err != nil {
		return err
	}
	if cm.filter != nil {
		return fmt.Errorf("filter has already been setted")
	}
	cm.filter = pbF
	return nil
}

// ToProto converts the RPC into a protobuf message
func (cm *CheckAndMutate) ToProto() proto.Message {
	mutateRequest, _, _ := cm.toProto(false)
	from, to := uint64(cm.fromTimestamp), uint64(cm.toTimestamp)
	mutateRequest.Condition = &pb.Condition{
		Row:         cm.key,
		Family:      cm.family,
		Qualifier:   cm.qualifier,
		CompareType: (*pb.CompareType)(cm.op),
		Comparator:  cm.comparator,
		Filter:      cm.filter,
		TimeRange: &pb.TimeRange{
			From: &from,
			To:   &to,
		},
	}
	return mutateRequest
}

func (cp *CheckAndMutate) CellBlocksEnabled() bool {
	// cellblocks are not supported for check and put request
	return false
}
