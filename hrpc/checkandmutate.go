// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package hrpc

import (
	"github.com/yangtau/gohbase/filter"
	"github.com/yangtau/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

// CheckAndMutate performs a provided Put operation if the value specified
// by condition equals to the one set in the HBase.
type CheckAndMutate struct {
	*Mutate

	family    []byte
	qualifier []byte

	comparator *pb.Comparator
}

// NewCheckAndMutate creates a new CheckAndPut request that will compare provided
// expectedValue with the on in HBase located at put's row and provided family:qualifier,
// and if they are equal, perform the provided put request on the row
func NewCheckAndMutate(mut *Mutate, family string,
	qualifier string, expectedValue []byte) (*CheckAndMutate, error) {

	// The condition that needs to match for the edit to be applied.
	exp := filter.NewByteArrayComparable(expectedValue)
	cmp, err := filter.NewBinaryComparator(exp).ConstructPBComparator()
	if err != nil {
		return nil, err
	}

	// CheckAndPut is not batchable as MultiResponse doesn't return Processed field
	// for Mutate Action
	mut.setSkipBatch(true)

	return &CheckAndMutate{
		Mutate:     mut,
		family:     []byte(family),
		qualifier:  []byte(qualifier),
		comparator: cmp,
	}, nil
}

// ToProto converts the RPC into a protobuf message
func (cp *CheckAndMutate) ToProto() proto.Message {
	mutateRequest, _, _ := cp.toProto(false)
	mutateRequest.Condition = &pb.Condition{
		Row:       cp.key,
		Family:    cp.family,
		Qualifier: cp.qualifier,
		// TODO: not only EQUAL
		CompareType: pb.CompareType_EQUAL.Enum(),
		Comparator:  cp.comparator,
	}
	return mutateRequest
}

func (cp *CheckAndMutate) CellBlocksEnabled() bool {
	// cellblocks are not supported for check and put request
	return false
}
