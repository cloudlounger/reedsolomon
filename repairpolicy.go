package reedsolomon

import (
	"errors"
	"sort"
)

var ErrCannotRepair = errors.New("can not repair")
var ErrConflictState = errors.New("conflict state, can not repair")
var ErrInvalidInputShard = errors.New("invalid inputs shard")
var ErrDuplicatedShard = errors.New("duplicated shard")
var ErrNoBrokenShard = errors.New("no broken shard")
var ErrTooManyBrokenShards = errors.New("too many broken shards")

type PolicyFactory struct {
	dataShards   int
	localShards  int
	globalShards int
	choiceTree   *ChoiceTree
}

func NewPolicyFactory(dataShards, localShards, globalShards int) *PolicyFactory {
	choiceTree := NewChoiceTree()
	var AvailiableChoices [][]int
	if dataShards == 4 && localShards == 2 && globalShards == 3 {
		AvailiableChoices = DecodeCompactSlice(AvailiableChoicesCompact_4_2_3[:])
	} else if dataShards == 28 && localShards == 2 && globalShards == 3 {
		AvailiableChoices = DecodeCompactSlice(AvailiableChoicesCompact_28_2_3[:])
	} else {
		panic("not support")
	}
	for _, choice := range AvailiableChoices {
		choiceTree.AddChoice(choice)
	}
	return &PolicyFactory{
		dataShards:   dataShards,
		localShards:  localShards,
		globalShards: globalShards,
		choiceTree:   choiceTree,
	}
}

func (f *PolicyFactory) GeneratePolicy(availiableShards []int, brokensShards []int) (nextLoadShards []int, err error) {
	if f.localShards != 2 {
		err = ErrLocalShards
		return
	}
	sort.Slice(availiableShards, func(i, j int) bool {
		return availiableShards[i] < availiableShards[j]
	})
	sort.Slice(brokensShards, func(i, j int) bool {
		return brokensShards[i] < brokensShards[j]
	})
	if err = f.checkInput(availiableShards, brokensShards); err != nil {
		return
	}
	if len(brokensShards) == 0 {
		err = ErrNoBrokenShard
		return
	}
	// local repair
	if len(brokensShards) == 1 && len(availiableShards) == 0 {
		brokenShard := brokensShards[0]
		if brokenShard > f.dataShards+1 {
			for i := 0; i < f.dataShards; i++ {
				nextLoadShards = append(nextLoadShards, i)
			}
			return
		}
		if brokenShard < f.dataShards/2 || brokenShard == f.dataShards {
			for i := 0; i < f.dataShards/2; i++ {
				if brokenShard != i {
					nextLoadShards = append(nextLoadShards, i)
				}
			}
			if brokenShard != f.dataShards {
				nextLoadShards = append(nextLoadShards, f.dataShards)
			}
		} else if brokenShard < f.dataShards || brokenShard == f.dataShards+1 {
			for i := f.dataShards / 2; i < f.dataShards; i++ {
				if brokenShard != i {
					nextLoadShards = append(nextLoadShards, i)
				}
			}
			if brokenShard != f.dataShards+1 {
				nextLoadShards = append(nextLoadShards, f.dataShards+1)
			}
		}
		return
	}
	if len(brokensShards) > f.globalShards+f.localShards {
		err = ErrTooManyBrokenShards
		return
	}
	// global repair
	shards := make([]*Shard, 0, len(availiableShards)+len(brokensShards))
	for _, shardIndex := range availiableShards {
		shards = append(shards, &Shard{ShardIndex: shardIndex, IsBroken: false})
	}
	for _, shardIndex := range brokensShards {
		shards = append(shards, &Shard{ShardIndex: shardIndex, IsBroken: true})
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ShardIndex < shards[j].ShardIndex
	})
	brokenSet := NewSet()
	brokenSet.AddSlice(brokensShards)
	// recursive traversal of multitree
	knownShards := make(map[int]bool)
	for _, shard := range shards {
		knownShards[shard.ShardIndex] = shard.IsBroken
	}
	var isLeafe bool
	var ok bool
	for i := 0; i < len(f.choiceTree.rootShards); i++ {
		rootShard := f.choiceTree.rootShards[i]
		if brokenSet.Has(rootShard) {
			continue
		}
		var rootLevel *ChoiceLevel
		rootLevel, ok = f.choiceTree.rootLevel[rootShard]
		if !ok {
			err = ErrConflictState
			return
		}
		nextLoadShards, isLeafe = f.search(rootLevel, len(availiableShards), f.dataShards-len(availiableShards), nil, knownShards)
		if len(nextLoadShards) == f.dataShards-len(availiableShards) && isLeafe {
			return
		}
	}
	nextLoadShards = nil
	err = ErrCannotRepair
	return
}

func (f *PolicyFactory) search(node *ChoiceLevel, loadedNumber, searchNumber int, alreadLoadShards []int, knownShards map[int]bool) (nextLoadShards []int, isleafe bool) {
	//fmt.Println("------- handle node", node.shardIndex)
	isBroken, ok := knownShards[node.shardIndex]
	if ok && isBroken {
		return alreadLoadShards, false
	}
	nextLoadShards = alreadLoadShards
	if !ok {
		nextLoadShards = append(alreadLoadShards, node.shardIndex)
	} else {
		//fmt.Println("------- loadedNumber -----", node.shardIndex, node.childShards)
		loadedNumber -= 1
	}
	//fmt.Println("------- will walk child nodes -----", node.shardIndex, node.childShards)
	if node.childs == nil {
		if loadedNumber == 0 {
			return nextLoadShards, true
		} else {
			return nextLoadShards, false
		}

	}
	for _, shardIndex := range node.childShards {
		//fmt.Println("----0--- walk child node", shardIndex, nextLoadShards)
		child := node.childs[shardIndex]
		childLoadShards, isLeafe := f.search(child, loadedNumber, searchNumber, nextLoadShards, knownShards)
		if len(childLoadShards) == searchNumber && isLeafe {
			//fmt.Println("----1--- walk child node", shardIndex, "result:", childLoadShards)
			return childLoadShards, true
		}
		//fmt.Println("---2---- walk child node", shardIndex, "result:", childLoadShards)
	}
	return nextLoadShards, false
}

func (f *PolicyFactory) checkInput(availiableShards []int, brokensShards []int) (err error) {
	totalShards := f.dataShards + f.localShards + f.globalShards
	for _, v := range availiableShards {
		if v < 0 || v >= totalShards {
			err = ErrInvalidInputShard
			return
		}
	}
	for _, v := range brokensShards {
		if v < 0 || v >= totalShards {
			err = ErrInvalidInputShard
			return
		}
	}
	availiableSet := NewSet()
	brokenSet := NewSet()
	if err = availiableSet.AddSlice(availiableShards); err != nil {
		return
	}
	if err = brokenSet.AddSlice(brokensShards); err != nil {
		return
	}
	if availiableSet.hasSameValue(brokenSet) {
		err = ErrDuplicatedShard
		return
	}
	return
}

type Set struct {
	m map[int]struct{}
}

func NewSet() *Set {
	return &Set{
		m: make(map[int]struct{}),
	}
}

func (s *Set) Add(value int) error {
	if _, ok := s.m[value]; ok {
		return ErrDuplicatedShard
	}
	s.m[value] = struct{}{}
	return nil
}

func (s *Set) AddSlice(values []int) error {
	for _, v := range values {
		if _, ok := s.m[v]; ok {
			return ErrDuplicatedShard
		}
	}
	for _, v := range values {
		s.m[v] = struct{}{}
	}
	return nil
}

func (s *Set) Has(value int) bool {
	if _, ok := s.m[value]; ok {
		return true
	}
	return false
}

func (s *Set) hasSameValue(set *Set) bool {
	for v := range set.m {
		if s.Has(v) {
			return true
		}
	}
	return false
}

type Shard struct {
	ShardIndex int
	IsBroken   bool
}

type ChoiceTree struct {
	rootLevel  map[int]*ChoiceLevel
	rootShards []int
}

func NewChoiceTree() *ChoiceTree {
	return &ChoiceTree{
		rootLevel: make(map[int]*ChoiceLevel),
	}
}

func (c *ChoiceTree) AddChoice(choice []int) {
	var preChoiceLevel *ChoiceLevel
	for _, shardIndex := range choice {
		if preChoiceLevel == nil {
			if _, ok := c.rootLevel[shardIndex]; !ok {
				c.rootLevel[shardIndex] = &ChoiceLevel{
					shardIndex: shardIndex,
					childs:     make(map[int]*ChoiceLevel),
				}
				c.rootShards = append(c.rootShards, shardIndex)
				preChoiceLevel = c.rootLevel[shardIndex]
			} else {
				preChoiceLevel = c.rootLevel[shardIndex]
			}
		} else {
			if preChoiceLevel.childs == nil {
				preChoiceLevel.childs = make(map[int]*ChoiceLevel)
			}
			child, ok := preChoiceLevel.childs[shardIndex]
			if !ok {
				child = &ChoiceLevel{
					shardIndex: shardIndex,
				}
				preChoiceLevel.childs[shardIndex] = child
				preChoiceLevel.childShards = append(preChoiceLevel.childShards, shardIndex)
			}
			preChoiceLevel = child
		}
	}
}

type ChoiceLevel struct {
	shardIndex  int
	childs      map[int]*ChoiceLevel
	childShards []int // childs are disordered, while childShards can guarantee order
}

func DecodeCompactSlice(choices []int) (AvailiableChoices [][]int) {
	AvailiableChoices = make([][]int, len(choices))
	for i := 0; i < len(choices); i++ {
		v := choices[i]
		j := 0
		for v != 0 {
			value := v % 2
			if value != 0 {
				AvailiableChoices[i] = append(AvailiableChoices[i], j)
			}
			j++
			v = v / 2
		}
	}
	return
}
